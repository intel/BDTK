/*
 * Copyright(c) 2022-2023 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace cider {
namespace transformer {
namespace test {
namespace util {

using ::facebook::velox::BaseVector;
using ::facebook::velox::BufferPtr;
using ::facebook::velox::FlatVector;
using ::facebook::velox::RowTypePtr;
using ::facebook::velox::RowVector;
using ::facebook::velox::RowVectorPtr;
using ::facebook::velox::TypeKind;
using ::facebook::velox::TypePtr;
using ::facebook::velox::vector_size_t;
using ::facebook::velox::VectorPtr;
using ::facebook::velox::memory::MemoryPool;

class BatchDataGenerator {
 public:
  explicit BatchDataGenerator(MemoryPool* pool) : pool_(pool) {}

  auto generate(RowTypePtr& rowType,
                int rowVectorSize,
                vector_size_t vectorSize,
                bool withNull) {
    std::mt19937 gen{std::mt19937::default_seed};
    std::vector<RowVectorPtr> batches;
    for (int i = 0; i < rowVectorSize; ++i) {
      auto batch =
        createRowVector(rowType, vectorSize, gen, withNull ? randomNulls(7) : nullptr);
      batches.push_back(batch);
    }
    return batches;
  }

  RowVectorPtr createRowVector(RowTypePtr& rowType,
                               vector_size_t vectorSize,
                               std::mt19937& gen,
                               std::function<bool(vector_size_t)> isNullAt = nullptr) {
    std::vector<VectorPtr> children;
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      auto vectorPtr = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(createScalar,
                                                          rowType->childAt(i)->kind(),
                                                          rowType->childAt(i),
                                                          vectorSize,
                                                          gen,
                                                          isNullAt);
      children.emplace_back(vectorPtr);
    }

    return std::make_shared<RowVector>(
        pool_, rowType, BufferPtr(nullptr), vectorSize, children);
  }

 private:
  std::function<bool(vector_size_t)> randomNulls(int32_t n) {
    return [n](vector_size_t) { return folly::Random::rand32() % n == 0; };
  }

  template <typename T>
  T gen_value(std::mt19937& gen);

  template <TypeKind KIND>
  VectorPtr createScalar(TypePtr type,
                         vector_size_t size,
                         std::mt19937& gen,
                         std::function<bool(vector_size_t)> isNullAt = nullptr) {
    using T = facebook::velox::TypeTraits<KIND>::NativeType;
    auto flatVector =
        std::dynamic_pointer_cast<FlatVector<T>>(BaseVector::create(type, size, pool_));
    for (vector_size_t i = 0; i < size; ++i) {
      if (isNullAt && isNullAt(i)) {
        flatVector->setNull(i, true);
      } else {
        flatVector->set(i, gen_value<T>(gen));
      }
    }
    return flatVector;
  }

 private:
  MemoryPool* pool_;
};

}  // namespace util
}  // namespace test
}  // namespace transformer
}  // namespace cider
