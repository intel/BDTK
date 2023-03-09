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

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace RangedBatchGenerator {

struct MinMaxRange;
using MinMaxRangeVec = std::vector<MinMaxRange>;
using facebook::velox::BaseVector;
using facebook::velox::RowVector;
using facebook::velox::Type;
using facebook::velox::TypeKind;
using facebook::velox::VectorPtr;

template <typename T>
static T getDefaultMaxValue() {
  if (std::is_same_v<bool, T>) {
    return 1;
  } else if (std::is_integral_v<T>) {
    return std::numeric_limits<T>::max() - 1;
  } else {
    return std::numeric_limits<T>::max();
  }
}

template <typename T>
static T getDefaultMinValue() {
  if (std::is_same_v<bool, T>) {
    return 0;
  } else if (std::is_integral_v<T>) {
    return std::numeric_limits<T>::min() + 1;
  } else {
    return std::numeric_limits<T>::min();
  }
}

struct MinMaxRange {
  std::optional<double> min;
  std::optional<double> max;
  std::optional<double> nullProb;

  MinMaxRange() {}
  MinMaxRange(double min_val, double max_val) : min(min_val), max(max_val) {}
  MinMaxRange(double min_val, double max_val, double null_prob_val)
      : min(min_val), max(max_val), nullProb(null_prob_val) {}
};

template <TypeKind kind,
          typename NativeT = typename facebook::velox::TypeTraits<kind>::NativeType,
          typename std::enable_if_t<(kind == TypeKind::SHORT_DECIMAL ||
                                     kind == TypeKind::LONG_DECIMAL) ||
                                        !std::is_trivial_v<NativeT>,
                                    bool> = true>
static void generateRangedVector(const std::shared_ptr<const Type>& type,
                                 BaseVector* vec,
                                 uint64_t capacity,
                                 const MinMaxRangeVec& rangeVec,
                                 size_t& colIndex,
                                 std::mt19937& gen) {
  VELOX_NYI();
}

template <TypeKind kind,
          typename NativeT = typename facebook::velox::TypeTraits<kind>::NativeType,
          typename std::enable_if_t<(kind != TypeKind::SHORT_DECIMAL &&
                                     kind != TypeKind::LONG_DECIMAL) &&
                                        std::is_trivial_v<NativeT>,
                                    bool> = true>
static void generateRangedVector(const std::shared_ptr<const Type>& type,
                                 BaseVector* vec,
                                 uint64_t capacity,
                                 const MinMaxRangeVec& rangeVec,
                                 size_t& colIndex,
                                 std::mt19937& gen) {
  static_assert(!std::is_void_v<NativeT>,
                "Type \'void\' is invalid for trivial type generateRangedVector.");

  const auto targetMin = rangeVec[colIndex].min.has_value()
                             ? rangeVec[colIndex].min.value()
                             : getDefaultMinValue<NativeT>(),
             targetMax = rangeVec[colIndex].max.has_value()
                             ? rangeVec[colIndex].max.value()
                             : getDefaultMaxValue<NativeT>();

  auto dataVector = vec->asFlatVector<NativeT>();
  VELOX_CHECK(dataVector);
  dataVector->resize(capacity);

  if (rangeVec[colIndex].nullProb.has_value()) {
    auto rawNulls = dataVector->mutableNulls(capacity);
    VELOX_CHECK(rawNulls);
    facebook::velox::bits::fillBits(
        rawNulls->template asMutable<uint64_t>(), 0, capacity, true);
  }

  NativeT* data = dataVector->mutableRawValues();
  for (size_t i = 0; i < capacity; ++i) {
    data[i] = folly::Random::rand64(targetMin, targetMax, gen);
    if (rangeVec[colIndex].nullProb.has_value() &&
        folly::Random::randDouble01() < rangeVec[colIndex].nullProb.value()) {
      dataVector->setNull(i, true);
    }
  }

  ++colIndex;
}

template <>
void generateRangedVector<TypeKind::ROW>(const std::shared_ptr<const Type>& type,
                                         BaseVector* vec,
                                         uint64_t capacity,
                                         const MinMaxRangeVec& rangeVec,
                                         size_t& colIndex,
                                         std::mt19937& gen) {
  auto& rowType = type->asRow();
  auto rowVec = vec->as<RowVector>();
  VELOX_CHECK(rowVec);

  rowVec->resize(capacity);

  for (size_t i = 0; i < rowType.size(); ++i) {
    VELOX_DYNAMIC_TYPE_DISPATCH(generateRangedVector,
                                rowType.childAt(i)->kind(),
                                rowType.childAt(i),
                                rowVec->childAt(i).get(),
                                capacity,
                                rangeVec,
                                colIndex,
                                gen);
  }
}

VectorPtr createRangedBatch(const std::shared_ptr<const Type>& type,
                            uint64_t capacity,
                            facebook::velox::memory::MemoryPool& memoryPool,
                            const MinMaxRangeVec& rangeVec,
                            std::mt19937::result_type seed) {
  std::mt19937 gen{seed};
  auto generatedBatch = BaseVector::create(type, 0, &memoryPool);

  size_t colIndex = 0;
  generateRangedVector<TypeKind::ROW>(
      type, generatedBatch.get(), capacity, rangeVec, colIndex, gen);

  return generatedBatch;
}
};  // namespace RangedBatchGenerator
