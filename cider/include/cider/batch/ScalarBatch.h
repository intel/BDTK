/*
 * Copyright (c) 2022 Intel Corporation.
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

#ifndef CIDER_SCALAR_BATCH_H
#define CIDER_SCALAR_BATCH_H

#include <type_traits>

#include "CiderBatch.h"

template <typename T>
class ScalarBatch final : public CiderBatch {
 public:
  static std::unique_ptr<ScalarBatch<T>> Create(ArrowSchema* schema,
                                                std::shared_ptr<CiderAllocator> allocator,
                                                ArrowArray* array = nullptr) {
    return array ? std::make_unique<ScalarBatch<T>>(schema, array, allocator)
                 : std::make_unique<ScalarBatch<T>>(schema, allocator);
  }

  using NativeType = std::conditional_t<std::is_same_v<bool, T>, uint8_t, T>;

  explicit ScalarBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, allocator) {
    checkArrowEntries();
  }
  explicit ScalarBatch(ArrowSchema* schema,
                       ArrowArray* array,
                       std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, array, allocator) {
    checkArrowEntries();
  }

  NativeType* getMutableRawData() {
    CHECK(!isMoved());
    return reinterpret_cast<NativeType*>(const_cast<void*>(getBuffersPtr()[1]));
  }

  const NativeType* getRawData() const {
    CHECK(!isMoved());
    return reinterpret_cast<const NativeType*>(getBuffersPtr()[1]);
  }

 protected:
  bool resizeData(int64_t size) override {
    CHECK(!isMoved());
    if (!permitBufferAllocate()) {
      return false;
    }

    auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(getArrayPrivate());

    if constexpr (std::is_same_v<T, bool>) {
      array_holder->allocBuffer(1, (size + 7) >> 3);
    } else {
      array_holder->allocBuffer(1, sizeof(T) * size);
    }
    setLength(size);

    return true;
  }

 private:
  void checkArrowEntries() const {
    CHECK_EQ(getChildrenNum(), 0);
    CHECK_EQ(getBufferNum(), 2);
  }
};

template <>
class ScalarBatch<bool> final : public CiderBatch {
 public:
  static std::unique_ptr<ScalarBatch<bool>> Create(
      ArrowSchema* schema,
      std::shared_ptr<CiderAllocator> allocator,
      ArrowArray* array = nullptr) {
    return array ? std::make_unique<ScalarBatch<bool>>(schema, array, allocator)
                 : std::make_unique<ScalarBatch<bool>>(schema, allocator);
  }

  explicit ScalarBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, allocator) {
    checkArrowEntries();
  }
  explicit ScalarBatch(ArrowSchema* schema,
                       ArrowArray* array,
                       std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, array, allocator) {
    checkArrowEntries();
  }

  uint8_t* getMutableRawData() {
    CHECK(!isMoved());
    return reinterpret_cast<uint8_t*>(const_cast<void*>(getBuffersPtr()[1]));
  }

  const uint8_t* getRawData() const {
    CHECK(!isMoved());
    return reinterpret_cast<const uint8_t*>(getBuffersPtr()[1]);
  }

 protected:
  bool resizeData(int64_t size) override {
    CHECK(!isMoved());
    if (!permitBufferAllocate()) {
      return false;
    }

    auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(getArrayPrivate());

    auto bytes = (size + 7) >> 3;
    array_holder->allocBuffer(1, bytes);
    setLength(size);

    return true;
  }

 private:
  void checkArrowEntries() const {
    CHECK_EQ(getChildrenNum(), 0);
    CHECK_EQ(getBufferNum(), 2);
  }
};

#endif
