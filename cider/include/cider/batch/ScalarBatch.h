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

#define CIDERBATCH_WITH_ARROW

#include <cstdint>
#include "CiderBatch.h"

template <typename T>
class ScalarBatch final : public CiderBatch {
 public:
  static std::unique_ptr<ScalarBatch<T>> Create(ArrowSchema* schema,
                                                ArrowArray* array = nullptr) {
    return array ? std::make_unique<ScalarBatch<T>>(schema, array)
                 : std::make_unique<ScalarBatch<T>>(schema);
  }

  explicit ScalarBatch(ArrowSchema* schema) : CiderBatch(schema) { checkArrowEntries(); }
  explicit ScalarBatch(ArrowSchema* schema, ArrowArray* array)
      : CiderBatch(schema, array) {
    checkArrowEntries();
  }

  bool resizeBatch(int64_t size, bool default_not_null = false) override {
    CHECK(!isMoved());
    if (!permitBufferAllocate()) {
      return false;
    }

    auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(getArrayPrivate());

    array_holder->allocBuffer(1, sizeof(T) * size);
    bool ret = resizeNullVector(0, size, default_not_null);

    setLength(size);

    return ret;
  }

  T* getMutableRawData() {
    CHECK(!isMoved());
    return reinterpret_cast<T*>(const_cast<void*>(getBuffersPtr()[1]));
  }

  const T* getRawData() const {
    CHECK(!isMoved());
    return reinterpret_cast<const T*>(getBuffersPtr()[1]);
  }

 private:
  void checkArrowEntries() const {
    CHECK_EQ(getChildrenNum(), 0);
    CHECK_EQ(getBufferNum(), 2);
  }
};

#endif
