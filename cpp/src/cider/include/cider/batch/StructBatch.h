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

#ifndef CIDER_STRUCT_BATCH_H
#define CIDER_STRUCT_BATCH_H

#include <vector>
#include "CiderBatch.h"

class StructBatch final : public CiderBatch {
 public:
  static std::unique_ptr<StructBatch> Create(ArrowSchema* schema,
                                             std::shared_ptr<CiderAllocator> allocator,
                                             ArrowArray* array = nullptr) {
    return array ? std::make_unique<StructBatch>(schema, array, allocator)
                 : std::make_unique<StructBatch>(schema, allocator);
  }

  explicit StructBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, allocator) {
    checkArrowEntries();
  }
  explicit StructBatch(ArrowSchema* schema,
                       ArrowArray* array,
                       std::shared_ptr<CiderAllocator> allocator)
      : CiderBatch(schema, array, allocator) {
    checkArrowEntries();
  }

 protected:
  bool resizeData(int64_t size) override {
    CHECK(!isMoved());
    if (!permitBufferAllocate()) {
      return false;
    }

    setLength(size);

    return true;
  }

 private:
  void checkArrowEntries() const { CHECK_EQ(getBufferNum(), 1); }
};

#endif
