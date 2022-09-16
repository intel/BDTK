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

#define CIDERBATCH_WITH_ARROW

#include <vector>
#include "CiderBatch.h"

class StructBatch final : public CiderBatch {
 public:
  static std::unique_ptr<StructBatch> Create(ArrowSchema* schema,
                                             ArrowArray* array = nullptr) {
    return array ? std::make_unique<StructBatch>(schema, array)
                 : std::make_unique<StructBatch>(schema);
  }

  explicit StructBatch(ArrowSchema* schema) : CiderBatch(schema) { checkArrowEntries(); }
  explicit StructBatch(ArrowSchema* schema, ArrowArray* array)
      : CiderBatch(schema, array) {
    checkArrowEntries();
  }

  bool resizeBatch(int64_t size, bool default_not_null = false) override {
    CHECK(!isMoved());
    if (!permitBufferAllocate()) {
      return false;
    }

    bool ret = resizeNullVector(0, size, default_not_null);
    setLength(size);

    return ret;
  }

 private:
  void checkArrowEntries() const { CHECK_EQ(getBufferNum(), 1); }
};

#endif
