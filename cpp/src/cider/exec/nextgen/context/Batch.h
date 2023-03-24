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
#ifndef NEXTGEN_CONTEXT_BATCH_H
#define NEXTGEN_CONTEXT_BATCH_H

#include <memory>
#include <unordered_map>

#include "exec/module/batch/ArrowABI.h"
#include "include/cider/batch/CiderBatchUtils.h"

class CiderAllocator;
using CiderAllocatorPtr = std::shared_ptr<CiderAllocator>;

namespace cider::exec::nextgen::context {
class Batch {
 public:
  Batch(const SQLTypeInfo& type,
        const CiderAllocatorPtr& allocator,
        std::unordered_map<int, int> bare_output_input_map)
      : bare_output_input_map_(bare_output_input_map) {
    schema_.release = nullptr;
    array_.release = nullptr;
  }

  Batch(ArrowSchema& schema, ArrowArray& array) : schema_(schema), array_(array) {}

  Batch() {}

  ~Batch() { release(); }

  // output batch can reuse input_array's children
  // temporary batch(used for materialization) no need to reuse other array's children
  void reset(const SQLTypeInfo& type,
             const CiderAllocatorPtr& allocator,
             const ArrowArray* input_array = nullptr,
             const ArrowSchema* input_schema = nullptr);

  void move(ArrowSchema& schema, ArrowArray& array) {
    schema = schema_;
    array = array_;

    schema_.release = nullptr;
    array_.release = nullptr;
  }

  void release() {
    if (schema_.release) {
      schema_.release(&schema_);
    }
    schema_.release = nullptr;

    if (array_.release) {
      array_.release(&array_);
    }
    array_.release = nullptr;
  }

  bool isMoved() const { return schema_.release; }

  ArrowArray* getArray() { return &array_; }
  ArrowSchema* getSchema() { return &schema_; }

 private:
  ArrowSchema schema_;
  ArrowArray array_;

  // bare column map
  // output column id to input column id;
  std::unordered_map<int, int> bare_output_input_map_;
};

using BatchPtr = std::unique_ptr<Batch>;
using BatchSharedPtr = std::shared_ptr<Batch>;
}  // namespace cider::exec::nextgen::context
#endif  // NEXTGEN_CONTEXT_BATCH_H
