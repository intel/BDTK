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
#ifndef NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
#define NEXTGEN_CONTEXT_RUNTIMECONTEXT_H

#include <cstddef>
#include <cstdint>

#include "exec/nextgen/context/ContextDescriptors-fwd.h"

class CiderAllocator;
class StringHeap;
class SQLTypeInfo;
using CiderAllocatorPtr = std::shared_ptr<CiderAllocator>;

namespace cider::exec::nextgen::context {

class RuntimeContext {
 public:
  explicit RuntimeContext(int64_t ctx_num) : runtime_ctx_pointers_(ctx_num, nullptr) {}

  size_t getContextItemNum() const { return runtime_ctx_pointers_.size(); }

  void* getContextItem(size_t id) { return runtime_ctx_pointers_[id]; }

  void* getStringHeapPtr() { return string_heap_ptr_.get(); }

  void addBatch(const BatchDescriptorPtr& descriptor);

  void addBuffer(const BufferDescriptorPtr& descriptor);

  void addHashTable(const HashTableDescriptorPtr& descriptor);

  void addBuildTable(const BuildTableDescriptorPtr& descriptor);

  void addCiderSet(const CiderSetDescriptorPtr& descriptor);

  void instantiate(const CiderAllocatorPtr& allocator);

  const int8_t* getTrimStringOperCharMapById(int id) const;

  void setTrimStringOperCharMaps(const TrimCharMapsPtr& maps);

  Batch* getOutputBatch();

  Batch* getNonGroupByAggOutputBatch();

  void resetBatch(const CiderAllocatorPtr& allocator,
                  const ArrowArray& array,
                  const ArrowSchema& schema);

  void destroyStringHeap();

 private:
  std::vector<void*> runtime_ctx_pointers_;
  std::vector<std::pair<BatchDescriptorPtr, BatchPtr>> batch_holder_;
  std::vector<std::pair<BufferDescriptorPtr, BufferPtr>> buffer_holder_;
  std::vector<std::pair<CiderSetDescriptorPtr, CiderSetPtr>> cider_set_holder_;
  std::shared_ptr<StringHeap> string_heap_ptr_;
  HashTableDescriptorPtr hashtable_holder_;
  BuildTableDescriptorPtr buildtable_holder_;
  TrimCharMapsPtr trim_char_maps_;

 public:
  // bare column map
  // output column id to input column id;
  std::unordered_map<int, int> bare_output_input_map_;
};

using RuntimeCtxPtr = std::unique_ptr<RuntimeContext>;
}  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_RUNTIMECONTEXT_H
