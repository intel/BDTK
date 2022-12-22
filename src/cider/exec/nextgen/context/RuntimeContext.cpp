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

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/operators/extractor/AggExtractorBuilder.h"

namespace cider::exec::nextgen::context {
void RuntimeContext::addBatch(const CodegenContext::BatchDescriptorPtr& descriptor) {
  batch_holder_.emplace_back(descriptor, nullptr);
}

void RuntimeContext::addBuffer(const CodegenContext::BufferDescriptorPtr& descriptor) {
  buffer_holder_.emplace_back(descriptor, nullptr);
}

void RuntimeContext::instantiate(const CiderAllocatorPtr& allocator) {
  // Instantiation of batches.
  for (auto& batch_desc : batch_holder_) {
    if (nullptr == batch_desc.second) {
      batch_desc.second = std::make_unique<Batch>(batch_desc.first->type, allocator);
      runtime_ctx_pointers_[batch_desc.first->ctx_id] = batch_desc.second.get();
    }
  }

  // Instantiation of buffers.
  for (auto& buffer_desc : buffer_holder_) {
    if (nullptr == buffer_desc.second) {
      buffer_desc.second = std::make_unique<Buffer>(
          buffer_desc.first->capacity, allocator, buffer_desc.first->initializer_);
      runtime_ctx_pointers_[buffer_desc.first->ctx_id] = buffer_desc.second.get();
    }
  }

  string_heap_ptr_ = std::make_shared<StringHeap>(allocator);
}

void allocateBatchMem(ArrowArray* array,
                      int64_t length,
                      bool is_struct = true,
                      int64_t value_size = 0) {
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);
  // null buffer (size rounded up)
  holder->allocBuffer(0, (length + 7) / 8);
  auto child_null_buffer = holder->getBufferAs<int8_t>(0);
  memset(child_null_buffer, 0xFF, (length + 7) / 8);

  // value buffer
  if (!is_struct) {
    holder->allocBuffer(1, value_size * length);
  }

  // set length
  array->length = length;
}

Batch* RuntimeContext::getNonGroupByAggOutputBatch() {
  AggExprsInfoVector& info = reinterpret_cast<CodegenContext::AggBufferDescriptor*>(
                                 buffer_holder_.back().first.get())
                                 ->info_;
  int8_t* buf = buffer_holder_.back().second->getBuffer();
  Batch* batch = batch_holder_.back().second.get();

  // allocate mem
  // row struct
  auto arrow_array = batch->getArray();
  allocateBatchMem(arrow_array, 1);

  // child value
  for (size_t i = 0; i < arrow_array->n_children; i++) {
    auto child_array = arrow_array->children[i];
    allocateBatchMem(child_array, 1, false, info[i].sql_type_info_.get_size());
  }

  std::vector<std::unique_ptr<operators::NextgenAggExtractor>> non_groupby_agg_extractors;
  non_groupby_agg_extractors.reserve(info.size());
  auto null_buffer_start_ = info.back().start_offset_ + info.back().byte_size_;

  for (size_t i = 0; i < info.size(); ++i) {
    info[i].null_offset_ = null_buffer_start_ + i;
    std::unique_ptr<operators::NextgenAggExtractor> extractor =
        operators::NextgenAggExtractorBuilder::buildNextgenAggExtractor(buf, info[i]);
    extractor->extract({buf}, arrow_array->children[i]);
  }

  return batch;
}

}  // namespace cider::exec::nextgen::context
