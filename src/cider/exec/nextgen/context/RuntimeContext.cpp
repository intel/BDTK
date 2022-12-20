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
}  // namespace cider::exec::nextgen::context
