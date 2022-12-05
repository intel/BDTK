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
#ifndef NEXTGEN_CONTEXT_BUFFER_H
#define NEXTGEN_CONTEXT_BUFFER_H

#include "cider/CiderAllocator.h"

namespace cider::exec::nextgen::context {
class Buffer {
 public:
  Buffer(const int32_t capacity, const CiderAllocatorPtr& allocator)
      : capacity_(capacity)
      , allocator_(allocator)
      , buffer_(allocator_->allocate(capacity_)) {}

  ~Buffer() { allocator_->deallocate(buffer_, capacity_); }

  int8_t* getBuffer() { return buffer_; }

 private:
  int32_t capacity_;
  CiderAllocatorPtr allocator_;
  int8_t* buffer_;
};

using BufferPtr = std::unique_ptr<Buffer>;

}  // namespace cider::exec::nextgen::context
#endif  // NEXTGEN_CONTEXT_BUFFER_H
