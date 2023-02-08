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

#ifndef CIDER_VELOX_ALLOCATOR_H
#define CIDER_VELOX_ALLOCATOR_H

#include "cider/CiderAllocator.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::plugin {

class PoolAllocator : public CiderAllocator {
 public:
  explicit PoolAllocator(memory::MemoryPool* pool) : pool_(pool) {}

  int8_t* allocate(size_t size) final {
    return reinterpret_cast<int8_t*>(pool_->allocate(size));
  }

  void deallocate(int8_t* p, size_t size) final { pool_->free(p, size); }

  int8_t* reallocate(int8_t* p, size_t size, size_t newSize) final {
    return reinterpret_cast<int8_t*>(pool_->reallocate(p, size, newSize));
  }

 private:
  memory::MemoryPool* pool_;
};

}  // namespace facebook::velox::plugin
#endif  // CIDER_VELOX_ALLOCATOR_H
