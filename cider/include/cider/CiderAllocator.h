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

#ifndef CIDER_ALLOCATOR_H
#define CIDER_ALLOCATOR_H

#include <cstring>
#include <memory>

static constexpr uint16_t kNoAlignment = alignof(max_align_t);
static constexpr uint16_t kMaxAlignment = 64;
static constexpr size_t kMaxMemory = std::numeric_limits<size_t>::max();

// Cider User can define new allocator which allcatoe memory from their pool
class CiderAllocator {
 public:
  virtual int8_t* allocate(size_t size) = 0;
  virtual void deallocate(int8_t* p, size_t size) = 0;

  virtual int8_t* reallocate(int8_t* p, size_t size, size_t newSize) {
    int8_t* newP = allocate(newSize);
    std::memmove(newP, p, size);
    deallocate(p, size);
    return newP;
  }
  virtual size_t getCap() { return kMaxMemory; }
  virtual size_t getMemoryUsage() { return 0; }
};

// use std::allocator<int8_t> as default allocator
class CiderDefaultAllocator : public CiderAllocator {
 public:
  int8_t* allocate(size_t size) final { return allocator_.allocate(size); }
  void deallocate(int8_t* p, size_t size) final { allocator_.deallocate(p, size); }

 private:
  std::allocator<int8_t> allocator_{};
};

template <uint16_t ALIGNMENT = kNoAlignment>
class AlignAllocator : public CiderAllocator {
 public:
  explicit AlignAllocator(std::shared_ptr<CiderAllocator> parent) : parent_(parent) {}

  int8_t* allocate(size_t size) final {
    if constexpr (ALIGNMENT <= kNoAlignment || ALIGNMENT > kMaxAlignment) {
      return parent_->allocate(size);
    }

    // |            ALIGNMENT            |
    //     |         offset              |
    // |                |     1 byte     |              size                 |
    // |---|------------|--offset value--|-----------------------------------|
    //     p                            ret

    // calc actual need size
    size_t alloc_size = ALIGNMENT + size;
    // do allocate
    int8_t* p = parent_->allocate(alloc_size);
    // calc return address
    int8_t* ret = alignAddress(p);
    // calc offset
    uint8_t offset = (uint64_t)ret - (uint64_t)p;
    // save offset in the one byte before ret
    *(uint8_t*)(ret - 1) = offset;

    return ret;
  }

  void deallocate(int8_t* p, size_t size) final {
    if constexpr (ALIGNMENT > kNoAlignment && ALIGNMENT <= kMaxAlignment) {
      uint8_t offset = *(uint8_t*)(p - 1);
      p = adjustAddress(p, offset);
    }
    parent_->deallocate(p, size);
    return;
  }

 private:
  int8_t* alignAddress(int8_t* p) const noexcept {
    // if p happen to aligned, return next aligned address.
    // keep space for save offset in the head.
    return reinterpret_cast<int8_t*>((reinterpret_cast<uint64_t>(p) + ALIGNMENT) &
                                     ~(ALIGNMENT - 1));
  }
  int8_t* adjustAddress(int8_t* p, uint8_t offset) {
    return reinterpret_cast<int8_t*>(reinterpret_cast<uint64_t>(p) - offset);
  }

  std::shared_ptr<CiderAllocator> parent_;
};
#endif
