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

#ifndef CIDER_ALLOCATOR_H
#define CIDER_ALLOCATOR_H

#include <cstring>
#include <memory>

#include "cider/CiderException.h"

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

using CiderAllocatorPtr = std::shared_ptr<CiderAllocator>;

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
    if (p) {
      if constexpr (ALIGNMENT > kNoAlignment && ALIGNMENT <= kMaxAlignment) {
        uint8_t offset = *(uint8_t*)(p - 1);
        p = adjustAddress(p, offset);
      }
      parent_->deallocate(p, size);
      return;
    }
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

class AllocatedData {
 public:
  AllocatedData(CiderAllocator* allocator, int8_t* pointer, size_t allocated_size)
      : allocator_(allocator), pointer_(pointer), allocated_size_(allocated_size) {}
  ~AllocatedData() { reset(); }

  int8_t* get() { return pointer_; }
  const int8_t* get() const { return pointer_; }
  size_t getSize() const { return allocated_size_; }
  void reset() {
    if (!pointer_) {
      return;
    }
    allocator_->deallocate(pointer_, allocated_size_);
    pointer_ = nullptr;
  }

 private:
  CiderAllocator* allocator_;
  int8_t* pointer_;
  size_t allocated_size_;
};

struct ArenaChunk {
  ArenaChunk(CiderAllocator* allocator, size_t size)
      : current_position(0)
      , maximum_size(size)
      , prev(nullptr)
      , data(allocator, allocator->allocate(size), size) {}
  ~ArenaChunk() {
    if (next) {
      auto current_next = std::move(next);
      while (current_next) {
        current_next = std::move(current_next->next);
      }
    }
  }

  AllocatedData data;
  size_t current_position;
  size_t maximum_size;
  std::unique_ptr<ArenaChunk> next;
  ArenaChunk* prev;
};

class CiderArenaAllocator : public CiderAllocator {
  static constexpr const size_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 2048;

 public:
  CiderArenaAllocator(
      std::shared_ptr<CiderAllocator> parent = std::make_shared<CiderDefaultAllocator>(),
      size_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY)
      : parent_(parent) {
    head_ = nullptr;
    tail_ = nullptr;
    current_arena_capacity_ = initial_capacity;
  }

  int8_t* allocate(size_t len) final {
    if (!head_ || head_->current_position + len > head_->maximum_size) {
      do {
        current_arena_capacity_ *= 2;
      } while (current_arena_capacity_ < len);
      auto new_chunk =
          std::make_unique<ArenaChunk>(parent_.get(), current_arena_capacity_);
      total_capacity_ += current_arena_capacity_;
      if (head_) {
        head_->prev = new_chunk.get();
        new_chunk->next = move(head_);
      } else {
        tail_ = new_chunk.get();
      }
      head_ = move(new_chunk);
    }
    auto result = head_->data.get() + head_->current_position;
    head_->current_position += len;
    return result;
  }

  void deallocate(int8_t* p, size_t size) final {
    CIDER_THROW(CiderUnsupportedException,
                "Arena allocator does not support deallocate!");
  }

  int8_t* reallocate(int8_t* p, size_t size, size_t newSize) final {
    CIDER_THROW(CiderUnsupportedException,
                "Arena allocator does not support reallocate!");
  }
  size_t getCap() override { return total_capacity_; }

  void destory() {
    head_ = nullptr;
    tail_ = nullptr;
    current_arena_capacity_ = ARENA_ALLOCATOR_INITIAL_CAPACITY;
    total_capacity_ = 0;
  }

 private:
  size_t current_arena_capacity_;
  size_t total_capacity_ = 0;
  std::unique_ptr<ArenaChunk> head_;
  ArenaChunk* tail_;
  std::shared_ptr<CiderAllocator> parent_;
};

#endif
