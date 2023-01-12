/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) 2016-2022 ClickHouse, Inc.
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

#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include <boost/noncopyable.hpp>

namespace cider {

/** Memory pool to append something. For example, short strings.
 * Usage scenario:
 * - put lot of strings inside pool, keep their addresses;
 * - addresses remain valid during lifetime of pool;
 * - at destruction of pool, all memory is freed;
 * - memory is allocated and freed by large MemoryChunks;
 * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
 */
class Arena : private boost::noncopyable {
 private:
  static size_t roundUpToPageSize(size_t s, size_t page_size) { return 0; }

  /// If MemoryChunks size is less than 'linear_growth_threshold', then use exponential
  /// growth, otherwise - linear growth
  ///  (to not allocate too much excessive memory).
  size_t nextSize(size_t min_next_size) const { return 0; }

  /// Add next contiguous MemoryChunk of memory with size not less than specified.
  void addMemoryChunk(size_t min_size) {}

 public:
  /// Get piece of memory, without alignment.
  char* alloc(size_t size) { return nullptr; }

  /// Get piece of memory with alignment
  char* alignedAlloc(size_t size, size_t alignment) { return nullptr; }

  // TODO: Implement and enable later
  template <typename T>
  T* alloc() {
    return nullptr;
  }

  /** Rollback just performed allocation.
   * Must pass size not more that was just allocated.
   * Return the resulting head pointer, so that the caller can assert that
   * the allocation it intended to roll back was indeed the last one.
   */
  void* rollback(size_t size) { return nullptr; }

  /** Begin or expand a contiguous range of memory.
   * 'range_start' is the start of range. If nullptr, a new range is
   * allocated.
   * If there is no space in the current MemoryChunk to expand the range,
   * the entire range is copied to a new, bigger memory MemoryChunk, and the value
   * of 'range_start' is updated.
   * If the optional 'start_alignment' is specified, the start of range is
   * kept aligned to this value.
   *
   * NOTE This method is usable only for the last allocation made on this
   * Arena. For earlier allocations, see 'realloc' method.
   */
  char* allocContinue(size_t additional_bytes,
                      char const*& range_start,
                      size_t start_alignment = 0) {
    // TODO: Implement and enable later
    return nullptr;
  }

  // TODO: Implement and enable later
  /// NOTE Old memory region is wasted.
  char* realloc(const char* old_data, size_t old_size, size_t new_size) {
    return nullptr;
  }

  // TODO: Implement and enable later
  char* alignedRealloc(const char* old_data,
                       size_t old_size,
                       size_t new_size,
                       size_t alignment) {
    return nullptr;
  }

  /// Insert string without alignment.
  const char* insert(const char* data, size_t size) { return nullptr; }

  const char* alignedInsert(const char* data, size_t size, size_t alignment) {
    return nullptr;
  }

  /// Size of MemoryChunks in bytes.
  size_t size() const { return 0; }

  /// Bad method, don't use it -- the MemoryChunks are not your business, the entire
  /// purpose of the arena code is to manage them for you, so if you find
  /// yourself having to use this method, probably you're doing something wrong.
  size_t remainingSpaceInCurrentMemoryChunk() const { return 0; }
};

}  // namespace cider
