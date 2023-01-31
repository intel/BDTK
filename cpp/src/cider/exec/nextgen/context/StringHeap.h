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

#ifndef NEXTGEN_CONTEXT_STRING_HEAP_H
#define NEXTGEN_CONTEXT_STRING_HEAP_H

#include "cider/CiderAllocator.h"

struct string_t {
  string_t(const char* data, uint32_t len) {
    value.pointer.length = len;
    value.pointer.ptr = (char*)data;
  }
  char* getDataWriteable() const { return value.pointer.ptr; }
  const char* getDataUnsafe() const { return value.pointer.ptr; }
  size_t getSize() const { return value.pointer.length; }

 private:
  // TODO: may use velox/duckdb short string represetation in the future.
  union {
    struct {
      uint32_t length;
      char prefix[4];
      char* ptr;
    } pointer;
  } value;
};

class StringHeap {
 public:
  StringHeap(const CiderAllocatorPtr& parent_alloctor =
                 std::make_shared<CiderDefaultAllocator>())
      : allocator_(parent_alloctor), total_num_(0) {}
  ~StringHeap() { destroy(); }

  // Destroy all allocated buffer.
  void destroy() {
    allocator_.destory();
    total_num_ = 0;
  }

  // Add a string to the string heap, returns a pointer to the string
  string_t addString(const char* data, size_t len) { return addBlob(data, len); }
  // Add a string to the string heap, returns a pointer to the string
  string_t addString(const string_t& data) {
    return addString(data.getDataUnsafe(), data.getSize());
  }
  // Allocates space for an empty string of size "len" on the heap
  string_t emptyString(size_t len) {
    total_num_++;
    auto pointer = (const char*)allocator_.allocate(len);
    return string_t(pointer, len);
  }
  // Returns how many string stored in this heap
  size_t getNum() { return total_num_; }

 private:
  //! Add a blob to the string heap;
  string_t addBlob(const char* data, size_t len) {
    auto insert_string = emptyString(len);
    auto pointer = insert_string.getDataWriteable();
    memcpy(pointer, data, len);
    return insert_string;
  }

  CiderArenaAllocator allocator_;
  size_t total_num_;
};

#endif
