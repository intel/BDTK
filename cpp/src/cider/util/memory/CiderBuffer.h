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

#ifndef CIDER_CIDERBUFFER_H
#define CIDER_CIDERBUFFER_H

#include "util/MemoryLevel.h"
#include "util/memory/Buffer/AbstractBuffer.h"

#include <cassert>
#include <stdexcept>

#include "util/Logger.h"

// ColumnFetcher needs a Chunk, so we impl this class derived from AbstractBuffer to build
// a Chunk. Ideal usage of this class will be where HashJoin is built. This class will
// only provide getMemoryPtr method. Other methods should be UNREACHABLE and never be
// called in our code path.
class CiderBuffer : public AbstractBuffer {
 public:
  explicit CiderBuffer(const int8_t* mem) : AbstractBuffer(-1), mem_(mem) {}
  ~CiderBuffer() {}

  inline Data_Namespace::MemoryLevel getType() const override {
    return Data_Namespace::CPU_LEVEL;
  }

  void read(int8_t* const dst,
            const size_t num_bytes,
            const size_t offset = 0,
            const MemoryLevel dst_buffer_type = Data_Namespace::CPU_LEVEL,
            const int device_id = -1) override {
    UNREACHABLE();
  }

  void write(int8_t* src,
             const size_t num_bytes,
             const size_t offset = 0,
             const MemoryLevel src_buffer_type = Data_Namespace::CPU_LEVEL,
             const int device_id = -1) override {
    UNREACHABLE();
  }

  void append(int8_t* src,
              const size_t num_bytes,
              const MemoryLevel src_buffer_type = Data_Namespace::CPU_LEVEL,
              const int deviceId = -1) override {
    UNREACHABLE();
  }

  void reserve(const size_t num_bytes) override { UNREACHABLE(); };

  /**
   * @brief Returns a raw, constant (read-only) pointer to the underlying buffer.
   * @return A constant memory pointer for read-only access.
   * const_cast is dangerous, but it would be fine if caller treat it as constant.
   */
  int8_t* getMemoryPtr() override { return const_cast<int8_t*>(mem_); };

  void setMemoryPtr(int8_t* new_ptr) override { UNREACHABLE(); }
  /// Returns the total number of bytes allocated for the buffer.
  inline size_t reservedSize() const override { UNREACHABLE(); }

  /// Returns the number of pages in the buffer.
  inline size_t pageCount() const override { UNREACHABLE(); }

  /// Returns the size in bytes of each page in the buffer.
  inline size_t pageSize() const override { UNREACHABLE(); }

  inline int pin() override { return (++pin_count_); }
  inline int unPin() override { return (--pin_count_); }
  inline int getPinCount() override { return (pin_count_); }

 private:
  const int8_t* mem_;  /// pointer to beginning of buffer's memory
  std::atomic_int32_t pin_count_ = 0;
  size_t reservedSize_;
};

#endif  // CIDER_CIDERBUFFER_H
