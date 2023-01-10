/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include "CpuBuffer.h"

#include <cassert>
#include <cstring>
#include "util/Logger.h"

namespace Buffer_Namespace {

CpuBuffer::CpuBuffer(BufferMgr* bm,
                     BufferList::iterator segment_iter,
                     const int device_id,
                     const size_t page_size,
                     const size_t num_bytes)
    : Buffer(bm, segment_iter, device_id, page_size, num_bytes) {}

void CpuBuffer::readData(int8_t* const dst,
                         const size_t num_bytes,
                         const size_t offset,
                         const MemoryLevel dst_memory_level,
                         const int dst_device_id) {
  if (dst_memory_level == CPU_LEVEL) {
    memcpy(dst, mem_ + offset, num_bytes);
  } else {
    LOG(FATAL) << "Unsupported buffer type";
  }
}

void CpuBuffer::writeData(int8_t* const src,
                          const size_t num_bytes,
                          const size_t offset,
                          const MemoryLevel src_memory_level,
                          const int src_device_id) {
  if (src_memory_level == CPU_LEVEL) {
    // std::cout << "Writing to CPU from source CPU" << std::endl;
    memcpy(mem_ + offset, src, num_bytes);
  } else {
    LOG(FATAL) << "Unsupported buffer type";
  }
}

}  // namespace Buffer_Namespace
