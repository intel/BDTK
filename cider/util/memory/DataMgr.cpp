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

/**
 * @file    DataMgr.cpp
 */

#include "DataMgr.h"
#include <algorithm>
#include <boost/filesystem.hpp>
#include <limits>
#include "util/memory/BufferMgr/CpuBufferMgr/CpuBufferMgr.h"
#include "util/memory/BufferMgr/CpuBufferMgr/TieredCpuBufferMgr.h"

extern bool g_enable_fsi;
bool g_enable_tiered_cpu_mem{false};
size_t g_pmem_size{0};

namespace Data_Namespace {

DataMgr::DataMgr(const std::string& dataDir, const size_t numReaderThreads)
    : dataDir_(dataDir)
    , buffer_provider_(std::make_unique<DataMgrBufferProvider>(this))
    , data_provider_(std::make_unique<DataMgrDataProvider>(this)) {}

DataMgr::~DataMgr() {
  int numLevels = bufferMgrs_.size();
  for (int level = numLevels - 1; level >= 0; --level) {
    for (size_t device = 0; device < bufferMgrs_[level].size(); device++) {
      delete bufferMgrs_[level][device];
    }
  }
}

DataMgr::SystemMemoryUsage DataMgr::getSystemMemoryUsage() const {
  SystemMemoryUsage usage;

#ifdef __linux__

  // Determine Linux available memory and total memory.
  // Available memory is different from free memory because
  // when Linux sees free memory, it tries to use it for
  // stuff like disk caching. However, the memory is not
  // reserved and is still available to be allocated by
  // user processes.
  // Parsing /proc/meminfo for this info isn't very elegant
  // but as a virtual file it should be reasonably fast.
  // See also:
  //   https://github.com/torvalds/linux/commit/34e431b0ae398fc54ea69ff85ec700722c9da773
  ProcMeminfoParser mi;
  usage.free = mi["MemAvailable"];
  usage.total = mi["MemTotal"];

  // Determine process memory in use.
  // See also:
  //   https://stackoverflow.com/questions/669438/how-to-get-memory-usage-at-runtime-using-c
  //   http://man7.org/linux/man-pages/man5/proc.5.html
  int64_t size = 0;
  int64_t resident = 0;
  int64_t shared = 0;

  std::ifstream fstatm("/proc/self/statm");
  fstatm >> size >> resident >> shared;
  fstatm.close();

  long page_size =
      sysconf(_SC_PAGE_SIZE);  // in case x86-64 is configured to use 2MB pages

  usage.resident = resident * page_size;
  usage.vtotal = size * page_size;
  usage.regular = (resident - shared) * page_size;
  usage.shared = shared * page_size;

  ProcBuddyinfoParser bi;
  usage.frag = bi.getFragmentationPercent();

#else

  usage.total = 0;
  usage.free = 0;
  usage.resident = 0;
  usage.vtotal = 0;
  usage.regular = 0;
  usage.shared = 0;
  usage.frag = 0;

#endif

  return usage;
}

size_t DataMgr::getTotalSystemMemory() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

void DataMgr::allocateCpuBufferMgr(int32_t device_id,
                                   size_t total_cpu_size,
                                   size_t minCpuSlabSize,
                                   size_t maxCpuSlabSize,
                                   size_t page_size,
                                   const CpuTierSizeVector& cpu_tier_sizes) {
  if (g_enable_tiered_cpu_mem) {
    bufferMgrs_[1].push_back(new Buffer_Namespace::TieredCpuBufferMgr(0,
                                                                      total_cpu_size,
                                                                      minCpuSlabSize,
                                                                      maxCpuSlabSize,
                                                                      page_size,
                                                                      cpu_tier_sizes,
                                                                      bufferMgrs_[0][0]));
  } else {
    bufferMgrs_[1].push_back(new Buffer_Namespace::CpuBufferMgr(
        0, total_cpu_size, minCpuSlabSize, maxCpuSlabSize, page_size, bufferMgrs_[0][0]));
  }
}

std::vector<MemoryInfo> DataMgr::getMemoryInfo(const MemoryLevel memLevel) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);

  std::vector<MemoryInfo> mem_info;
  if (memLevel == MemoryLevel::CPU_LEVEL) {
    Buffer_Namespace::CpuBufferMgr* cpu_buffer =
        dynamic_cast<Buffer_Namespace::CpuBufferMgr*>(
            bufferMgrs_[MemoryLevel::CPU_LEVEL][0]);
    CHECK(cpu_buffer);
    MemoryInfo mi;

    mi.pageSize = cpu_buffer->getPageSize();
    mi.maxNumPages = cpu_buffer->getMaxSize() / mi.pageSize;
    mi.isAllocationCapped = cpu_buffer->isAllocationCapped();
    mi.numPageAllocated = cpu_buffer->getAllocated() / mi.pageSize;

    const auto& slab_segments = cpu_buffer->getSlabSegments();
    for (size_t slab_num = 0; slab_num < slab_segments.size(); ++slab_num) {
      for (auto segment : slab_segments[slab_num]) {
        MemoryData md;
        md.slabNum = slab_num;
        md.startPage = segment.start_page;
        md.numPages = segment.num_pages;
        md.touch = segment.last_touched;
        md.memStatus = segment.mem_status;
        md.chunk_key.insert(
            md.chunk_key.end(), segment.chunk_key.begin(), segment.chunk_key.end());
        mi.nodeMemoryData.push_back(md);
      }
    }
    mem_info.push_back(mi);
  }
  return mem_info;
}

std::string DataMgr::dumpLevel(const MemoryLevel memLevel) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  return bufferMgrs_[memLevel][0]->printSlabs();
}

void DataMgr::clearMemory(const MemoryLevel memLevel) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  auto buffer_mgr_for_cpu =
      dynamic_cast<Buffer_Namespace::BufferMgr*>(bufferMgrs_[memLevel][0]);
  CHECK(buffer_mgr_for_cpu);
  buffer_mgr_for_cpu->clearSlabs();
}

bool DataMgr::isBufferOnDevice(const ChunkKey& key,
                               const MemoryLevel memLevel,
                               const int deviceId) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  return bufferMgrs_[memLevel][deviceId]->isBufferOnDevice(key);
}

void DataMgr::getChunkMetadataVecForKeyPrefix(ChunkMetadataVector& chunkMetadataVec,
                                              const ChunkKey& keyPrefix) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  bufferMgrs_[0][0]->getChunkMetadataVecForKeyPrefix(chunkMetadataVec, keyPrefix);
}

AbstractBuffer* DataMgr::createChunkBuffer(const ChunkKey& key,
                                           const MemoryLevel memoryLevel,
                                           const int deviceId,
                                           const size_t page_size) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  int level = static_cast<int>(memoryLevel);
  return bufferMgrs_[level][deviceId]->createBuffer(key, page_size);
}

AbstractBuffer* DataMgr::getChunkBuffer(const ChunkKey& key,
                                        const MemoryLevel memoryLevel,
                                        const int deviceId,
                                        const size_t numBytes) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  const auto level = static_cast<size_t>(memoryLevel);
  CHECK_LT(level, levelSizes_.size());     // make sure we have a legit buffermgr
  CHECK_LT(deviceId, levelSizes_[level]);  // make sure we have a legit buffermgr
  return bufferMgrs_[level][deviceId]->getBuffer(key, numBytes);
}

void DataMgr::deleteChunksWithPrefix(const ChunkKey& keyPrefix) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);

  int numLevels = bufferMgrs_.size();
  for (int level = numLevels - 1; level >= 0; --level) {
    for (int device = 0; device < levelSizes_[level]; ++device) {
      bufferMgrs_[level][device]->deleteBuffersWithPrefix(keyPrefix);
    }
  }
}

// only deletes the chunks at the given memory level
void DataMgr::deleteChunksWithPrefix(const ChunkKey& keyPrefix,
                                     const MemoryLevel memLevel) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);

  if (bufferMgrs_.size() <= memLevel) {
    return;
  }
  for (int device = 0; device < levelSizes_[memLevel]; ++device) {
    bufferMgrs_[memLevel][device]->deleteBuffersWithPrefix(keyPrefix);
  }
}

AbstractBuffer* DataMgr::alloc(const MemoryLevel memoryLevel,
                               const int deviceId,
                               const size_t numBytes) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  const auto level = static_cast<int>(memoryLevel);
  CHECK_LT(deviceId, levelSizes_[level]);
  return bufferMgrs_[level][deviceId]->alloc(numBytes);
}

void DataMgr::free(AbstractBuffer* buffer) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  int level = static_cast<int>(buffer->getType());
  bufferMgrs_[level][buffer->getDeviceId()]->free(buffer);
}

void DataMgr::copy(AbstractBuffer* destBuffer, AbstractBuffer* srcBuffer) {
  destBuffer->write(srcBuffer->getMemoryPtr(),
                    srcBuffer->size(),
                    0,
                    srcBuffer->getType(),
                    srcBuffer->getDeviceId());
}

// could add function below to do arbitrary copies between buffers

// void DataMgr::copy(AbstractBuffer *destBuffer, const AbstractBuffer *srcBuffer, const
// size_t numBytes, const size_t destOffset, const size_t srcOffset) {
//} /

void DataMgr::checkpoint(const int db_id, const int tb_id) {
  // TODO(adb): do we need a buffer mgr lock here?
  // MAT Yes to reduce Parallel Executor TSAN issues (and correctness for now)
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  for (auto levelIt = bufferMgrs_.rbegin(); levelIt != bufferMgrs_.rend(); ++levelIt) {
    // use reverse iterator so we start at CPU then DISK
    for (auto deviceIt = levelIt->begin(); deviceIt != levelIt->end(); ++deviceIt) {
      (*deviceIt)->checkpoint(db_id, tb_id);
    }
  }
}

void DataMgr::checkpoint(const int db_id,
                         const int table_id,
                         const MemoryLevel memory_level) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  CHECK_LT(static_cast<size_t>(memory_level), bufferMgrs_.size());
  CHECK_LT(static_cast<size_t>(memory_level), levelSizes_.size());
  for (int device_id = 0; device_id < levelSizes_[memory_level]; device_id++) {
    bufferMgrs_[memory_level][device_id]->checkpoint(db_id, table_id);
  }
}

void DataMgr::checkpoint() {
  // TODO(adb): SAA
  // MAT Yes to reduce Parallel Executor TSAN issues (and correctness for now)
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  for (auto levelIt = bufferMgrs_.rbegin(); levelIt != bufferMgrs_.rend(); ++levelIt) {
    // use reverse iterator so we start at CPU then DISK
    for (auto deviceIt = levelIt->begin(); deviceIt != levelIt->end(); ++deviceIt) {
      (*deviceIt)->checkpoint();
    }
  }
}

void DataMgr::removeTableRelatedDS(const int db_id, const int tb_id) {
  std::lock_guard<std::mutex> buffer_lock(buffer_access_mutex_);
  bufferMgrs_[0][0]->removeTableRelatedDS(db_id, tb_id);
}

std::ostream& operator<<(std::ostream& os, const DataMgr::SystemMemoryUsage& mem_info) {
  os << "jsonlog ";
  os << "{";
  os << " \"name\": \"CPU Memory Info\",";
  os << " \"TotalMB\": " << mem_info.total / (1024. * 1024.) << ",";
  os << " \"FreeMB\": " << mem_info.free / (1024. * 1024.) << ",";
  os << " \"ProcessMB\": " << mem_info.resident / (1024. * 1024.) << ",";
  os << " \"VirtualMB\": " << mem_info.vtotal / (1024. * 1024.) << ",";
  os << " \"ProcessPlusSwapMB\": " << mem_info.regular / (1024. * 1024.) << ",";
  os << " \"ProcessSharedMB\": " << mem_info.shared / (1024. * 1024.) << ",";
  os << " \"FragmentationPercent\": " << mem_info.frag;
  os << " }";
  return os;
}

Buffer_Namespace::CpuBufferMgr* DataMgr::getCpuBufferMgr() const {
  return dynamic_cast<Buffer_Namespace::CpuBufferMgr*>(bufferMgrs_[1][0]);
}

}  // namespace Data_Namespace
