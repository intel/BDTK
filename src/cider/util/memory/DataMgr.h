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
 * @file    DataMgr.h
 */
#ifndef DATAMGR_H
#define DATAMGR_H

#include "AbstractBufferMgr.h"
#include "DataMgrBufferProvider.h"
#include "DataMgrDataProvider.h"
#include "type/schema/ColumnInfo.h"
#include "util/MemoryInfo.h"
#include "util/MemoryLevel.h"
#include "util/mapd_shared_mutex.h"
#include "util/memory/Buffer/AbstractBuffer.h"
#include "util/memory/BufferMgr/Buffer.h"
#include "util/memory/BufferMgr/BufferMgr.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace File_Namespace {
class FileBuffer;
}  // namespace File_Namespace

namespace Buffer_Namespace {
class CpuBufferMgr;
}

struct DictDescriptor;

namespace Data_Namespace {

//! Parse /proc/meminfo into key/value pairs.
class ProcMeminfoParser {
  std::unordered_map<std::string, size_t> items_;

 public:
  ProcMeminfoParser() {
    std::ifstream f("/proc/meminfo");
    std::stringstream ss;
    ss << f.rdbuf();

    for (const std::string& line : split(ss.str(), "\n")) {
      if (line.empty()) {
        continue;
      }
      const auto nv = split(line, ":", 1);
      CHECK(nv.size() == 2) << "unexpected line format in /proc/meminfo: " << line;
      const auto name = strip(nv[0]), value = to_lower(strip(nv[1]));
      auto v = split(value);
      CHECK(v.size() == 1 || v.size() == 2)
          << "unexpected line format in /proc/meminfo: " << line;
      items_[name] = std::atoll(v[0].c_str());
      if (v.size() == 2) {
        CHECK(v[1] == "kb") << "unexpected unit suffix in /proc/meminfo: " << line;
        items_[name] *= 1024;
      }
    }
  }

  auto operator[](const std::string& name) { return items_[name]; }
  auto begin() { return items_.begin(); }
  auto end() { return items_.end(); }
};

//! Parse /proc/buddyinfo into a Fragmentation health score.
class ProcBuddyinfoParser {
  std::string inputText_;
  std::vector<size_t> orders_;
  size_t fragmentationPercent_;

 public:
  ProcBuddyinfoParser(std::string text = {}) {
    if (text.empty()) {
      std::ifstream f("/proc/buddyinfo");
      std::stringstream ss;
      ss << f.rdbuf();
      text = ss.str();
    }
    inputText_ = text;

    const size_t skipped_columns = 4;
    // NOTE(sy): For now this calculation ignores the first four buddyinfo columns,
    // but in the future we could break out subscores by node and/or by zone.
    size_t number_of_columns = 0;
    for (const std::string& line : split(text, "\n")) {
      if (line.empty()) {
        continue;
      }
      const auto columns = split(line);
      CHECK_GT(columns.size(), skipped_columns) << "unexpected line format: " << line;
      if (number_of_columns != 0) {
        CHECK_EQ(columns.size(), number_of_columns)
            << "expected line to have " << number_of_columns << " columns: " << line;
      } else {
        number_of_columns = columns.size();
        orders_.resize(number_of_columns - skipped_columns, 0);
      }
      for (size_t i = skipped_columns; i < number_of_columns; ++i) {
        orders_[i - skipped_columns] += strtoull(columns[i].c_str(), NULL, 10);
      }
    }

    const long page_size =
        sysconf(_SC_PAGE_SIZE);  // in case x86-64 is configured to use 2MB pages
    size_t scaled = 0;
    size_t total = 0;
    for (size_t order = 0; order < orders_.size(); ++order) {
      const size_t bytes = orders_[order] * (size_t(1) << order) * page_size;
      scaled += (bytes * (orders_.size() - 1 - order)) / (orders_.size() - 1);
      total += bytes;
    }

    CHECK_GT(total, size_t(0)) << "failed to parse:\n" << text;
    fragmentationPercent_ = (scaled * 100) / total;
  }

  auto operator[](size_t order) { return orders_[order]; }
  auto begin() { return orders_.begin(); }
  auto end() { return orders_.end(); }
  auto getFragmentationPercent() { return fragmentationPercent_; }
  auto getInputText() { return inputText_; }
};

class DataMgr {
 public:
  explicit DataMgr(const std::string& dataDir,
                   const size_t numReaderThreads =
                       0 /* 0 means use default for # of reader threads */);
  ~DataMgr();
  AbstractBuffer* createChunkBuffer(const ChunkKey& key,
                                    const MemoryLevel memoryLevel,
                                    const int deviceId = 0,
                                    const size_t page_size = 0);
  AbstractBuffer* getChunkBuffer(const ChunkKey& key,
                                 const MemoryLevel memoryLevel,
                                 const int deviceId = 0,
                                 const size_t numBytes = 0);
  void deleteChunksWithPrefix(const ChunkKey& keyPrefix);
  void deleteChunksWithPrefix(const ChunkKey& keyPrefix, const MemoryLevel memLevel);
  AbstractBuffer* alloc(const MemoryLevel memoryLevel,
                        const int deviceId,
                        const size_t numBytes);
  void free(AbstractBuffer* buffer);
  // copies one buffer to another
  void copy(AbstractBuffer* destBuffer, AbstractBuffer* srcBuffer);
  bool isBufferOnDevice(const ChunkKey& key,
                        const MemoryLevel memLevel,
                        const int deviceId);
  std::vector<MemoryInfo> getMemoryInfo(const MemoryLevel memLevel);
  std::string dumpLevel(const MemoryLevel memLevel);
  void clearMemory(const MemoryLevel memLevel);

  const std::map<ChunkKey, File_Namespace::FileBuffer*>& getChunkMap();
  void checkpoint(const int db_id,
                  const int tb_id);  // checkpoint for individual table of DB
  void checkpoint(const int db_id, const int table_id, const MemoryLevel memory_level);
  void getChunkMetadataVecForKeyPrefix(ChunkMetadataVector& chunkMetadataVec,
                                       const ChunkKey& keyPrefix);
  void removeTableRelatedDS(const int db_id, const int tb_id);

  // database_id, table_id, column_id, fragment_id
  std::vector<int> levelSizes_;

  struct SystemMemoryUsage {
    size_t free;      // available CPU RAM memory in bytes
    size_t total;     // total CPU RAM memory in bytes
    size_t resident;  // resident process memory in bytes
    size_t vtotal;    // total process virtual memory in bytes
    size_t regular;   // process bytes non-shared
    size_t shared;    // process bytes shared (file maps + shmem)
    size_t frag;      // fragmentation percent
  };

  SystemMemoryUsage getSystemMemoryUsage() const;
  static size_t getTotalSystemMemory();

  // Used for testing.
  Buffer_Namespace::CpuBufferMgr* getCpuBufferMgr() const;

  const DictDescriptor* getDictMetadata(int db_id,
                                        int dict_id,
                                        bool load_dict = true) const;

  Fragmenter_Namespace::TableInfo getTableMetadata(int db_id, int table_id) const;

  BufferProvider* getBufferProvider() const { return buffer_provider_.get(); }

  DataProvider* getDataProvider() const { return data_provider_.get(); }

 private:
  void checkpoint();  // checkpoint for whole DB, called from convertDB proc only
  void allocateCpuBufferMgr(int32_t device_id,
                            size_t total_cpu_size,
                            size_t minCpuSlabSize,
                            size_t maxCpuSlabSize,
                            size_t page_size,
                            const std::vector<size_t>& cpu_tier_sizes);

  std::vector<std::vector<AbstractBufferMgr*>> bufferMgrs_;
  std::string dataDir_;
  std::mutex buffer_access_mutex_;
  std::unique_ptr<DataMgrBufferProvider> buffer_provider_;
  std::unique_ptr<DataMgrDataProvider> data_provider_;
};

std::ostream& operator<<(std::ostream& os, const DataMgr::SystemMemoryUsage&);

}  // namespace Data_Namespace

#endif  // DATAMGR_H
