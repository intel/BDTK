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

#ifndef CIDER_CIDERAGGSPILLBUFFERMGR_H
#define CIDER_CIDERAGGSPILLBUFFERMGR_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

class CiderAggSpillFile;

class CiderAggSpillBufferMgr {
 public:
  enum BufferMode { RMODE, WMODE, RWMODE };

  CiderAggSpillBufferMgr(BufferMode buffer_mode,
                         bool need_dump,
                         const std::shared_ptr<CiderAggSpillFile>& spill_file = nullptr,
                         size_t init_partition_index = 0,
                         size_t page_num_per_partition = 512);
  ~CiderAggSpillBufferMgr();

  int8_t* getBuffer() { return static_cast<int8_t*>(addr_); }
  std::shared_ptr<CiderAggSpillFile> getSpillFile() { return spill_file_; }
  size_t getPartitionSize() const { return partition_size_; }
  void* toNextPartition();
  void* toPrevPartition();
  void* toPartitionAt(size_t index);
  void syncBuffer();

  constexpr static size_t PAGESIZE = 4 * 1024;

 private:
  void mapCurrentPartition();

  const BufferMode mode_;
  size_t partition_size_;  // Size of spill partition, byte.
  std::shared_ptr<CiderAggSpillFile> spill_file_;
  size_t curr_partition_index_;

  void* addr_;
  int prot_;
  int flags_;
};

class CiderAggSpillFile {
 public:
  CiderAggSpillFile(const std::string& fname, size_t partition_size);
  ~CiderAggSpillFile();

  int getFd() const { return fd_; }
  size_t getFileSize() const { return file_size_; }
  size_t getPartitionSize() const { return partition_size_; }
  size_t getPartitionNum() const { return partition_num_; }

  void resizeSpillFile(size_t partition_num);

  static std::string getBasePath();

 private:
  const std::string fname_;
  int fd_;

  const size_t partition_size_;
  size_t partition_num_;
  size_t file_size_;
};

#endif
