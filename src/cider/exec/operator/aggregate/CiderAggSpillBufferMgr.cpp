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

#include "exec/operator/aggregate/CiderAggSpillBufferMgr.h"

#include <fcntl.h>
#include <linux/mman.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cerrno>

#include <boost/filesystem.hpp>
#include <ctime>
#include <exception>
#include <string>

#include "util/Logger.h"

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

CiderAggSpillBufferMgr::CiderAggSpillBufferMgr(
    BufferMode buffer_mode,
    bool need_dump,
    const std::shared_ptr<CiderAggSpillFile>& spill_file,
    size_t init_partition_index,
    size_t page_num_per_partition)
    : mode_(buffer_mode)
    , partition_size_(page_num_per_partition * PAGESIZE)
    , spill_file_(spill_file)
    , curr_partition_index_(init_partition_index)
    , addr_(nullptr)
    , prot_(0)
    , flags_(0) {
  if (spill_file_ && curr_partition_index_ >= spill_file_->getPartitionNum()) {
    LOG(ERROR) << "Inital partition index out of range, index=" << curr_partition_index_
               << " partition num=" << spill_file_->getPartitionNum();
  }

  switch (mode_) {
    case RMODE:
      prot_ |= PROT_READ;
      if (!spill_file_) {
        LOG(ERROR) << "RMODE but no file to read.";
      }
      break;
    case WMODE:
      prot_ |= PROT_WRITE;
      break;
    default:
      prot_ |= PROT_READ | PROT_WRITE;
  }

  flags_ |= MAP_POPULATE;

  // if ((2 * 1024 * 1024) == partition_size_) {
  //   flags_ |= MAP_HUGETLB | MAP_HUGE_2MB;
  // }

  if (!need_dump) {
    // Cache Only
    flags_ |= MAP_ANONYMOUS | MAP_PRIVATE;
  } else {
    if (!spill_file_) {
      auto spill_file_name = std::rand();
      auto tid = gettid();
      time_t now = time(nullptr);
      spill_file_ = std::make_shared<CiderAggSpillFile>(
          std::to_string(tid) + "_" + std::to_string(now) + "_" +
              std::to_string(spill_file_name),
          partition_size_);
    } else {
      CHECK_EQ(partition_size_, spill_file_->getPartitionSize());
    }

    flags_ |= ((RMODE == mode_) ? MAP_PRIVATE : MAP_SHARED);
    if (WMODE == mode_) {
      flags_ |= MAP_NONBLOCK;
    }
  }

  addr_ = mmap(addr_,
               partition_size_,
               prot_,
               flags_,
               spill_file_ ? spill_file_->getFd() : -1,
               curr_partition_index_ * partition_size_);
  if (MAP_FAILED == addr_) {
    LOG(ERROR) << "Memory map failed, errno=" << errno;
  }
}

CiderAggSpillBufferMgr::~CiderAggSpillBufferMgr() {
  if (addr_ != nullptr && MAP_FAILED != addr_) {
    if (munmap(addr_, partition_size_)) {
      LOG(ERROR) << "Memory unmap failed, errno=" << errno;
    }
  }
}

void CiderAggSpillBufferMgr::syncBuffer() {
  if (spill_file_ && RMODE != mode_) {
    if (msync(addr_, partition_size_, MS_SYNC)) {
      LOG(ERROR) << "Sync memory failed, errno=" << errno;
    }
  }
}

void CiderAggSpillBufferMgr::mapCurrentPartition() {
  syncBuffer();
  addr_ = mmap(addr_,
               partition_size_,
               prot_,
               flags_ | MAP_FIXED,
               spill_file_->getFd(),
               curr_partition_index_ * partition_size_);
  if (MAP_FAILED == addr_) {
    LOG(ERROR) << "Memory map failed, errno=" << errno;
  }
}

void* CiderAggSpillBufferMgr::toNextPartition() {
  if (!spill_file_) {
    return nullptr;
  }

  ++curr_partition_index_;
  if (curr_partition_index_ >= spill_file_->getPartitionNum()) {
    spill_file_->resizeSpillFile(curr_partition_index_ + 1);
  }
  mapCurrentPartition();

  return addr_;
}

void* CiderAggSpillBufferMgr::toPrevPartition() {
  if (!spill_file_ || 0 == curr_partition_index_) {
    return nullptr;
  }

  --curr_partition_index_;
  mapCurrentPartition();

  return addr_;
}

void* CiderAggSpillBufferMgr::toPartitionAt(size_t index) {
  if (!spill_file_ || !(index >= 0 && index < spill_file_->getPartitionNum())) {
    return nullptr;
  }

  curr_partition_index_ = index;
  mapCurrentPartition();

  return addr_;
}

CiderAggSpillFile::CiderAggSpillFile(const std::string& fname, size_t partition_size)
    : fname_(fname)
    , fd_(-1)
    , partition_size_(partition_size)
    , partition_num_(1)
    , file_size_(partition_size) {
  using namespace boost::filesystem;
  if (!exists(getBasePath()) || !is_directory(getBasePath())) {
    if (!create_directory(getBasePath())) {
      LOG(ERROR) << "Create spill file dictionary: " << getBasePath() << " failed.";
    }
  }

  auto file_path = canonical(getBasePath()) / fname_;
  fd_ = open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

  if (fd_ < 0) {
    LOG(ERROR) << "Create spill file: " << file_path.native()
               << " failed, errno=" << errno;
  } else {
    LOG(DEBUG1) << "Create spill file: " << file_path.native() << " Fd=" << fd_;
  }

  resizeSpillFile(1);
}

CiderAggSpillFile::~CiderAggSpillFile() {
  using namespace boost::filesystem;
  auto file_path = canonical(getBasePath()) / fname_;

  if (close(fd_) < 0) {
    LOG(ERROR) << "Close spill file: " << file_path.native()
               << " failed, errno=" << errno;
  }
  if (remove(file_path)) {
    LOG(DEBUG1) << "Remove spill file: " << file_path.native();
  } else {
    LOG(ERROR) << "Remove spill file: " << file_path.native() << " failed.";
  }
}

void CiderAggSpillFile::resizeSpillFile(size_t partition_num) {
  if (ftruncate(fd_, partition_num * partition_size_)) {
    LOG(ERROR) << "Expand spill file: " << fname_ << " failed. Fd= " << fd_
               << "errno=" << errno;
  } else {
    fsync(fd_);
    partition_num_ = partition_num;
    file_size_ = partition_num_ * partition_size_;
    LOG(DEBUG1) << "Expand spill file: " << fname_ << " success. Fd= " << fd_
                << " File Size=" << file_size_ << " Bytes";
  }
}

std::string CiderAggSpillFile::getBasePath() {
  static std::string SPILL_FILE_BASE_PATH = "./cider_spill_files";
  return SPILL_FILE_BASE_PATH;
}
