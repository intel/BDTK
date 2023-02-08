/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "util/filesystem/cider_fs.h"

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "util/Logger.h"

namespace cider {

int get_page_size() {
  return getpagesize();
}

size_t file_size(const int fd) {
  struct stat buf;
  int err = fstat(fd, &buf);
  CHECK_EQ(0, err);
  return buf.st_size;
}

void* checked_mmap(const int fd, const size_t sz) {
  auto ptr = mmap(nullptr, sz, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  CHECK(ptr != reinterpret_cast<void*>(-1));
#ifdef __linux__
#ifdef MADV_HUGEPAGE
  madvise(ptr, sz, MADV_RANDOM | MADV_WILLNEED | MADV_HUGEPAGE);
#else
  madvise(ptr, sz, MADV_RANDOM | MADV_WILLNEED);
#endif
#endif
  return ptr;
}

void checked_munmap(void* addr, size_t length) {
  CHECK_EQ(0, munmap(addr, length));
}

int msync(void* addr, size_t length, bool async) {
  // TODO: support MS_INVALIDATE?
  return ::msync(addr, length, async ? MS_ASYNC : MS_SYNC);
}

int fsync(int fd) {
  return ::fsync(fd);
}

int open(const char* path, int flags, int mode) {
  return ::open(path, flags, mode);
}

void close(const int fd) {
  ::close(fd);
}

::FILE* fopen(const char* filename, const char* mode) {
  return ::fopen(filename, mode);
}

}  // namespace cider
