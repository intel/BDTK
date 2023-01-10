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

#include <util/Logger.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include "TestHelpers.h"

#include "exec/operator/aggregate/CiderAggSpillBufferMgr.h"

extern bool g_is_test_env;
using namespace TestHelpers;

static size_t page_num = 512;
static size_t partition_num = 1024;
constexpr static size_t PAGE_SIZE = 4 * 1024;

class CiderAggSpillBufferMgrTest : public ::testing::Test {};

TEST_F(CiderAggSpillBufferMgrTest, PrivateBufferTest) {
  CiderAggSpillBufferMgr buffer_mgr(
      CiderAggSpillBufferMgr::RWMODE, false, nullptr, 0, page_num);
  EXPECT_EQ(buffer_mgr.getSpillFile(), nullptr);
  EXPECT_EQ(buffer_mgr.getPartitionSize(), page_num * PAGE_SIZE);

  int64_t* buffer_addr = reinterpret_cast<int64_t*>(buffer_mgr.getBuffer());
  EXPECT_NE(buffer_addr, nullptr);

  for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
    buffer_addr[i] = i;
  }
}

TEST_F(CiderAggSpillBufferMgrTest, BufferDumpTest) {
  CiderAggSpillBufferMgr buffer_mgr(
      CiderAggSpillBufferMgr::RWMODE, true, nullptr, 0, page_num);
  EXPECT_NE(buffer_mgr.getSpillFile(), nullptr);
  EXPECT_EQ(buffer_mgr.getPartitionSize(), page_num * PAGE_SIZE);

  int64_t* buffer_addr = reinterpret_cast<int64_t*>(buffer_mgr.getBuffer());
  EXPECT_NE(buffer_addr, nullptr);

  int64_t* curr_buffer_addr = buffer_addr;
  for (int64_t partition_index = 0; partition_index < partition_num; ++partition_index) {
    EXPECT_EQ(curr_buffer_addr, buffer_addr);
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      curr_buffer_addr[i] = i + partition_index;
    }
    curr_buffer_addr = reinterpret_cast<int64_t*>(buffer_mgr.toNextPartition());
  }
  EXPECT_EQ(buffer_mgr.getSpillFile()->getPartitionNum(), partition_num + 1);

  curr_buffer_addr = reinterpret_cast<int64_t*>(buffer_mgr.toPartitionAt(0));
  for (int64_t partition_index = 0; partition_index < partition_num; ++partition_index) {
    EXPECT_EQ(curr_buffer_addr, buffer_addr);
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      EXPECT_EQ(curr_buffer_addr[i], i + partition_index);
    }
    curr_buffer_addr = reinterpret_cast<int64_t*>(buffer_mgr.toNextPartition());
  }
}

TEST_F(CiderAggSpillBufferMgrTest, BufferSharedTest) {
  CiderAggSpillBufferMgr write_buffer_mgr(
      CiderAggSpillBufferMgr::WMODE, true, nullptr, 0, page_num);
  EXPECT_NE(write_buffer_mgr.getSpillFile(), nullptr);
  EXPECT_EQ(write_buffer_mgr.getPartitionSize(), page_num * PAGE_SIZE);

  int64_t* write_buffer_addr = reinterpret_cast<int64_t*>(write_buffer_mgr.getBuffer());
  EXPECT_NE(write_buffer_addr, nullptr);

  int64_t* curr_write_buffer_addr = write_buffer_addr;
  for (int64_t partition_index = 0; partition_index < partition_num; ++partition_index) {
    EXPECT_EQ(curr_write_buffer_addr, write_buffer_addr);
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      curr_write_buffer_addr[i] = i + partition_index;
    }
    curr_write_buffer_addr =
        reinterpret_cast<int64_t*>(write_buffer_mgr.toNextPartition());
  }
  EXPECT_EQ(write_buffer_mgr.getSpillFile()->getPartitionNum(), partition_num + 1);

  curr_write_buffer_addr = reinterpret_cast<int64_t*>(write_buffer_mgr.toPartitionAt(0));

  CiderAggSpillBufferMgr read_buffer_mgr(
      CiderAggSpillBufferMgr::RMODE, true, write_buffer_mgr.getSpillFile(), 0, page_num);
  int64_t* read_buffer_addr = reinterpret_cast<int64_t*>(read_buffer_mgr.getBuffer());
  int64_t* curr_read_buffer_addr = read_buffer_addr;

  for (int64_t partition_index = 0; partition_index < partition_num; ++partition_index) {
    EXPECT_EQ(curr_write_buffer_addr, write_buffer_addr);
    EXPECT_EQ(curr_read_buffer_addr, read_buffer_addr);
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      EXPECT_EQ(curr_read_buffer_addr[i], i + partition_index);
    }
    curr_read_buffer_addr = reinterpret_cast<int64_t*>(read_buffer_mgr.toNextPartition());
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      curr_write_buffer_addr[i] = i + partition_index * 2;
    }
    curr_write_buffer_addr =
        reinterpret_cast<int64_t*>(write_buffer_mgr.toNextPartition());
  }

  for (int64_t partition_index = partition_num - 1; partition_index >= 0;
       --partition_index) {
    curr_read_buffer_addr = reinterpret_cast<int64_t*>(read_buffer_mgr.toPrevPartition());
    EXPECT_EQ(curr_read_buffer_addr, read_buffer_addr);
    for (int64_t i = 0; i < (page_num * PAGE_SIZE / sizeof(int64_t)); ++i) {
      EXPECT_EQ(curr_read_buffer_addr[i], i + partition_index * 2);
    }
  }
}

int main(int argc, char** argv) {
  g_is_test_env = true;
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
