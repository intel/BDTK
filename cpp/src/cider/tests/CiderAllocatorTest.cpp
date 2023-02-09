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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include "util/Logger.h"
#include "cider/CiderAllocator.h"

class CiderAllocatorTest : public ::testing::Test {};

TEST_F(CiderAllocatorTest, AlignTest) {
  auto alloc = std::make_shared<CiderDefaultAllocator>();
  auto alignAlloc = std::make_shared<AlignAllocator<64>>(alloc);

  int8_t* p = alignAlloc->allocate(19);
  alignAlloc->deallocate(p, 19);

  EXPECT_FALSE((int64_t)p & (64 - 1));
}

TEST_F(CiderAllocatorTest, ArenaAllocator) {
  auto allocator = std::make_shared<CiderArenaAllocator>();

  int8_t* ptr1 = allocator->allocate(1000);
  EXPECT_EQ(allocator->getCap(), 4096);
  int8_t* ptr2 = allocator->allocate(1000);
  EXPECT_EQ(allocator->getCap(), 4096);
  int8_t* ptr3 = allocator->allocate(4000);
  EXPECT_EQ(allocator->getCap(), 4096 + 8192);

  EXPECT_THROW(allocator->deallocate(ptr1, 1000), CiderUnsupportedException);
  EXPECT_THROW(allocator->reallocate(ptr1, 1000, 2000), CiderUnsupportedException);

  allocator->destory();
  allocator = nullptr;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
