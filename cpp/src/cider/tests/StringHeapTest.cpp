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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "exec/nextgen/context/StringHeap.h"
#include "util/Logger.h"

TEST(StringHeapTest, addString) {
  StringHeap heap;
  const char* str1 = "abcdef";
  heap.addString(str1, 6);
  EXPECT_EQ(heap.getNum(), 1);
  auto str = heap.emptyString(10);
  EXPECT_EQ(heap.getNum(), 2);
  heap.destroy();
  EXPECT_EQ(heap.getNum(), 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    // LOG(ERROR) << e.what();
  }

  return err;
}
