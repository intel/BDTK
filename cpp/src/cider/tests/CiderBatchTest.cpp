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
#include "TestHelpers.h"
#include "cider/CiderBatch.h"

#include <cstdint>
#include <iostream>
#include <vector>

class CiderBatchTest : public ::testing::Test {};

TEST_F(CiderBatchTest, CiderBatchMoveTest) {
  constexpr size_t batch1_col_num = 3;
  constexpr size_t batch2_col_num = 2;

  std::vector<size_t> column1_size{4, 8, 2};
  std::vector<size_t> column2_size{1, 4};
  CiderBatch batch1(10, column1_size);
  CiderBatch batch2(10, column2_size);

  std::cout << "Batch1: \n" << batch1.toString() << std::endl;
  std::cout << "Batch2: \n" << batch2.toString() << std::endl;

  batch2 = std::move(batch1);

  std::cout << "Batch1: \n" << batch1.toString() << std::endl;
  std::cout << "Batch2: \n" << batch2.toString() << std::endl;
}

TEST_F(CiderBatchTest, TestEmptyBatch) {
  CiderBatch empty_batch = CiderBatch();
  std::cout << empty_batch.toString() << std::endl;
}

TEST_F(CiderBatchTest, TestNullSchema) {
  CiderBatch empty_batch = CiderBatch();
  std::cout << empty_batch.toString() << std::endl;
}

TEST_F(CiderBatchTest, TestEmptyColumnSize) {
  std::vector<size_t> column_size;
  CiderBatch batch = CiderBatch(10, column_size);
  std::cout << batch.toString() << std::endl;
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
