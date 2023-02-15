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
#include "tests/utils/CiderNextgenBenchmarkBase.h"
#include "util/Logger.h"
using namespace cider::test::util;

int global_row_num = 0;

class BenchmarkTest : public CiderNextgenBenchmarkBase {
 public:
  BenchmarkTest() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE,
        col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE);)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        global_row_num,
        {"col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64)},
        {},
        GeneratePattern::Random,
        -1000'000,
        1000'000);
  }
};

TEST_F(BenchmarkTest, singleFilter) {
  benchSQL("SELECT * FROM test WHERE col_1 > 0");
  benchSQL("SELECT col_1 FROM test WHERE col_1 > 0");
}

TEST_F(BenchmarkTest, singleFilter1) {
  benchSQL("SELECT col_2 FROM test WHERE col_1 > 0");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  std::vector<int> row_nums{1000, 2'000, 3'000};

  int err{0};

  for (int i = 0; i < row_nums.size(); i++) {
    global_row_num = row_nums[i];
    try {
      err = RUN_ALL_TESTS();
    } catch (const std::exception& e) {
      LOG(ERROR) << e.what();
    }
  }
  return err;
}
