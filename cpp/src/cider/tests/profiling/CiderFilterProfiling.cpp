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

#include "tests/utils/CiderProfilingBase.h"

class CiderFilterProfiling : public CiderProfilingBase {
 public:
  CiderFilterProfiling() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE,
        col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE);)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        1000'000,
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
        1000'000))};
  }
};

TEST_F(CiderFilterProfiling, singleFilter) {
  benchSQL("SELECT * FROM test WHERE col_1 > 0");
  benchSQL("SELECT * FROM test WHERE col_2 > 0");
  benchSQL("SELECT * FROM test WHERE col_3 > 0");
  benchSQL("SELECT * FROM test WHERE col_4 > 0");
}

TEST_F(CiderFilterProfiling, multipleFilter) {
  benchSQL(
      "SELECT * FROM test WHERE col_1 > -100 AND col_1 < 100 AND col_2 > -100 AND col_2 "
      "< 100 AND col_3 > -100 AND col_3 < 100 AND col_4 > -100 AND col_4 < 100 ");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
