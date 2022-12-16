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

#include "tests/utils/CiderProfilingBase.h"

class CiderStringProfiling : public CiderProfilingBase {
 public:
  CiderStringProfiling() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100000,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {},
        GeneratePattern::Random,
        2,
        20))};
  }
};

TEST_F(CiderStringProfiling, substrBench) {
  benchSQL("SELECT SUBSTRING(col_2, 1, 10) FROM test");
}

TEST_F(CiderStringProfiling, likeBench) {
  benchSQL("SELECT col_1 FROM test WHERE col_2 like '%aaa' ");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  logger::init(log_options);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
