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

#include "tests/benchmark/CiderBenchmarkUtils.cpp"
#include "tests/utils/CiderProfilingBase.h"
class CiderGroupbyProfiling : public CiderProfilingBase {
 public:
  CiderGroupbyProfiling() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, id3 BIGINT NOT "
        "NULL, id4 BIGINT NOT NULL, id5 BIGINT NOT NULL, id6 BIGINT NOT NULL, v1 BIGINT "
        "NOT NULL, "
        "v2 BIGINT NOT NULL,v3 DOUBLE NOT NULL);";
    std::vector<std::string> col_name = {
        "id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"};
    input_ = {readFromCsv("/data/G1_1e7_1e2_0_0.csv", col_name)};
  }
};

TEST_F(CiderGroupbyProfiling, GroupbySum) {
  benchSQL("SELECT sum(v1),sum(v2),sum(v3) FROM test group by id6");
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
