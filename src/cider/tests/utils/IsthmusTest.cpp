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

#include "Utils.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "util/Logger.h"

TEST(IsthmusTest, couldRun) {
  std::string sql = "select sum(L_LINENUMBER) from lineitem";
  auto res = RunIsthmus::processTpchSql(sql);
  CHECK_GT(res.size(), 0);
}

TEST(IsthmusTest, processSql) {
  std::string sql = "select col_1, col_2 from test";
  std::string create_ddl = "CREATE TABLE test(col_1 BIGINT, col_2 BIGINT)";
  auto res = RunIsthmus::processSql(sql, create_ddl);
  CHECK_GT(res.size(), 0);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
