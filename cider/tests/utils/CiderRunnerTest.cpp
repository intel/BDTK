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

#include <gtest/gtest.h>
#include <string>

#include "CiderRunner.h"
#include "QueryDataGenerator.h"

std::string create_ddl() {
  return R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT,
  col_3 FLOAT, col_4 DOUBLE);)";
}

std::shared_ptr<CiderBatch> create_default_batch() {
  return std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(1000,
                                               {"col_1", "col_2", "col_3", "col_4"},
                                               {CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I64),
                                                CREATE_SUBSTRAIT_TYPE(Fp32),
                                                CREATE_SUBSTRAIT_TYPE(Fp64)}));
}

TEST(CiderRunnerTest, statelessRunnerMultiBatchTest) {
  CiderCompiler compiler;
  std::string ddl = create_ddl();
  std::string sql = "SELECT col_1 FROM test WHERE col_1 > 0 ";
  auto com_res = compiler.compile(ddl, sql);

  auto input_batch = create_default_batch();

  auto runner = CiderRunner::createCiderRunner(com_res);

  EXPECT_TRUE(std::dynamic_pointer_cast<CiderStatelessRunner>(runner));

  for (int i = 0; i < 1000000; i++) {
    auto res = runner->processNextBatch(input_batch);
  }
  auto res_f = runner->finish();
}

TEST(CiderRunnerTest, statefulRunnerMultiBatchTest) {
  CiderCompiler compiler;
  std::string ddl = create_ddl();
  std::string sql = "SELECT sum(col_1) FROM test WHERE col_1 > 0 ";
  auto com_res = compiler.compile(ddl, sql);

  auto input_batch = create_default_batch();

  auto runner = CiderRunner::createCiderRunner(com_res);
  // Agg query -> CiderStatefulRunner
  EXPECT_TRUE(std::dynamic_pointer_cast<CiderStatefulRunner>(runner));

  for (int i = 0; i < 1000000; i++) {
    auto res = runner->processNextBatch(input_batch);
  }
  while (!runner->isFinished()) {
    auto res_f = runner->finish();
    EXPECT_EQ(res_f->row_num(), 1);
  }
}

TEST(CiderRunnerTest, statelessRunnerMultiThreadTest) {
  CiderCompiler compiler;
  std::string ddl = create_ddl();
  std::string sql = "SELECT col_1 FROM test WHERE col_1 > 0 ";
  auto com_res = compiler.compile(ddl, sql);

  auto input_batch = create_default_batch();

  std::function test_func = [](std::shared_ptr<CiderCompilationResult> com_res,
                               std::shared_ptr<CiderBatch> input_batch) {
    auto runner = CiderRunner::createCiderRunner(com_res);
    for (int i = 0; i < 100000; i++) {
      auto res = runner->processNextBatch(input_batch);
    }
    auto res_f = runner->finish();
  };

  std::vector<std::thread> thread_vec(10);
  for (int i = 0; i < 10; i++) {
    thread_vec[i] = std::thread(test_func, com_res, input_batch);
  }
  for (int i = 0; i < 10; i++) {
    thread_vec[i].join();
  }
}

TEST(CiderRunnerTest, statefulRunnerMultiThreadTest) {
  CiderCompiler compiler;
  std::string ddl = create_ddl();
  std::string sql = "SELECT sum(col_1) FROM test WHERE col_1 > 0 ";
  auto com_res = compiler.compile(ddl, sql);

  auto input_batch = create_default_batch();

  std::function test_func = [](std::shared_ptr<CiderCompilationResult> com_res,
      std::shared_ptr<CiderBatch> input_batch) {
    auto runner = CiderRunner::createCiderRunner(com_res);
    for (int i = 0; i < 100000; i++) {
      auto res = runner->processNextBatch(input_batch);
    }
    auto res_f = runner->finish();
  };

  std::vector<std::thread> thread_vec(10);
  for (int i = 0; i < 10; i++) {
    thread_vec[i] = std::thread(test_func, com_res, input_batch);
  }
  for (int i = 0; i < 10; i++) {
    thread_vec[i].join();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
