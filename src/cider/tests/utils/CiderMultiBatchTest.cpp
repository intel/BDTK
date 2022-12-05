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
#include "CiderBatchChecker.h"
#include "tests/utils/CiderTestBase.h"

class CiderMultiBatchTest : public CiderTestBase {};

static std::vector<std::shared_ptr<CiderBatch>> convertToPtr(
    std::vector<CiderBatch> input_vec) {
  std::vector<std::shared_ptr<CiderBatch>> output_vec;
  for (auto it = input_vec.begin(); it < input_vec.end(); it++) {
    output_vec.emplace_back(std::make_shared<CiderBatch>(std::move(*it)));
  }
  return output_vec;
}

TEST(CiderMultiBatchTest, simpleSelectTest) {
  std::string sql1 = "SELECT * FROM table_test";
  std::string sql2 = "SELECT col_a, col_b FROM table_test";
  std::string sql3 = "SELECT col_a, col_b FROM table_test WHERE col_a > 5 and col_b < 10";

  CiderQueryRunner ciderQueryRunner;
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  DuckDbQueryRunner duckDbQueryRunner;

  auto input_batch_1 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  auto input_batch_2 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          20,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  std::vector<std::shared_ptr<CiderBatch>> input_vec;
  input_vec.emplace_back(input_batch_1);
  input_vec.emplace_back(input_batch_2);
  duckDbQueryRunner.createTableAndInsertData(table_name, create_ddl, input_vec);

  // select star
  auto duck_res = duckDbQueryRunner.runSql(sql1);
  std::vector<std::shared_ptr<CiderBatch>> expected_res_vec;
  std::vector<std::shared_ptr<CiderBatch>> actual_res_vec;
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql1, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec));

  // select col name
  duck_res = duckDbQueryRunner.runSql(sql2);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql2, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec));

  // select col name and filter
  duck_res = duckDbQueryRunner.runSql(sql3);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql3, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec));
}

TEST(CiderMultiBatchTest, simpleAggTest) {
  std::string sql1 = "SELECT SUM(col_a) AS col_a_sum FROM table_test";
  std::string sql2 = "SELECT SUM(col_a), SUM(col_b) FROM table_test";
  std::string sql3 = "SELECT SUM(col_a), SUM(col_b) FROM table_test WHERE col_a > 5";

  CiderQueryRunner ciderQueryRunner;
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  DuckDbQueryRunner duckDbQueryRunner;

  auto input_batch_1 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  auto input_batch_2 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          20,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  std::vector<std::shared_ptr<CiderBatch>> input_vec;
  input_vec.emplace_back(input_batch_1);
  input_vec.emplace_back(input_batch_2);

  duckDbQueryRunner.createTableAndInsertData(table_name, create_ddl, input_vec);

  // one agg
  auto duck_res = duckDbQueryRunner.runSql(sql1);
  std::vector<std::shared_ptr<CiderBatch>> expected_res_vec;
  std::vector<std::shared_ptr<CiderBatch>> actual_res_vec;
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql1, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));

  // two aggs
  duck_res = duckDbQueryRunner.runSql(sql2);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql2, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));

  // two aggs and filter
  duck_res = duckDbQueryRunner.runSql(sql3);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql3, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));
}

TEST(CiderMultiBatchTest, simpleGroupByTest) {
  std::string sql1 = "SELECT SUM(col_a) AS col_a_sum FROM table_test GROUP BY col_a";
  std::string sql2 =
      "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, col_b";
  std::string sql3 =
      "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, col_b "
      "HAVING col_a > 5";

  CiderQueryRunner ciderQueryRunner;
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  DuckDbQueryRunner duckDbQueryRunner;

  auto input_batch_1 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  auto input_batch_2 =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          20,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));
  std::vector<std::shared_ptr<CiderBatch>> input_vec;
  input_vec.emplace_back(input_batch_1);
  input_vec.emplace_back(input_batch_2);

  duckDbQueryRunner.createTableAndInsertData(table_name, create_ddl, input_vec);

  // one group by key with one agg
  auto duck_res = duckDbQueryRunner.runSql(sql1);
  std::vector<std::shared_ptr<CiderBatch>> expected_res_vec;
  std::vector<std::shared_ptr<CiderBatch>> actual_res_vec;
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql1, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));

  // two group by keys with two aggs
  duck_res = duckDbQueryRunner.runSql(sql2);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql2, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));

  // two group by keys with two aggs and one having condition
  duck_res = duckDbQueryRunner.runSql(sql3);
  expected_res_vec = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
  actual_res_vec = convertToPtr(ciderQueryRunner.runQueryMultiBatches(sql3, input_vec));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_res_vec, actual_res_vec, true));
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
