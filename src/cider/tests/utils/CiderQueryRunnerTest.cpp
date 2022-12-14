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
#include "CiderQueryRunner.h"
#include "QueryDataGenerator.h"
#include "cider/CiderCompileModule.h"

TEST(CiderQueryRunnerTest, simpleTest) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl = "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));

  std::string sql_select_star = "SELECT * FROM table_test";
  auto res_batch_1 = std::make_shared<CiderBatch>(
      ciderQueryRunner.runQueryOneBatch(sql_select_star, input_batch));
  EXPECT_EQ(res_batch_1->column_num(), 2);
  EXPECT_EQ(res_batch_1->row_num(), 10);
  EXPECT_TRUE(CiderBatchChecker::checkEq(input_batch, res_batch_1));

  std::string sql_select_2_cols = "SELECT col_a, col_b FROM table_test";
  auto res_batch_2 = std::make_shared<CiderBatch>(
      ciderQueryRunner.runQueryOneBatch(sql_select_2_cols, input_batch));
  EXPECT_EQ(res_batch_2->column_num(), 2);
  EXPECT_EQ(res_batch_2->row_num(), 10);
  EXPECT_TRUE(CiderBatchChecker::checkEq(input_batch, res_batch_2));

  auto single_col = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int64_t>(
              "", CREATE_SUBSTRAIT_TYPE(I64), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
          .build());
  std::string sql_select_col_a = "SELECT col_a FROM table_test";
  auto res_batch_3 = std::make_shared<CiderBatch>(
      ciderQueryRunner.runQueryOneBatch(sql_select_col_a, input_batch));
  EXPECT_EQ(res_batch_3->column_num(), 1);
  EXPECT_EQ(res_batch_3->row_num(), 10);
  EXPECT_TRUE(CiderBatchChecker::checkEq(single_col, res_batch_3));

  std::string sql_select_col_b = "SELECT col_b FROM table_test";
  auto res_batch_4 = std::make_shared<CiderBatch>(
      ciderQueryRunner.runQueryOneBatch(sql_select_col_b, input_batch));
  EXPECT_EQ(res_batch_4->column_num(), 1);
  EXPECT_EQ(res_batch_4->row_num(), 10);
  EXPECT_TRUE(CiderBatchChecker::checkEq(single_col, res_batch_4));
}

TEST(CiderQueryRunnerTest, joinTest) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl =
      "CREATE TABLE test_left(col_l_a BIGINT, col_l_b BIGINT); "
      "CREATE TABLE test_right(col_r_a BIGINT, col_r_b BIGINT);";
  ciderQueryRunner.prepare(create_ddl);

  auto left_batch = QueryDataGenerator::generateBatchByTypes(
      10,
      {"col_l_a", "col_l_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)});
  auto right_batch = QueryDataGenerator::generateBatchByTypes(
      10,
      {"col_r_a", "col_r_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)});

  std::string sql = "SELECT col_l_a FROM test_left JOIN test_right ON col_l_b = col_r_b";

  auto res = ciderQueryRunner.runJoinQueryOneBatch(sql, left_batch, right_batch);
  EXPECT_EQ(res.row_num(), 10);
  EXPECT_EQ(res.column_num(), 1);
}

TEST(CiderQueryRunnerTest, joinEmptyBuildTableTest) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl =
      "CREATE TABLE test_left(col_l_a BIGINT, col_l_b BIGINT); "
      "CREATE TABLE test_right(col_r_a BIGINT, col_r_b BIGINT);";
  ciderQueryRunner.prepare(create_ddl);

  auto left_batch = QueryDataGenerator::generateBatchByTypes(
      10,
      {"col_l_a", "col_l_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)});
  auto right_batch = QueryDataGenerator::generateBatchByTypes(
      0,
      {"col_r_a", "col_r_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)});

  std::string sql = "SELECT col_l_a FROM test_left JOIN test_right ON col_l_b = col_r_b";

  EXPECT_THROW(ciderQueryRunner.runJoinQueryOneBatch(sql, left_batch, right_batch),
               CiderCompileException);
}

TEST(CiderQueryRunnerTest, NotFeedBuildTableTest) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl =
      "CREATE TABLE test_left(col_l_a BIGINT, col_l_b BIGINT); "
      "CREATE TABLE test_right(col_r_a BIGINT, col_r_b BIGINT);";
  ciderQueryRunner.prepare(create_ddl);

  auto left_batch = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
      10,
      {"col_l_a", "col_l_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)}));

  std::string sql = "SELECT col_l_a FROM test_left JOIN test_right ON col_l_b = col_r_b";

  EXPECT_THROW(ciderQueryRunner.runQueryOneBatch(sql, left_batch), CiderCompileException);
}

TEST(CiderQueryRunnerTest, VarChar) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl = "CREATE TABLE test(col_a BIGINT, col_b VARCHAR(10));";
  ciderQueryRunner.prepare(create_ddl);
  char* str = "aab";
  CiderByteArray byteArray(3, (const uint8_t*)str);
  auto input_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), {1})
          .addColumn<CiderByteArray>(
              "", CREATE_SUBSTRAIT_TYPE(Varchar), {std::move(byteArray)})
          .build());

  std::string sql = "SELECT col_a FROM test where col_b <> 'aaa'";
  auto res = ciderQueryRunner.runQueryOneBatch(sql, input_batch);
  EXPECT_EQ(res.row_num(), 1);
}

TEST(CiderQueryRunnerTest, Char) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl = "CREATE TABLE test(col_a BIGINT, col_b CHAR(3));";
  ciderQueryRunner.prepare(create_ddl);
  char* str = "aaa";
  CiderByteArray byteArray(3, (const uint8_t*)str);
  auto input_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), {1})
          .addColumn<CiderByteArray>(
              "", CREATE_SUBSTRAIT_TYPE(Varchar), {std::move(byteArray)})
          .build());

  std::string sql = "SELECT col_a FROM test where col_b <> 'aaa'";
  auto res = ciderQueryRunner.runQueryOneBatch(sql, input_batch);
  EXPECT_EQ(res.row_num(), 0);
}

TEST(CiderQueryRunnerTest, GeneratedChar) {
  CiderQueryRunner ciderQueryRunner;
  std::string create_ddl = "CREATE TABLE test(col_a BIGINT, col_b CHAR(3));";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_a", "col_b"},
          {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(Varchar)}));

  std::string sql = "SELECT col_b FROM test where col_b <> 'aaa'";
  auto res = ciderQueryRunner.runQueryOneBatch(sql, input_batch);
  EXPECT_EQ(res.row_num(), 10);
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
