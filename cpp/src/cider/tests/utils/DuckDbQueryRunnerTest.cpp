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
#include "ArrowArrayBuilder.h"
#include "CiderBatchBuilder.h"
#include "CiderBatchChecker.h"
#include "DuckDbQueryRunner.h"
#include "QueryArrowDataGenerator.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "util/Logger.h"

#include <vector>

#define ARROW_SIMPLE_TEST_SUITE(C_TYPE, SUBSTRAIT_TYPE, SQL_TYPE)                 \
  {                                                                               \
    DuckDbQueryRunner runner;                                                     \
    std::vector<C_TYPE> expected_data{0, 1, 2, 3, 4};                             \
    std::vector<bool> null_vecs_1{false, false, false, false, false};             \
    std::vector<bool> null_vecs_2{false, true, false, true, false};               \
                                                                                  \
    auto input_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(       \
        ArrowArrayBuilder()                                                       \
            .setRowNum(5)                                                         \
            .addColumn<C_TYPE>("col_1",                                           \
                               CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE),             \
                               expected_data,                                     \
                               null_vecs_1)                                       \
            .addColumn<C_TYPE>("col_2",                                           \
                               CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE),             \
                               expected_data,                                     \
                               null_vecs_2)                                       \
            .build());                                                            \
                                                                                  \
    auto expected_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(    \
        ArrowArrayBuilder()                                                       \
            .setRowNum(5)                                                         \
            .addColumn<C_TYPE>("col_1",                                           \
                               CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE),             \
                               expected_data,                                     \
                               null_vecs_1)                                       \
            .addColumn<C_TYPE>("col_2",                                           \
                               CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE),             \
                               expected_data,                                     \
                               null_vecs_2)                                       \
            .build());                                                            \
                                                                                  \
    /* Create table, run query and check results */                               \
    std::string table_name = "table_test";                                        \
    std::string create_ddl =                                                      \
        "CREATE TABLE table_test(col_a " #SQL_TYPE ", col_b " #SQL_TYPE ")";      \
                                                                                  \
    runner.createTableAndInsertArrowData(table_name, create_ddl, input_batch);    \
    auto res = runner.runSql("select * from table_test;");                        \
    CHECK(!res->HasError());                                                      \
    CHECK_EQ(res->ColumnCount(), 2);                                              \
                                                                                  \
    auto actual_batches =                                                         \
        DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);          \
    EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batches)); \
  }

TEST(DuckDBArrowQueryRunnerTest, simpleIntegerArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int32_t, I32, INTEGER);
}

TEST(DuckDBArrowQueryRunnerTest, simpleTinyIntArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int8_t, I8, TINYINT);
}

TEST(DuckDBArrowQueryRunnerTest, simpleSmallIntArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int16_t, I16, SMALLINT);
}

TEST(DuckDBArrowQueryRunnerTest, simpleBigIntArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int64_t, I64, BIGINT);
}

TEST(DuckDBArrowQueryRunnerTest, simpleFloatArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(float, Fp32, FLOAT);
}

TEST(DuckDBArrowQueryRunnerTest, simpleDoubleArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(double, Fp64, DOUBLE);
}

TEST(DuckDBArrowQueryRunnerTest, simpleBooleanArrowTest) {
  DuckDbQueryRunner runner;

  auto batch_vec =
      std::vector<bool>{true, false, true, false, true, false, true, false, true, false};
  auto batch_null =
      std::vector<bool>{true, true, true, true, true, false, false, false, false, false};
  auto input_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", batch_vec, batch_null)
          .addBoolColumn<bool>("", batch_vec)
          .build());
  auto expected_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", batch_vec, batch_null)
          .addBoolColumn<bool>("", batch_vec)
          .build());

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a BOOLEAN, col_b BOOLEAN)";
  runner.createTableAndInsertArrowData(table_name, create_ddl, input_batch);

  auto res = runner.runSql("select * from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batches = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batches));
}

TEST(DuckDBArrowQueryRunnerTest, multiTableTest) {
  DuckDbQueryRunner runner;

  std::vector<int> col1{0, 1, 2, 3, 4};
  auto table_data1 = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col_a", CREATE_SUBSTRAIT_TYPE(I32), col1)
          .addColumn("col_b", CREATE_SUBSTRAIT_TYPE(I32), col1)
          .build());

  std::string table_name1 = "table_test1";
  std::string create_ddl1 = "CREATE TABLE table_test1(col_a INTEGER, col_b INTEGER)";

  runner.createTableAndInsertArrowData(table_name1, create_ddl1, table_data1);

  std::string table_name2 = "table_test2";
  std::string create_ddl2 = "CREATE TABLE table_test2(col_a INTEGER, col_b INTEGER)";

  std::vector<int> col2{1, 2, 3, 4, 5};
  auto table_data2 = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col_a", CREATE_SUBSTRAIT_TYPE(I32), col2)
          .addColumn("col_b", CREATE_SUBSTRAIT_TYPE(I32), col2)
          .build());

  runner.createTableAndInsertArrowData(table_name2, create_ddl2, table_data2);

  auto res = runner.runSql(
      "SELECT table_test1.col_a "
      "FROM table_test1 JOIN table_test2 "
      "ON table_test1.col_a = table_test2.col_a;");

  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 1);

  auto actual_batch = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);

  auto expected_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4})
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batch));
}

TEST(DuckDBArrowQueryRunnerTest, multiBatchFetchTest) {
  DuckDbQueryRunner runner;
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INT, col_b BIGINT)";

  std::vector<int32_t> col_1(5000);
  std::vector<int64_t> col_2(5000);
  for (int i = 0; i < 5000; i++) {
    col_1[i] = i + 1;
    col_2[i] = i - 1;
  }
  auto input_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int32_t>("col_a", CREATE_SUBSTRAIT_TYPE(I32), col_1)
          .addColumn<int64_t>("col_b", CREATE_SUBSTRAIT_TYPE(I64), col_2)
          .build());

  runner.createTableAndInsertArrowData(table_name, create_ddl, input_batch);
  auto res = runner.runSql("select col_a, col_b from table_test;");

  CHECK(!res->HasError());
  auto multi_batch_res = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(input_batch, multi_batch_res));
}

TEST(DuckDBArrowQueryRunnerTest, HugeIntTest) {
  DuckDbQueryRunner runner;
  std::vector<int> expected_data{0, 1, 2, 3, 4};
  std::vector<bool> null_vecs{false, false, false, true, true};

  auto input_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col_a", CREATE_SUBSTRAIT_TYPE(I32), expected_data)
          .addColumn<int>("col_b", CREATE_SUBSTRAIT_TYPE(I32), expected_data, null_vecs)
          .build());

  /* Create table, run query and check results */
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INTEGER, col_b INTEGER)";

  runner.createTableAndInsertArrowData(table_name, create_ddl, input_batch);
  auto res = runner.runSql("select SUM(col_a), SUM(col_b) from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batches = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);

  auto expected_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(1)
          .addColumn<int64_t>("r1", CREATE_SUBSTRAIT_TYPE(I64), std::vector<int64_t>{10})
          .addColumn<int64_t>("r2", CREATE_SUBSTRAIT_TYPE(I64), std::vector<int64_t>{3})
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_batches, expected_batch));
}

TEST(DuckDBArrowQueryRunnerTest, FixedPointDecimalTest) {
  DuckDbQueryRunner runner;
  std::vector<int> expected_data{0, 1, 2, 3, 4};
  std::vector<bool> null_vecs{false, false, false, true, true};

  auto input_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col_a", CREATE_SUBSTRAIT_TYPE(I32), expected_data)
          .addColumn<int>("col_b", CREATE_SUBSTRAIT_TYPE(I32), expected_data, null_vecs)
          .build());

  /* Create table, run query and check results */
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INTEGER, col_b INTEGER)";

  runner.createTableAndInsertArrowData(table_name, create_ddl, input_batch);

  // INTEGER +/- floating point will yield DECIMALs
  auto res = runner.runSql("select col_a + 0.123, (col_b + 0.4) / 2 from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batches = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);

  auto res_a = std::vector<double>{0.123, 1.123, 2.123, 3.123, 4.123};
  auto res_b = std::vector<double>{0.2, 0.7, 1.2, 1.7, 2.2};
  auto expected_batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<double>("r1", CREATE_SUBSTRAIT_TYPE(Fp64), res_a)
          .addColumn<double>("r2", CREATE_SUBSTRAIT_TYPE(Fp64), res_b, null_vecs)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batches));
}

TEST(DuckDBArrowQueryRunnerTest, VarCharTest) {
  DuckDbQueryRunner runner;

  auto vec = std::vector<std::string>{"aaaaa", "bbbbb", "aaaaabbbbb", "ccccc", "ddddd"};
  auto nulls = std::vector<bool>{false, false, true, false, false};

  auto [data, offsets] = ArrowBuilderUtils::createDataAndOffsetFromStrVector(vec);
  auto batch = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addUTF8Column("col_str", data, offsets)
          .addUTF8Column("col_str_null", data, offsets, nulls)
          .build());

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a VARCHAR, col_b VARCHAR)";

  runner.createTableAndInsertArrowData(table_name, create_ddl, batch);

  auto res = runner.runSql("select * from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batch = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(batch, actual_batch));
}

TEST(DuckDBArrowQueryRunnerTest, VarCharStringCompatTest) {
  DuckDbQueryRunner runner;

  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;

  QueryArrowDataGenerator::generateBatchByTypes(
      schema,
      array,
      10,
      {"col_vc", "col_str"},
      {CREATE_SUBSTRAIT_TYPE(Varchar), CREATE_SUBSTRAIT_TYPE(String)},
      {2, 2},
      GeneratePattern::Random,
      0,
      10);

  auto batch = std::make_shared<CiderBatch>(
      schema, array, std::make_shared<CiderDefaultAllocator>());

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a VARCHAR, col_b STRING)";

  runner.createTableAndInsertArrowData(table_name, create_ddl, batch);

  auto res = runner.runSql("select * from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batch = DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(batch, actual_batch));
}

// old DuckDBQueryRunnerTests below

TEST(DuckDBQueryRunnerTest, basicTest) {
  DuckDbQueryRunner runner;

  std::vector<int> col{0, 1, 2, 3, 4};
  std::vector<std::vector<int>> table_data{col, col};

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INTEGER, col_b INTEGER)";

  runner.createTableAndInsertData(table_name, create_ddl, table_data);

  auto res = runner.runSql("select * from table_test;");

  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), col)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), col)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));
}

TEST(DuckDBQueryRunnerTest, VarCharTest) {
  DuckDbQueryRunner runner;

  std::vector<CiderByteArray> vec;
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));

  auto batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(Varchar), vec)
          .build());

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a VARCHAR)";

  std::vector<std::shared_ptr<CiderBatch>> input_vec{batch};

  runner.createTableAndInsertData(table_name, create_ddl, input_vec);

  auto res = runner.runSql("select * from table_test;");
  CHECK(!res->HasError());

  auto actual_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkEq(batch, actual_batch));
}

TEST(DuckDBQueryRunnerTest, multiTableTest) {
  DuckDbQueryRunner runner;

  std::vector<int> col1{0, 1, 2, 3, 4};
  std::vector<std::vector<int>> table_data1{col1, col1};

  std::string table_name1 = "table_test1";
  std::string create_ddl1 = "CREATE TABLE table_test1(col_a INTEGER, col_b INTEGER)";

  runner.createTableAndInsertData(table_name1, create_ddl1, table_data1);

  std::string table_name2 = "table_test2";
  std::string create_ddl2 = "CREATE TABLE table_test2(col_a INTEGER, col_b INTEGER)";

  std::vector<int> col2{1, 2, 3, 4, 5};
  std::vector<std::vector<int>> table_data2{col2, col2};
  runner.createTableAndInsertData(table_name2, create_ddl2, table_data2);

  auto res = runner.runSql(
      "select table_test1.col_a from table_test1 JOIN table_test2 ON table_test1.col_a = "
      "table_test2.col_a;");

  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 1);

  auto actual_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4})
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));
}

TEST(DuckDBQueryRunnerTest, insertCiderBatchTest) {
  DuckDbQueryRunner runner;

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INTEGER, col_b INTEGER)";

  auto in_batch_ptr = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col_a", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
          .addColumn<int>("col_b", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
          .build());
  std::vector<std::shared_ptr<CiderBatch>> input_vec{in_batch_ptr};

  runner.createTableAndInsertData(table_name, create_ddl, input_vec);

  auto res = runner.runSql("select * from table_test;");
  CHECK(!res->HasError());
  CHECK_EQ(res->ColumnCount(), 2);

  auto actual_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);

  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, in_batch_ptr));
}

TEST(DuckDBQueryRunnerTest, dateTest) {
  DuckDbQueryRunner runner;

  std::vector<CiderDateType> col;
  col.push_back(CiderDateType("1970-01-01"));
  col.push_back(CiderDateType("1970-01-02"));
  col.push_back(CiderDateType("1970-01-03"));
  col.push_back(CiderDateType("1970-01-04"));
  col.push_back(CiderDateType("1970-01-05"));

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a DATE )";

  auto in_batch_ptr = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addTimingColumn<CiderDateType>("col_a", CREATE_SUBSTRAIT_TYPE(Date), col)
          .build());
  std::vector<std::shared_ptr<CiderBatch>> input_vec{in_batch_ptr};

  runner.createTableAndInsertData(table_name, create_ddl, input_vec);

  auto res = runner.runSql("select col_a from table_test where col_a >= '1970-01-04';");
  CHECK(!res->HasError());

  auto res_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);
  EXPECT_EQ((res_batch[0])->row_num(), 2);
  EXPECT_EQ((res_batch[0])->column_num(), 1);

  std::vector<CiderDateType> expected_col;
  expected_col.push_back(CiderDateType("1970-01-04"));
  expected_col.push_back(CiderDateType("1970-01-05"));
  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addTimingColumn<CiderDateType>(
              "col_a", CREATE_SUBSTRAIT_TYPE(Date), expected_col)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(res_batch, expected_batch));
}

TEST(DuckDBQueryRunnerTest, nullDataTest) {
  DuckDbQueryRunner runner;

  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INT, col_b BIGINT)";

  auto input_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int32_t>("col_a",
                              CREATE_SUBSTRAIT_TYPE(I32),
                              {1, 2, 3, INT32_MIN, INT32_MIN},
                              {0, 0, 0, 1, 1})
          .addColumn<int64_t>("col_b",
                              CREATE_SUBSTRAIT_TYPE(I64),
                              {2, 3, 4, INT64_MIN, INT64_MIN},
                              {0, 0, 0, 1, 1})
          .build());

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int32_t>("col_a",
                              CREATE_SUBSTRAIT_TYPE(I32),
                              {2, 3, 4, INT32_MIN, INT32_MIN},
                              {0, 0, 0, 1, 1})
          .addColumn<int64_t>("col_b",
                              CREATE_SUBSTRAIT_TYPE(I64),
                              {1, 2, 3, INT64_MIN, INT64_MIN},
                              {0, 0, 0, 1, 1})
          .build());

  runner.createTableAndInsertData(table_name, create_ddl, input_batch);
  auto res = runner.runSql("select col_a + 1, col_b - 1 from table_test;");

  CHECK(!res->HasError());
  auto actual_batch = DuckDbResultConvertor::fetchDataToCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));
}

TEST(DuckDBQueryRunnerTest, multiBatchFetchTest) {
  DuckDbQueryRunner runner;
  std::string table_name = "table_test";
  std::string create_ddl = "CREATE TABLE table_test(col_a INT, col_b BIGINT)";

  std::vector<int32_t> col_1(5000);
  std::vector<int64_t> col_2(5000);
  for (int i = 0; i < 5000; i++) {
    col_1[i] = i + 1;
    col_2[i] = i - 1;
  }
  auto input_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int32_t>("col_a", CREATE_SUBSTRAIT_TYPE(I32), col_1)
          .addColumn<int64_t>("col_b", CREATE_SUBSTRAIT_TYPE(I64), col_2)
          .build());

  runner.createTableAndInsertData(table_name, create_ddl, input_batch);
  auto res = runner.runSql("select col_a, col_b from table_test;");

  CHECK(!res->HasError());
  auto multi_batch_res = DuckDbResultConvertor::fetchDataToCiderBatch(res);
  EXPECT_TRUE(CiderBatchChecker::checkEq(input_batch, multi_batch_res));
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
