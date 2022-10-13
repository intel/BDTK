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

#include <gtest/gtest.h>

#include "CiderBatchBuilder.h"
#include "CiderBatchChecker.h"
#include "DuckDbQueryRunner.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "util/Logger.h"

#include <vector>

template <typename T>
std::tuple<std::vector<std::vector<T>>, std::vector<std::vector<bool>>>
generateSequenceData(int n_rows = 10, int n_nulls = 5) {
  CHECK(n_nulls <= n_rows);
  int n_not_nulls = n_rows - n_nulls;

  // col_1 will never contain null
  std::vector<T> col_1;
  std::vector<bool> valid_1(n_rows, true);

  // last n_nulls elements of col_2 will be null
  std::vector<T> col_2;
  std::vector<bool> valid_2;

  for (int i = 0; i < n_rows; ++i) {
    bool is_valid = (i < n_not_nulls);
    T value = static_cast<T>(i);
    col_1.push_back(value);
    col_2.push_back(is_valid ? value : std::numeric_limits<T>::min());
    valid_2.push_back(is_valid);
  }

  std::vector<std::vector<T>> cols{col_1, col_2};
  std::vector<std::vector<bool>> valids{valid_1, valid_2};

  return {cols, valids};
}

template <typename T>
void checkDuckDbScalarOutput(
    const std::vector<std::shared_ptr<CiderBatch>>& actual_batches,
    const std::vector<std::vector<T>>& expected_data,
    const std::vector<std::vector<bool>>& expected_nulls = {}) {
  /// TODO: (YBRua) To be deprecated.
  /// Change this to CiderBatchChecker after Checker is implemented

  // expected data should at least contain something
  EXPECT_TRUE(expected_data.size() > 0);
  // currently only supports one CiderBatch
  EXPECT_EQ(actual_batches.size(), 1);
  auto actual_batch = actual_batches[0];

  // output should be a CiderBatch containing one struct type
  EXPECT_EQ(actual_batch->getNullCount(), 0);
  EXPECT_EQ(actual_batch->getBufferNum(), 1);
  EXPECT_EQ(actual_batch->getCiderType(), SQLTypes::kSTRUCT);

  // check col and row nums
  EXPECT_EQ(actual_batch->getChildrenNum(), expected_data.size());
  EXPECT_EQ(actual_batch->getLength(), expected_data[0].size());

  // check data
  for (auto i = 0; i < actual_batch->getChildrenNum(); ++i) {
    auto child = actual_batch->getChildAt(i);
    // scalar (primitive type) result should contain 2 buffers
    EXPECT_EQ(child->getBufferNum(), 2);
    // check child row nums
    EXPECT_EQ(child->getLength(), expected_data[i].size());

    auto data_buffer = child->as<ScalarBatch<int32_t>>()->getRawData();
    auto null_buffer = child->getNulls();
    auto null_count = int{0};
    for (auto j = 0; j < child->getLength(); ++j) {
      EXPECT_EQ(data_buffer[j], expected_data[i][j]);
      EXPECT_EQ(CiderBitUtils::isBitSetAt(null_buffer, j), expected_nulls[i][j]);
    }
  }
}

#define ARROW_SIMPLE_TEST_SUITE(C_TYPE, S_TYPE, SQL_TYPE)                               \
  {                                                                                     \
    DuckDbQueryRunner runner;                                                           \
    auto [expected_data, expected_nulls] = generateSequenceData<C_TYPE>();              \
                                                                                        \
    /* CiderBatchBuilder expects a NULL vector                                          \
     * but expected_nulls is actually a VALID vector so we flip it here*/               \
    auto null_vecs = expected_nulls;                                                    \
    std::for_each(null_vecs.begin(), null_vecs.end(), [](std::vector<bool>& null_vec) { \
      null_vec.flip();                                                                  \
    });                                                                                 \
                                                                                        \
    /* TODO: (YBRua) The CiderBatch generated here is not in Arrow format               \
     * Change it to an Arrow-formatted batch after CiderBatchBuilder is updated */      \
    auto batch = std::make_shared<CiderBatch>(                                          \
        CiderBatchBuilder()                                                             \
            .setRowNum(10)                                                              \
            .addColumn<C_TYPE>(                                                         \
                "col_1", CREATE_SUBSTRAIT_TYPE(S_TYPE), expected_data[0], null_vecs[0]) \
            .addColumn<C_TYPE>(                                                         \
                "col_2", CREATE_SUBSTRAIT_TYPE(S_TYPE), expected_data[1], null_vecs[1]) \
            .build());                                                                  \
                                                                                        \
    std::string table_name = "table_test";                                              \
    std::string create_ddl =                                                            \
        "CREATE TABLE table_test(col_a " #SQL_TYPE ", col_b " #SQL_TYPE ")";            \
                                                                                        \
    runner.createTableAndInsertData(table_name, create_ddl, batch);                     \
    auto res = runner.runSql("select * from table_test;");                              \
    CHECK(!res->HasError());                                                            \
    CHECK_EQ(res->ColumnCount(), 2);                                                    \
                                                                                        \
    auto actual_batches =                                                               \
        DuckDbResultConvertor::fetchDataToArrowFormattedCiderBatch(res);                \
    checkDuckDbScalarOutput<C_TYPE>(actual_batches, expected_data, expected_nulls);     \
  }

TEST(DuckDBResultConvertorTest, simpleI32ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int32_t, I32, INTEGER);
}

TEST(DuckDBResultConvertorTest, simpleI8ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int8_t, I8, TINYINT);
}

TEST(DuckDBResultConvertorTest, simpleI16ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int16_t, I16, SMALLINT);
}

TEST(DuckDBResultConvertorTest, simpleI64ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(int64_t, I64, BIGINT);
}

TEST(DuckDBResultConvertorTest, simpleFp32ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(float, Fp32, FLOAT);
}

TEST(DuckDBResultConvertorTest, simpleI64ArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(double, Fp64, DOUBLE);
}

TEST(DuckDBResultConvertorTest, simpleBoolArrowTest) {
  ARROW_SIMPLE_TEST_SUITE(bool, Bool, BOOLEAN);
}

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
  std::cout << "error: " << res->error << std::endl;  // YBRua: wrap this into an if?

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

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
