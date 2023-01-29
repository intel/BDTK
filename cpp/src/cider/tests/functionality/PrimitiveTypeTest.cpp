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

#define QUERY_RUN_AND_COMPARE()                                                          \
  {                                                                                      \
    std::vector<std::shared_ptr<CiderBatch>> actual_batches;                             \
    auto actual_batch1 = std::make_shared<CiderBatch>(ciderQueryRunner.runQueryOneBatch( \
        "SELECT col_a, col_b, col_c FROM table_test WHERE col_b IS NOT NULL",            \
        input_batch));                                                                   \
    auto actual_batch2 = std::make_shared<CiderBatch>(ciderQueryRunner.runQueryOneBatch( \
        "SELECT col_a, col_b, col_c FROM table_test WHERE col_b IS NULL", input_batch)); \
    actual_batches.emplace_back(actual_batch1);                                          \
    actual_batches.emplace_back(actual_batch2);                                          \
    EXPECT_TRUE(CiderBatchChecker::checkEq(input_batch, actual_batches, true));          \
    EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batches, input_batch, true));          \
  }

int32_t row_num = 10;
int32_t col_num = 3;

void queryAndCheck(std::string sql,
                   CiderQueryRunner ciderQueryRunner,
                   std::shared_ptr<CiderBatch> input_batch) {
  auto res_batch = ciderQueryRunner.runQueryOneBatch(sql, input_batch);
  EXPECT_EQ(res_batch.column_num(), col_num);
  EXPECT_EQ(res_batch.row_num(), row_num);
  EXPECT_TRUE(CiderBatchChecker::checkEq(
      std::move(input_batch), std::make_shared<CiderBatch>(std::move(res_batch))));
}

TEST(PrimitiveTypeTest, booleanTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a BOOLEAN NOT NULL, col_b BOOLEAN, col_c BOOLEAN)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Bool),
                                                CREATE_SUBSTRAIT_TYPE(Bool),
                                                CREATE_SUBSTRAIT_TYPE(Bool)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               1));
  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, tinyintTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a TINYINT NOT NULL, col_b TINYINT, col_c TINYINT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I8),
                                                CREATE_SUBSTRAIT_TYPE(I8),
                                                CREATE_SUBSTRAIT_TYPE(I8)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, smallintTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a SMALLINT NOT NULL, col_b SMALLINT, col_c SMALLINT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I16),
                                                CREATE_SUBSTRAIT_TYPE(I16),
                                                CREATE_SUBSTRAIT_TYPE(I16)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, integerTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a INTEGER NOT NULL, col_b INTEGER, col_c INTEGER)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, integerMaxBoundaryTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a INTEGER NOT NULL, col_b INTEGER, col_c INTEGER)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               INT32_MAX,
                                               INT32_MAX));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, integerMinBoundaryTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a INTEGER NOT NULL, col_b INTEGER, col_c INTEGER)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32),
                                                CREATE_SUBSTRAIT_TYPE(I32)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               INT32_MIN,
                                               INT32_MIN));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, bigintTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a BIGINT NOT NULL, col_b BIGINT, col_c BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(I64),
                                                CREATE_SUBSTRAIT_TYPE(I64),
                                                CREATE_SUBSTRAIT_TYPE(I64)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, floatTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a FLOAT NOT NULL, col_b FLOAT, col_c FLOAT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Fp32),
                                                CREATE_SUBSTRAIT_TYPE(Fp32),
                                                CREATE_SUBSTRAIT_TYPE(Fp32)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, doubleTest) {
  CiderQueryRunner ciderQueryRunner;
  // col_a is nullable but doesn't have null value.
  // col_b all values are null.
  // col_c each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a DOUBLE NOT NULL, col_b DOUBLE, col_c DOUBLE)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Fp64),
                                                CREATE_SUBSTRAIT_TYPE(Fp64),
                                                CREATE_SUBSTRAIT_TYPE(Fp64)},
                                               {0, 1, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, mixedTypeNotNullTest) {
  CiderQueryRunner ciderQueryRunner;
  // all col is nullable but doesn't have null value.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a BOOLEAN NOT NULL, col_b FLOAT NOT NULL, col_c "
      "TINYINT NOT NULL)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Bool),
                                                CREATE_SUBSTRAIT_TYPE(Fp32),
                                                CREATE_SUBSTRAIT_TYPE(I8)},
                                               {0, 0, 0},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck("SELECT col_a, col_b, col_c FROM table_test WHERE col_b IS NOT NULL",
                ciderQueryRunner,
                input_batch);
}

TEST(PrimitiveTypeTest, mixedTypeNullTest) {
  CiderQueryRunner ciderQueryRunner;
  // all col values are null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a BOOLEAN, col_b DOUBLE, col_c INTEGER)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Bool),
                                                CREATE_SUBSTRAIT_TYPE(Fp64),
                                                CREATE_SUBSTRAIT_TYPE(I32)},
                                               {1, 1, 1},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
  QUERY_RUN_AND_COMPARE();
}

TEST(PrimitiveTypeTest, mixedTypeHalfNullTest) {
  CiderQueryRunner ciderQueryRunner;
  // all col's each value has 1/2 chance to be null.
  std::string create_ddl =
      "CREATE TABLE table_test(col_a BOOLEAN, col_b DOUBLE, col_c BIGINT)";
  ciderQueryRunner.prepare(create_ddl);

  auto input_batch = std::make_shared<CiderBatch>(
      QueryDataGenerator::generateBatchByTypes(row_num,
                                               {"col_a", "col_b", "col_c"},
                                               {CREATE_SUBSTRAIT_TYPE(Bool),
                                                CREATE_SUBSTRAIT_TYPE(Fp64),
                                                CREATE_SUBSTRAIT_TYPE(I64)},
                                               {2, 2, 2},
                                               GeneratePattern::Random,
                                               0,
                                               0));

  queryAndCheck("SELECT * FROM table_test", ciderQueryRunner, input_batch);
  queryAndCheck(
      "SELECT col_a, col_b, col_c FROM table_test", ciderQueryRunner, input_batch);
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
