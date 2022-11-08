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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include "ArrowArrayBuilder.h"
#include "QueryArrowDataGenerator.h"
#include "tests/utils/CiderTestBase.h"

class CiderAggArrowTest : public CiderTestBase {
 public:
  CiderAggArrowTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 "
        "BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 "
        "TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, "
        "half_null_fp32 FLOAT, half_null_fp64 DOUBLE);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_i8",
                                                   "col_i16",
                                                   "col_i32",
                                                   "col_i64",
                                                   "col_fp32",
                                                   "col_fp64",
                                                   "half_null_i8",
                                                   "half_null_i16",
                                                   "half_null_i32",
                                                   "half_null_i64",
                                                   "half_null_fp32",
                                                   "half_null_fp64"},
                                                  {CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I16),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I16),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64)},
                                                  {0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2});
  }
};

/*
 =========================================================================
 following tests are Aggregation on column scenario
 =========================================================================
*/
TEST_F(CiderAggArrowTest, sumTest) {
  // SUM(tinyint)
  assertQueryArrow("SELECT SUM(col_i8) FROM test");
  // SUM(smallint)
  assertQueryArrow("SELECT SUM(col_i16) FROM test");
  // SUM(int)
  assertQueryArrow("SELECT SUM(col_i32) FROM test");
  // SUM(bigint)
  assertQueryArrow("SELECT SUM(col_i64) FROM test");
  // SUM(float)
  assertQueryArrow("SELECT SUM(col_fp32) FROM test");
  // SUM(double)
  assertQueryArrow("SELECT SUM(col_fp64) FROM test");
  // TODO: SUM(decimal)
  // SUM(tinyint) with half null
  assertQueryArrow("SELECT SUM(half_null_i8) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i8) FROM test where half_null_i8 IS NOT NULL");
  // SUM(smallint) with half null
  assertQueryArrow("SELECT SUM(half_null_i16) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i16) FROM test where half_null_i16 IS NOT NULL");
  // SUM(int) with half null
  assertQueryArrow("SELECT SUM(half_null_i32) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // SUM(bigint) with half null
  assertQueryArrow("SELECT SUM(half_null_i64) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // SUM(float) with half null
  assertQueryArrow("SELECT SUM(half_null_fp32) FROM test");
  assertQueryArrow(
      "SELECT SUM(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // SUM(double) with half null
  assertQueryArrow("SELECT SUM(half_null_fp64) FROM test");
  assertQueryArrow(
      "SELECT SUM(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: SUM(decimal) with half null
}

TEST_F(CiderAggArrowTest, countTest) {
  // In cider, COUNT(*) has same syntax as COUNT(1)
  // COUNT(*)
  assertQueryArrow("SELECT COUNT(*) FROM test");

  // COUNT(1)
  assertQueryArrow("SELECT COUNT(1) FROM test");

  // COUNT GROUP BY
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32", "", true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32", "", true);
  //   assertQueryArrow("SELECT col_i32, COUNT(col_i32) FROM test GROUP BY col_i32", "",
  //   true);

  // COUNT FILTER and GROUP BY
  //   assertQueryArrow(
  //       "SELECT col_i8, COUNT(*) FROM test WHERE col_i8 <> 4 AND col_i8 <> 5 GROUP BY "
  //       "col_i8",
  //       "",
  //       true);

  // COUNT GROUP BY with null
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32", "", true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32", "", true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test GROUP BY
  //       half_null_fp32",
  //       "",
  //       true);

  // COUNT(*) FILTER and GROUP BY without NULL
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(*) FROM test WHERE half_null_fp32 IS NOT NULL
  //       GROUP " "BY half_null_fp32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(1) FROM test WHERE half_null_fp32 IS NOT NULL
  //       GROUP " "BY half_null_fp32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS
  //       " "NOT NULL GROUP BY half_null_fp32",
  //       "",
  //       true);

  // COUNT(*) FILTER and GROUP BY with NULL
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(1) FROM test WHERE half_null_fp32 IS NULL GROUP
  //       BY " "half_null_fp32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(*) FROM test WHERE half_null_fp32 IS NULL GROUP
  //       BY " "half_null_fp32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS
  //       " "NULL GROUP BY half_null_fp32",
  //       "",
  //       true);

  // COUNT AGG
  assertQueryArrow("SELECT COUNT(*), MIN(half_null_fp64) FROM test");
  assertQueryArrow("SELECT COUNT(1), MAX(col_i8) FROM test");
  assertQueryArrow("SELECT COUNT(col_i8), MAX(col_i8) FROM test");

  // COUNT AGG with GROUP BY
  //   assertQueryArrow(
  //       "SELECT SUM(col_i32), COUNT(*), SUM(half_null_fp32) FROM test GROUP BY
  //       col_i32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT SUM(col_i32), COUNT(1), SUM(half_null_fp32) FROM test GROUP BY
  //       col_i32",
  //       "",
  //       true);
  //   assertQueryArrow(
  //       "SELECT SUM(col_i32), COUNT(half_null_fp32), SUM(half_null_fp32) FROM test
  //       GROUP " "BY col_i32",
  //       "",
  //       true);

  // COUNT with column is not supported by substrait-java yet
  // COUNT(INT)
  assertQueryArrow("SELECT COUNT(col_i32) FROM test");
  assertQueryArrow("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NULL");
  // COUNT(BIGINT)
  assertQueryArrow("SELECT COUNT(col_i64) FROM test");
  assertQueryArrow("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NULL");
  // COUNT(FLOAT)
  assertQueryArrow("SELECT COUNT(col_fp32) FROM test");
  assertQueryArrow("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NULL");
  // COUNT(DOUBLE)
  assertQueryArrow("SELECT COUNT(col_fp64) FROM test");
  assertQueryArrow("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NULL");
  // COUNT(INT) with half null
  assertQueryArrow("SELECT COUNT(half_null_i32) FROM test");
  assertQueryArrow(
      "SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NULL");
  // COUNT(BIGINT) with half null
  assertQueryArrow("SELECT COUNT(half_null_i64) FROM test");
  assertQueryArrow(
      "SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NULL");
  // COUNT(FLOAT) with half null
  assertQueryArrow("SELECT COUNT(half_null_fp32) FROM test");
  assertQueryArrow(
      "SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NULL");
  // COUNT(DOUBLE) with half null
  assertQueryArrow("SELECT COUNT(half_null_fp64) FROM test");
  assertQueryArrow(
      "SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NULL");
  // COUNT(SMALLINT) with half null
  assertQueryArrow("SELECT COUNT(half_null_i16) FROM test");
  assertQueryArrow(
      "SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NULL");
  // COUNT(TINYINT) with half null
  assertQueryArrow("SELECT COUNT(half_null_i8) FROM test");
  assertQueryArrow("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NOT NULL");
  assertQueryArrow("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NULL");

  // TODO: COUNT(decimal)
  // TODO: COUNT(decimal) with half null
}

TEST_F(CiderAggArrowTest, minOnColumnTest) {
  // agg min with different data type.
  assertQueryArrow(
      "SELECT MIN(col_i8), MIN(col_i64), MIN(col_fp32), MIN(col_fp64) FROM test");

  // agg min with data half null.
  assertQueryArrow(
      "SELECT MIN(half_null_i32), MIN(half_null_i64), MIN(half_null_fp32), "
      "MIN(half_null_fp64) "
      "FROM test");

  // MIN(int)
  assertQueryArrow("SELECT MIN(col_i8) FROM test");
  assertQueryArrow("SELECT MIN(col_i8) FROM test where col_i8 IS NOT NULL");
  // MIN(bigint)
  assertQueryArrow("SELECT MIN(col_i64) FROM test");
  assertQueryArrow("SELECT MIN(col_i64) FROM test where col_i64 IS NOT NULL");
  // MIN(float)
  assertQueryArrow("SELECT MIN(col_fp32) FROM test");
  assertQueryArrow("SELECT MIN(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MIN(double)
  assertQueryArrow("SELECT MIN(col_fp64) FROM test");
  assertQueryArrow("SELECT MIN(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MIN(decimal)
  // MIN(int) with half null
  assertQueryArrow("SELECT MIN(half_null_i32) FROM test");
  assertQueryArrow("SELECT MIN(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MIN(bigint) with half null
  assertQueryArrow("SELECT MIN(half_null_i64) FROM test");
  assertQueryArrow("SELECT MIN(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // MIN(float) with half null
  assertQueryArrow("SELECT MIN(half_null_fp32) FROM test");
  assertQueryArrow(
      "SELECT MIN(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MIN(double) with half null
  assertQueryArrow("SELECT MIN(half_null_fp64) FROM test");
  assertQueryArrow(
      "SELECT MIN(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MIN(decimal) with half null
}

TEST_F(CiderAggArrowTest, maxOnColumnTest) {
  // agg max with different data type.
  assertQueryArrow(
      "SELECT MAX(col_i8), MAX(col_i64), MAX(col_fp32), MAX(col_fp64) FROM test");

  // agg max with data half null.
  assertQueryArrow(
      "SELECT MAX(half_null_i32), MAX(half_null_i64), MAX(half_null_fp32), "
      "MAX(half_null_fp64) "
      "FROM test");

  // MAX(int)
  assertQueryArrow("SELECT MAX(col_i8) FROM test");
  assertQueryArrow("SELECT MAX(col_i8) FROM test where col_i8 IS NOT NULL");
  // MAX(bigint)
  assertQueryArrow("SELECT MAX(col_i64) FROM test");
  assertQueryArrow("SELECT MAX(col_i64) FROM test where col_i64 IS NOT NULL");
  // MAX(float)
  assertQueryArrow("SELECT MAX(col_fp32) FROM test");
  assertQueryArrow("SELECT MAX(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MAX(double)
  assertQueryArrow("SELECT MAX(col_fp64) FROM test");
  assertQueryArrow("SELECT MAX(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MAX(decimal)
  // MAX(int) with half null
  assertQueryArrow("SELECT MAX(half_null_i32) FROM test");
  assertQueryArrow("SELECT MAX(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MAX(bigint) with half null
  assertQueryArrow("SELECT MAX(half_null_i64) FROM test");
  assertQueryArrow("SELECT MAX(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // MAX(float) with half null
  assertQueryArrow("SELECT MAX(half_null_fp32) FROM test");
  assertQueryArrow(
      "SELECT MAX(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MAX(double) with half null
  assertQueryArrow("SELECT MAX(half_null_fp64) FROM test");
  assertQueryArrow(
      "SELECT MAX(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MAX(decimal) with half null
}

/*
 =========================================================================
 following tests are Aggregation on expression scenario
 =========================================================================
*/
TEST_F(CiderAggArrowTest, sumOnExpressionTest) {
  assertQueryArrow("SELECT SUM(col_i32 + col_i8), SUM(col_i8 + col_i32) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 - col_i8) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQueryArrow("SELECT SUM(col_i32 / col_i8) FROM test");

  assertQueryArrow("SELECT SUM(col_i32 + 10) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 - 10) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 * 10) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 / 10) FROM test");

  // this may not be a valid case since we support partial agg only.
  // assertQueryArrow("SELECT SUM(col_i32) / 10 FROM test");

  assertQueryArrow("SELECT SUM(col_i32 * 20 + 10) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 * (1 + col_i8)) FROM test");
  assertQueryArrow("SELECT SUM(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");

  assertQueryArrow(
      "SELECT SUM(half_null_i32 + col_i8), SUM(col_i8 + half_null_i32) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 - col_i8) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 * col_i8) FROM test");
  // divide zero
  // assertQueryArrow("SELECT SUM(half_null_i32 / col_i8) FROM test");

  assertQueryArrow("SELECT SUM(half_null_i32 + 10) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 - 10) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 * 10) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 / 10) FROM test");
  assertQueryArrow(
      "SELECT SUM(half_null_i32 + 10) FROM test where half_null_i32 IS NOT NULL");
  assertQueryArrow(
      "SELECT SUM(half_null_i32 - 10) FROM test where half_null_i32 IS NOT NULL");
  assertQueryArrow(
      "SELECT SUM(half_null_i32 * 10) FROM test where half_null_i32 IS NOT NULL");
  assertQueryArrow(
      "SELECT SUM(half_null_i32 / 10) FROM test where half_null_i32 IS NOT NULL");

  assertQueryArrow("SELECT SUM(half_null_i32 * 20 + 10) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 * 10 + col_i8 * 5) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 * (1 + col_i8)) FROM test");
  assertQueryArrow("SELECT SUM(half_null_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

TEST_F(CiderAggArrowTest, minOnExpressionTest) {
  assertQueryArrow("SELECT MIN(col_i32 + col_i8), MIN(col_i8 + col_i32) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 - col_i8) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQueryArrow("SELECT MIN(col_i32 / col_i8) FROM test");

  assertQueryArrow("SELECT MIN(col_i32 + 10) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 - 10) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 * 10) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 / 10) FROM test");

  assertQueryArrow("SELECT MIN(col_i32 * 20 + 10) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 * (1 + col_i8)) FROM test");
  assertQueryArrow("SELECT MIN(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

TEST_F(CiderAggArrowTest, maxOnExpressionTest) {
  assertQueryArrow("SELECT MAX(col_i32 + col_i8), MAX(col_i8 + col_i32) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 - col_i8) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQueryArrow("SELECT MAX(col_i32 / col_i8) FROM test");

  assertQueryArrow("SELECT MAX(col_i32 + 10) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 - 10) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 * 10) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 / 10) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 + 10) FROM test where col_i32 IS NOT NULL");
  assertQueryArrow("SELECT MAX(col_i32 - 10) FROM test where col_i32 IS NOT NULL");
  assertQueryArrow("SELECT MAX(col_i32 * 10) FROM test where col_i32 IS NOT NULL");
  assertQueryArrow("SELECT MAX(col_i32 / 10) FROM test where col_i32 IS NOT NULL");

  assertQueryArrow("SELECT MAX(col_i32 * 20 + 10) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 * (1 + col_i8)) FROM test");
  assertQueryArrow("SELECT MAX(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

// TODO: move this case to another file. it should belong to function development scope.
TEST_F(CiderAggArrowTest, castTest) {
  assertQueryArrow("SELECT CAST(col_i32 as tinyint) FROM test");
  assertQueryArrow("SELECT CAST(col_i32 as smallint) FROM test");
}

TEST_F(CiderAggArrowTest, sumCastTest) {
  // cast int column
  assertQueryArrow("SELECT SUM(cast(col_i32 as tinyint)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_i32 as smallint)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_i32 as float)) FROM test");

  // cast long column
  assertQueryArrow("SELECT SUM(cast(col_i64 as tinyint)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_i64 as smallint)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_i64 as int)) FROM test");

  // cast float column
  assertQueryArrow("SELECT SUM(cast(col_fp32 as int)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_fp32 as double)) FROM test");

  // cast double column
  assertQueryArrow("SELECT SUM(cast(col_fp64 as int)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_fp64 as bigint)) FROM test");
  assertQueryArrow("SELECT SUM(cast(col_fp64 as float)) FROM test");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  log_options.max_files_ = 0;  // stderr only by default
  logger::init(log_options);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
