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
#include "tests/utils/CiderTestBase.h"

#define NULL_VALUE_I32 std::numeric_limits<int32_t>::min()
#define NULL_VALUE_FLOAT std::numeric_limits<float>::min()
#define NULL_VALUE_DOUBLE std::numeric_limits<double>::min()

class CiderAggTest : public CiderTestBase {
 public:
  CiderAggTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_i8 TINYINT, col_i16 SMALLINT, col_i32 INT, col_i64 "
        "BIGINT, col_fp32 FLOAT, col_fp64 DOUBLE, half_null_i8 "
        "TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, "
        "half_null_fp32 FLOAT, half_null_fp64 DOUBLE);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(10,
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
                                                 {0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2}))};
  }
};

/*
 =========================================================================
 following tests are Aggregation on column scenario
 =========================================================================
*/
TEST_F(CiderAggTest, sumTest) {
  // SUM(tinyint)
  assertQuery("SELECT SUM(col_i8) FROM test");
  // SUM(smallint)
  assertQuery("SELECT SUM(col_i16) FROM test");
  // SUM(int)
  assertQuery("SELECT SUM(col_i32) FROM test");
  // SUM(bigint)
  assertQuery("SELECT SUM(col_i64) FROM test");
  // SUM(float)
  assertQuery("SELECT SUM(col_fp32) FROM test");
  // SUM(double)
  assertQuery("SELECT SUM(col_fp64) FROM test");
  // TODO: SUM(decimal)
  // SUM(tinyint) with half null
  assertQuery("SELECT SUM(half_null_i8) FROM test");
  assertQuery("SELECT SUM(half_null_i8) FROM test where half_null_i8 IS NOT NULL");
  // SUM(smallint) with half null
  assertQuery("SELECT SUM(half_null_i16) FROM test");
  assertQuery("SELECT SUM(half_null_i16) FROM test where half_null_i16 IS NOT NULL");
  // SUM(int) with half null
  assertQuery("SELECT SUM(half_null_i32) FROM test");
  assertQuery("SELECT SUM(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // SUM(bigint) with half null
  assertQuery("SELECT SUM(half_null_i64) FROM test");
  assertQuery("SELECT SUM(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // SUM(float) with half null
  assertQuery("SELECT SUM(half_null_fp32) FROM test");
  assertQuery("SELECT SUM(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // SUM(double) with half null
  assertQuery("SELECT SUM(half_null_fp64) FROM test");
  assertQuery("SELECT SUM(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: SUM(decimal) with half null
}

TEST_F(CiderAggTest, aggArrowTest) {
  // SUM(int)
  prepareArrowBatch();
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
  // SUM(smallint) with half null
  assertQueryArrow("SELECT SUM(half_null_i16) FROM test");
  // SUM(int) with half null
  assertQueryArrow("SELECT SUM(half_null_i32) FROM test");
  // SUM(bigint) with half null
  assertQueryArrow("SELECT SUM(half_null_i64) FROM test");
  // SUM(float) with half null
  assertQueryArrow("SELECT SUM(half_null_fp32) FROM test");
  // SUM(double) with half null
  assertQueryArrow("SELECT SUM(half_null_fp64) FROM test");
  // In cider, COUNT(*) has same syntax as COUNT(1)
  // COUNT(*)
  assertQuery("SELECT COUNT(*) FROM test");
  // COUNT(1)
  assertQuery("SELECT COUNT(1) FROM test");
  // COUNT AGG
  assertQuery("SELECT COUNT(*), MIN(half_null_fp64) FROM test");
  assertQuery("SELECT COUNT(1), MAX(col_i8) FROM test");
  assertQuery("SELECT COUNT(col_i8), MAX(col_i8) FROM test");
  // COUNT(DISTINCT tinyint)
  assertQuery("SELECT COUNT(DISTINCT col_i8) FROM test");
  // COUNT(DISTINCT smallint)
  assertQuery("SELECT COUNT(DISTINCT col_i16) FROM test");
  // COUNT(DISTINCT int)
  assertQuery("SELECT COUNT(DISTINCT col_i32) FROM test");
  // COUNT(DISTINCT bigint)
  assertQuery("SELECT COUNT(DISTINCT col_i64) FROM test");
}

TEST_F(CiderAggTest, countTest) {
  // In cider, COUNT(*) has same syntax as COUNT(1)
  // COUNT(*)
  assertQuery("SELECT COUNT(*) FROM test");

  // COUNT(1)
  assertQuery("SELECT COUNT(1) FROM test");

  // COUNT GROUP BY
  assertQuery(
      "SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32", "", true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32", "", true);
  assertQuery("SELECT col_i32, COUNT(col_i32) FROM test GROUP BY col_i32", "", true);

  // COUNT FILTER and GROUP BY
  assertQuery(
      "SELECT col_i8, COUNT(*) FROM test WHERE col_i8 <> 4 AND col_i8 <> 5 GROUP BY "
      "col_i8",
      "",
      true);

  // COUNT GROUP BY with null
  assertQuery(
      "SELECT half_null_fp32, COUNT(*) FROM test GROUP BY half_null_fp32", "", true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(1) FROM test GROUP BY half_null_fp32", "", true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test GROUP BY half_null_fp32",
      "",
      true);

  // COUNT(*) FILTER and GROUP BY without NULL
  assertQuery(
      "SELECT half_null_fp32, COUNT(*) FROM test WHERE half_null_fp32 IS NOT NULL GROUP "
      "BY half_null_fp32",
      "",
      true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(1) FROM test WHERE half_null_fp32 IS NOT NULL GROUP "
      "BY half_null_fp32",
      "",
      true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS "
      "NOT NULL GROUP BY half_null_fp32",
      "",
      true);

  // COUNT(*) FILTER and GROUP BY with NULL
  assertQuery(
      "SELECT half_null_fp32, COUNT(1) FROM test WHERE half_null_fp32 IS NULL GROUP BY "
      "half_null_fp32",
      "",
      true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(*) FROM test WHERE half_null_fp32 IS NULL GROUP BY "
      "half_null_fp32",
      "",
      true);
  assertQuery(
      "SELECT half_null_fp32, COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS "
      "NULL GROUP BY half_null_fp32",
      "",
      true);

  // COUNT AGG
  assertQuery("SELECT COUNT(*), MIN(half_null_fp64) FROM test");
  assertQuery("SELECT COUNT(1), MAX(col_i8) FROM test");
  assertQuery("SELECT COUNT(col_i8), MAX(col_i8) FROM test");

  // COUNT AGG with GROUP BY
  assertQuery(
      "SELECT SUM(col_i32), COUNT(*), SUM(half_null_fp32) FROM test GROUP BY col_i32",
      "",
      true);
  assertQuery(
      "SELECT SUM(col_i32), COUNT(1), SUM(half_null_fp32) FROM test GROUP BY col_i32",
      "",
      true);
  assertQuery(
      "SELECT SUM(col_i32), COUNT(half_null_fp32), SUM(half_null_fp32) FROM test GROUP "
      "BY col_i32",
      "",
      true);

  // COUNT with column is not supported by substrait-java yet
  // COUNT(INT)
  assertQuery("SELECT COUNT(col_i32) FROM test");
  assertQuery("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NOT NULL");
  assertQuery("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NULL");
  // COUNT(BIGINT)
  assertQuery("SELECT COUNT(col_i64) FROM test");
  assertQuery("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NOT NULL");
  assertQuery("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NULL");
  // COUNT(FLOAT)
  assertQuery("SELECT COUNT(col_fp32) FROM test");
  assertQuery("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NOT NULL");
  assertQuery("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NULL");
  // COUNT(DOUBLE)
  assertQuery("SELECT COUNT(col_fp64) FROM test");
  assertQuery("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NOT NULL");
  assertQuery("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NULL");
  // COUNT(INT) with half null
  assertQuery("SELECT COUNT(half_null_i32) FROM test");
  assertQuery("SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NULL");
  // COUNT(BIGINT) with half null
  assertQuery("SELECT COUNT(half_null_i64) FROM test");
  assertQuery("SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NULL");
  // COUNT(FLOAT) with half null
  assertQuery("SELECT COUNT(half_null_fp32) FROM test");
  assertQuery("SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NULL");
  // COUNT(DOUBLE) with half null
  assertQuery("SELECT COUNT(half_null_fp64) FROM test");
  assertQuery("SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NULL");
  // COUNT(SMALLINT) with half null
  assertQuery("SELECT COUNT(half_null_i16) FROM test");
  assertQuery("SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NULL");
  // COUNT(TINYINT) with half null
  assertQuery("SELECT COUNT(half_null_i8) FROM test");
  assertQuery("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NULL");

  // TODO: COUNT(decimal)
  // TODO: COUNT(decimal) with half null
}

TEST_F(CiderAggTest, countDistinctTest) {
  // COUNT(DISTINCT tinyint)
  assertQuery("SELECT COUNT(DISTINCT col_i8) FROM test");
  // COUNT(DISTINCT smallint)
  assertQuery("SELECT COUNT(DISTINCT col_i16) FROM test");
  // COUNT(DISTINCT int)
  assertQuery("SELECT COUNT(DISTINCT col_i32) FROM test");
  // COUNT(DISTINCT bigint)
  assertQuery("SELECT COUNT(DISTINCT col_i64) FROM test");
  // COUNT(DISTINCT tinyint) with half null
  assertQuery("SELECT COUNT(DISTINCT half_null_i8) FROM test");
  assertQuery(
      "SELECT COUNT(DISTINCT half_null_i8) FROM test where half_null_i8 IS NOT NULL");
  // COUNT(DISTINCT smallint) with half null
  assertQuery("SELECT COUNT(DISTINCT half_null_i16) FROM test");
  assertQuery(
      "SELECT COUNT(DISTINCT half_null_i16) FROM test where half_null_i16 IS NOT NULL");
  // COUNT(DISTINCT int) with half null
  assertQuery("SELECT COUNT(DISTINCT half_null_i32) FROM test");
  assertQuery(
      "SELECT COUNT(DISTINCT half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // COUNT(DISTINCT bigint) with half null
  assertQuery("SELECT COUNT(DISTINCT half_null_i64) FROM test");
  assertQuery(
      "SELECT COUNT(DISTINCT half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // SUM(tinyint), COUNT(DISTINCT tinyint)
  assertQuery("SELECT SUM(col_i8), COUNT(DISTINCT col_i8) FROM test");
  // SUM(smallint), COUNT(DISTINCT smallint)
  assertQuery("SELECT SUM(col_i16), COUNT(DISTINCT col_i16) FROM test");
  // SUM(int), COUNT(DISTINCT int)
  assertQuery("SELECT SUM(col_i32), COUNT(DISTINCT col_i32) FROM test");
  // SUM(bigint), COUNT(DISTINCT bigint)
  assertQuery("SELECT SUM(col_i64), COUNT(DISTINCT col_i64) FROM test");
  assertQuery("SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM test");
  assertQuery(
      "SELECT SUM(half_null_i8), COUNT(DISTINCT half_null_i8) FROM test where "
      "half_null_i8 IS NOT NULL");
  // SUM(smallint), COUNT(DISTINCT smallint) with half null
  assertQuery("SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM test");
  assertQuery(
      "SELECT SUM(half_null_i16), COUNT(DISTINCT half_null_i16) FROM test where "
      "half_null_i16 IS NOT NULL");
  // SUM(int), COUNT(DISTINCT int) with half null
  assertQuery("SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM test");
  assertQuery(
      "SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32) FROM test where "
      "half_null_i32 IS NOT NULL");
  // SUM(bigint), COUNT(DISTINCT bigint) with half null
  assertQuery("SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM test");
  assertQuery(
      "SELECT SUM(half_null_i64), COUNT(DISTINCT half_null_i64) FROM test where "
      "half_null_i64 IS NOT NULL");
  // SUM(int), COUNT(DISTINCT int),
  assertQuery(
      "SELECT SUM(col_i32), COUNT(DISTINCT col_i32), COUNT(DISTINCT col_i64) FROM test");
  // SUM(int), COUNT(DISTINCT int), COUNT(DISTINCT bigint) with half null
  assertQuery(
      "SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT "
      "half_null_i64) FROM test");
  assertQuery(
      "SELECT SUM(half_null_i32), COUNT(DISTINCT half_null_i32), COUNT(DISTINCT "
      "half_null_i64) FROM test where half_null_i32 IS NOT NULL AND half_null_i64 IS NOT "
      "NULL");
  // COUNT(DISTINCT int), group by tinyint
  assertQueryIgnoreOrder("SELECT COUNT(DISTINCT col_i32) FROM test GROUP BY col_i8");
  // FIXME: This sql will coredump
  // assertQuery("SELECT col_i8, COUNT(DISTINCT col_i32) FROM test GROUP BY col_i8");
}

TEST_F(CiderAggTest, minOnColumnTest) {
  // agg min with different data type.
  assertQuery("SELECT MIN(col_i8), MIN(col_i64), MIN(col_fp32), MIN(col_fp64) FROM test");

  // agg min with data half null.
  assertQuery(
      "SELECT MIN(half_null_i32), MIN(half_null_i64), MIN(half_null_fp32), "
      "MIN(half_null_fp64) "
      "FROM test");

  // MIN(int)
  assertQuery("SELECT MIN(col_i8) FROM test");
  assertQuery("SELECT MIN(col_i8) FROM test where col_i8 IS NOT NULL");
  // MIN(bigint)
  assertQuery("SELECT MIN(col_i64) FROM test");
  assertQuery("SELECT MIN(col_i64) FROM test where col_i64 IS NOT NULL");
  // MIN(float)
  assertQuery("SELECT MIN(col_fp32) FROM test");
  assertQuery("SELECT MIN(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MIN(double)
  assertQuery("SELECT MIN(col_fp64) FROM test");
  assertQuery("SELECT MIN(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MIN(decimal)
  // MIN(int) with half null
  assertQuery("SELECT MIN(half_null_i32) FROM test");
  assertQuery("SELECT MIN(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MIN(bigint) with half null
  assertQuery("SELECT MIN(half_null_i64) FROM test");
  assertQuery("SELECT MIN(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // MIN(float) with half null
  assertQuery("SELECT MIN(half_null_fp32) FROM test");
  assertQuery("SELECT MIN(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MIN(double) with half null
  assertQuery("SELECT MIN(half_null_fp64) FROM test");
  assertQuery("SELECT MIN(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MIN(decimal) with half null
}

TEST_F(CiderAggTest, maxOnColumnTest) {
  // agg max with different data type.
  assertQuery("SELECT MAX(col_i8), MAX(col_i64), MAX(col_fp32), MAX(col_fp64) FROM test");

  // agg max with data half null.
  assertQuery(
      "SELECT MAX(half_null_i32), MAX(half_null_i64), MAX(half_null_fp32), "
      "MAX(half_null_fp64) "
      "FROM test");

  // MAX(int)
  assertQuery("SELECT MAX(col_i8) FROM test");
  assertQuery("SELECT MAX(col_i8) FROM test where col_i8 IS NOT NULL");
  // MAX(bigint)
  assertQuery("SELECT MAX(col_i64) FROM test");
  assertQuery("SELECT MAX(col_i64) FROM test where col_i64 IS NOT NULL");
  // MAX(float)
  assertQuery("SELECT MAX(col_fp32) FROM test");
  assertQuery("SELECT MAX(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MAX(double)
  assertQuery("SELECT MAX(col_fp64) FROM test");
  assertQuery("SELECT MAX(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MAX(decimal)
  // MAX(int) with half null
  assertQuery("SELECT MAX(half_null_i32) FROM test");
  assertQuery("SELECT MAX(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MAX(bigint) with half null
  assertQuery("SELECT MAX(half_null_i64) FROM test");
  assertQuery("SELECT MAX(half_null_i64) FROM test where half_null_i64 IS NOT NULL");
  // MAX(float) with half null
  assertQuery("SELECT MAX(half_null_fp32) FROM test");
  assertQuery("SELECT MAX(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MAX(double) with half null
  assertQuery("SELECT MAX(half_null_fp64) FROM test");
  assertQuery("SELECT MAX(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MAX(decimal) with half null
}

TEST_F(CiderAggTest, aggWithConditionTest) {
  // multi agg funcs on col with condition.
  assertQuery(
      "SELECT SUM(col_i8), MAX(col_i64), SUM(col_fp32), MIN(col_fp64), COUNT(DISTINCT "
      "col_i32) "
      "FROM test "
      "where col_i8 > 0 and col_i32 > 0 and col_i64 > 0");

  assertQuery(
      "SELECT SUM(half_null_i32), MAX(half_null_i64), SUM(half_null_fp32), "
      "MIN(half_null_fp64) , COUNT(DISTINCT half_null_i16)"
      "FROM test "
      "where half_null_i32 > 0 or half_null_i64 > 0 or half_null_fp32 > 0 or "
      "half_null_fp64 > 0 and half_null_i16 > 0");
}

/*
 =========================================================================
 following tests are Aggregation on expression scenario
 =========================================================================
*/
TEST_F(CiderAggTest, sumOnExpressionTest) {
  assertQuery("SELECT SUM(col_i32 + col_i8), SUM(col_i8 + col_i32) FROM test");
  assertQuery("SELECT SUM(col_i32 - col_i8) FROM test");
  assertQuery("SELECT SUM(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQuery("SELECT SUM(col_i32 / col_i8) FROM test");

  assertQuery("SELECT SUM(col_i32 + 10) FROM test");
  assertQuery("SELECT SUM(col_i32 - 10) FROM test");
  assertQuery("SELECT SUM(col_i32 * 10) FROM test");
  assertQuery("SELECT SUM(col_i32 / 10) FROM test");

  // this may not be a valid case since we support partial agg only.
  // assertQuery("SELECT SUM(col_i32) / 10 FROM test");

  assertQuery("SELECT SUM(col_i32 * 20 + 10) FROM test");
  assertQuery("SELECT SUM(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQuery("SELECT SUM(col_i32 * (1 + col_i8)) FROM test");
  assertQuery("SELECT SUM(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");

  assertQuery(
      "SELECT SUM(half_null_i32 + col_i8), SUM(col_i8 + half_null_i32) FROM test");
  assertQuery("SELECT SUM(half_null_i32 - col_i8) FROM test");
  assertQuery("SELECT SUM(half_null_i32 * col_i8) FROM test");
  // divide zero
  // assertQuery("SELECT SUM(half_null_i32 / col_i8) FROM test");

  assertQuery("SELECT SUM(half_null_i32 + 10) FROM test");
  assertQuery("SELECT SUM(half_null_i32 - 10) FROM test");
  assertQuery("SELECT SUM(half_null_i32 * 10) FROM test");
  assertQuery("SELECT SUM(half_null_i32 / 10) FROM test");
  assertQuery("SELECT SUM(half_null_i32 + 10) FROM test where half_null_i32 IS NOT NULL");
  assertQuery("SELECT SUM(half_null_i32 - 10) FROM test where half_null_i32 IS NOT NULL");
  assertQuery("SELECT SUM(half_null_i32 * 10) FROM test where half_null_i32 IS NOT NULL");
  assertQuery("SELECT SUM(half_null_i32 / 10) FROM test where half_null_i32 IS NOT NULL");

  assertQuery("SELECT SUM(half_null_i32 * 20 + 10) FROM test");
  assertQuery("SELECT SUM(half_null_i32 * 10 + col_i8 * 5) FROM test");
  assertQuery("SELECT SUM(half_null_i32 * (1 + col_i8)) FROM test");
  assertQuery("SELECT SUM(half_null_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

TEST_F(CiderAggTest, minOnExpressionTest) {
  assertQuery("SELECT MIN(col_i32 + col_i8), MIN(col_i8 + col_i32) FROM test");
  assertQuery("SELECT MIN(col_i32 - col_i8) FROM test");
  assertQuery("SELECT MIN(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQuery("SELECT MIN(col_i32 / col_i8) FROM test");

  assertQuery("SELECT MIN(col_i32 + 10) FROM test");
  assertQuery("SELECT MIN(col_i32 - 10) FROM test");
  assertQuery("SELECT MIN(col_i32 * 10) FROM test");
  assertQuery("SELECT MIN(col_i32 / 10) FROM test");

  assertQuery("SELECT MIN(col_i32 * 20 + 10) FROM test");
  assertQuery("SELECT MIN(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQuery("SELECT MIN(col_i32 * (1 + col_i8)) FROM test");
  assertQuery("SELECT MIN(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

TEST_F(CiderAggTest, maxOnExpressionTest) {
  assertQuery("SELECT MAX(col_i32 + col_i8), MAX(col_i8 + col_i32) FROM test");
  assertQuery("SELECT MAX(col_i32 - col_i8) FROM test");
  assertQuery("SELECT MAX(col_i32 * col_i8) FROM test");
  // divide zero
  // assertQuery("SELECT MAX(col_i32 / col_i8) FROM test");

  assertQuery("SELECT MAX(col_i32 + 10) FROM test");
  assertQuery("SELECT MAX(col_i32 - 10) FROM test");
  assertQuery("SELECT MAX(col_i32 * 10) FROM test");
  assertQuery("SELECT MAX(col_i32 / 10) FROM test");
  assertQuery("SELECT MAX(col_i32 + 10) FROM test where col_i32 IS NOT NULL");
  assertQuery("SELECT MAX(col_i32 - 10) FROM test where col_i32 IS NOT NULL");
  assertQuery("SELECT MAX(col_i32 * 10) FROM test where col_i32 IS NOT NULL");
  assertQuery("SELECT MAX(col_i32 / 10) FROM test where col_i32 IS NOT NULL");

  assertQuery("SELECT MAX(col_i32 * 20 + 10) FROM test");
  assertQuery("SELECT MAX(col_i32 * 10 + col_i8 * 5) FROM test");
  assertQuery("SELECT MAX(col_i32 * (1 + col_i8)) FROM test");
  assertQuery("SELECT MAX(col_i32 * (1 + col_i8) * (1 - col_i8)) FROM test");
}

// TODO: move this case to another file. it should belong to function development scope.
TEST_F(CiderAggTest, castTest) {
  assertQuery("SELECT CAST(col_i32 as tinyint) FROM test");
  assertQuery("SELECT CAST(col_i32 as smallint) FROM test");
}

TEST_F(CiderAggTest, sumCastTest) {
  // cast int column
  assertQuery("SELECT SUM(cast(col_i32 as tinyint)) FROM test");
  assertQuery("SELECT SUM(cast(col_i32 as smallint)) FROM test");
  assertQuery("SELECT SUM(cast(col_i32 as float)) FROM test");

  // cast long column
  assertQuery("SELECT SUM(cast(col_i64 as tinyint)) FROM test");
  assertQuery("SELECT SUM(cast(col_i64 as smallint)) FROM test");
  assertQuery("SELECT SUM(cast(col_i64 as int)) FROM test");

  // cast float column
  assertQuery("SELECT SUM(cast(col_fp32 as int)) FROM test");
  assertQuery("SELECT SUM(cast(col_fp32 as double)) FROM test");

  // cast double column
  assertQuery("SELECT SUM(cast(col_fp64 as int)) FROM test");
  assertQuery("SELECT SUM(cast(col_fp64 as bigint)) FROM test");
  assertQuery("SELECT SUM(cast(col_fp64 as float)) FROM test");
}

class CiderPartialAVGIntegerTest : public CiderTestBase {
 public:
  CiderPartialAVGIntegerTest() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_i32 INT, col_i16 SMALLINT, col_i32_with_null INT, col_i32_all_null INT);)";
    std::vector<int32_t> vec_i32;
    vec_i32.push_back(100);
    vec_i32.push_back(110);
    vec_i32.push_back(120);
    std::vector<int16_t> vec_i16;
    vec_i16.push_back(100);
    vec_i16.push_back(110);
    vec_i16.push_back(120);
    std::vector<int32_t> vec_i32_with_null;
    vec_i32_with_null.push_back(100);
    vec_i32_with_null.push_back(NULL_VALUE_I32);
    vec_i32_with_null.push_back(NULL_VALUE_I32);
    std::vector<int32_t> vec_i32_all_null;
    vec_i32_all_null.push_back(NULL_VALUE_I32);
    vec_i32_all_null.push_back(NULL_VALUE_I32);
    vec_i32_all_null.push_back(NULL_VALUE_I32);
    auto batch = std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(3)
            .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), vec_i32)
            .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), vec_i16)
            .addColumn<int32_t>(
                "col_i32_with_null", CREATE_SUBSTRAIT_TYPE(I32), vec_i32_with_null)
            .addColumn<int32_t>(
                "col_i32_all_null", CREATE_SUBSTRAIT_TYPE(I32), vec_i32_all_null)
            .build());
    input_.push_back(batch);
  }
};

TEST_F(CiderPartialAVGIntegerTest, singlePartialAVG) {
  std::vector<double> expect_sum;
  expect_sum.push_back(330.0);
  std::vector<int64_t> expect_count;
  expect_count.push_back(3);

  auto expect_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count)
          .build());
  std::string type_json = R"(
    {
        "struct": {
           "types": [
            {
             "fp64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            },
            {
             "i64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            }
           ],
           "type_variation_reference": 0,
           "nullability": "NULLABILITY_REQUIRED"
        }
    }
    )";
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::vector<::substrait::Type> col_types;
  col_types.push_back(col_type);
  std::vector<std::string> col_names{"a0"};
  auto schema = std::make_shared<CiderTableSchema>(col_names, col_types);
  expect_batch->set_schema(schema);
  // select avg(col_i32) from test
  assertQuery("avg_partial.json", expect_batch);
}

TEST_F(CiderPartialAVGIntegerTest, withNullPartialAVG) {
  std::vector<double> expect_sum;
  expect_sum.push_back(100.0);
  std::vector<int64_t> expect_count;
  expect_count.push_back(1);

  auto expect_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count)
          .build());
  std::string type_json = R"(
    {
        "struct": {
           "types": [
            {
             "fp64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            },
            {
             "i64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            }
           ],
           "type_variation_reference": 0,
           "nullability": "NULLABILITY_REQUIRED"
        }
    }
    )";
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::vector<::substrait::Type> col_types;
  col_types.push_back(col_type);
  std::vector<std::string> col_names{"a0"};
  auto schema = std::make_shared<CiderTableSchema>(col_names, col_types);
  expect_batch->set_schema(schema);
  // select avg(col_i32_all_null) from test
  assertQuery("avg_with_null_partial.json", expect_batch);
}

TEST_F(CiderPartialAVGIntegerTest, allNullPartialAVG) {
  std::vector<double> expect_sum;
  expect_sum.push_back(NULL_VALUE_DOUBLE);
  std::vector<int64_t> expect_count;
  expect_count.push_back(0);

  auto expect_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count)
          .build());
  std::string type_json = R"(
    {
        "struct": {
           "types": [
            {
             "fp64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            },
            {
             "i64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            }
           ],
           "type_variation_reference": 0,
           "nullability": "NULLABILITY_REQUIRED"
        }
    }
    )";
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::vector<::substrait::Type> col_types;
  col_types.push_back(col_type);
  std::vector<std::string> col_names{"a0"};
  auto schema = std::make_shared<CiderTableSchema>(col_names, col_types);
  expect_batch->set_schema(schema);
  // select avg(col_i32_all_null) from test
  assertQuery("avg_all_null_partial.json", expect_batch);
}

TEST_F(CiderPartialAVGIntegerTest, mixedPartialAVG) {
  std::vector<double> expect_sum;
  expect_sum.push_back(330.0);
  std::vector<int64_t> expect_count;
  expect_count.push_back(3);
  std::vector<int64_t> expect_sum_bigint;
  expect_sum_bigint.push_back(330);
  auto expect_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum_bigint)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count)
          .build());
  std::string type_json = R"(
    {
        "struct": {
           "types": [
            {
             "fp64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            },
            {
             "i64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            }
           ],
           "type_variation_reference": 0,
           "nullability": "NULLABILITY_REQUIRED"
        }
    }
    )";
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::string sum_type_json = R"(
    {
      "i64": {
         "typeVariationReference": 0,
         "nullability": "NULLABILITY_REQUIRED"
      }
    }
  )";

  ::substrait::Type sum_type;
  google::protobuf::util::JsonStringToMessage(sum_type_json, &sum_type);
  std::vector<::substrait::Type> col_types;
  col_types.push_back(col_type);
  col_types.push_back(sum_type);
  col_types.push_back(col_type);
  std::vector<std::string> col_names{"a0", "a1", "a2"};
  auto schema = std::make_shared<CiderTableSchema>(col_names, col_types);
  expect_batch->set_schema(schema);
  // select avg(col_i32), sum(col_32), avg(col_i8) from test
  assertQuery("multi_integer_avg_partial.json", expect_batch);
}

class CiderPartialAVGFpTest : public CiderTestBase {
 public:
  CiderPartialAVGFpTest() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_fp64 DOUBLE,col_fp32 FLOAT, col_fp32_with_null FLOAT, col_fp32_all_null FLOAT);)";
    std::vector<double> vec_fp64;
    vec_fp64.push_back(11.11);
    vec_fp64.push_back(22.22);
    vec_fp64.push_back(33.33);
    std::vector<float> vec_fp32;
    vec_fp32.push_back(1.5);
    vec_fp32.push_back(4.6);
    vec_fp32.push_back(7.4);
    std::vector<float> col_fp32_with_null;
    col_fp32_with_null.push_back(NULL_VALUE_FLOAT);
    col_fp32_with_null.push_back(4.6);
    col_fp32_with_null.push_back(7.4);
    std::vector<float> col_fp32_all_null;
    col_fp32_all_null.push_back(NULL_VALUE_FLOAT);
    col_fp32_all_null.push_back(NULL_VALUE_FLOAT);
    col_fp32_all_null.push_back(NULL_VALUE_FLOAT);
    auto batch = std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(3)
            .addColumn<double>("col_fp64", CREATE_SUBSTRAIT_TYPE(Fp64), vec_fp64)
            .addColumn<float>("col_fp32", CREATE_SUBSTRAIT_TYPE(Fp32), vec_fp32)
            .addColumn<float>(
                "col_fp32_with_null", CREATE_SUBSTRAIT_TYPE(Fp32), col_fp32_with_null)
            .addColumn<float>(
                "col_fp32_all_null", CREATE_SUBSTRAIT_TYPE(Fp32), col_fp32_all_null)
            .build());
    input_.push_back(batch);
  }
};

TEST_F(CiderPartialAVGFpTest, mixedPartialAVG) {
  std::vector<double> expect_sum1;
  expect_sum1.push_back((double)66.66);
  std::vector<double> expect_sum2;
  expect_sum2.push_back((double)13.5);
  std::vector<double> expect_sum3;
  expect_sum3.push_back((double)12.0);
  std::vector<double> expect_sum4;
  expect_sum4.push_back(NULL_VALUE_DOUBLE);
  std::vector<int64_t> expect_count1;
  expect_count1.push_back(3);
  std::vector<int64_t> expect_count2;
  expect_count2.push_back(3);
  std::vector<int64_t> expect_count3;
  expect_count3.push_back(2);
  std::vector<int64_t> expect_count4;
  expect_count4.push_back(0);
  auto expect_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum1)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count1)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum2)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count2)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum3)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count3)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), expect_sum4)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_count4)
          .build());
  std::string type_json = R"(
    {
        "struct": {
           "types": [
            {
             "fp64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            },
            {
             "i64": {
              "type_variation_reference": 0,
              "nullability": "NULLABILITY_REQUIRED"
             }
            }
           ],
           "type_variation_reference": 0,
           "nullability": "NULLABILITY_REQUIRED"
        }
    }
    )";
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::vector<::substrait::Type> col_types;
  col_types.push_back(col_type);
  col_types.push_back(col_type);
  col_types.push_back(col_type);
  col_types.push_back(col_type);
  std::vector<std::string> col_names{"a0", "a1", "a2", "a3"};
  auto schema = std::make_shared<CiderTableSchema>(col_names, col_types);
  expect_batch->set_schema(schema);
  // select avg(col_fp64), avg(col_fp32), avg(col_fp32_with_null), avg(col_fp32_all_null)
  assertQuery("multi_fp_avg_partial.json", expect_batch);
}

class CiderCountDistinctConstantTest : public CiderTestBase {
 public:
  CiderCountDistinctConstantTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_i8 TINYINT, col_i32 INT);)";
    std::vector<int8_t> vec_i8;
    vec_i8.push_back(5);
    vec_i8.push_back(3);
    vec_i8.push_back(3);
    std::vector<int32_t> vec_i32;
    vec_i32.push_back(500);
    vec_i32.push_back(500);
    vec_i32.push_back(500);
    auto batch_1 = std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(3)
            .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), vec_i8)
            .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), vec_i32)
            .build());
    vec_i8.clear();
    vec_i8.push_back(4);
    vec_i8.push_back(4);
    vec_i8.push_back(4);
    vec_i32.clear();
    vec_i32.push_back(303);
    vec_i32.push_back(304);
    vec_i32.push_back(305);
    auto batch_2 = std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(3)
            .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), vec_i8)
            .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), vec_i32)
            .build());
    input_.push_back(batch_1);
    input_.push_back(batch_2);
  }
};

TEST_F(CiderCountDistinctConstantTest, countDistinctConstantTest) {
  std::vector<int64_t> expect_col_a_1;
  expect_col_a_1.push_back(2);
  std::vector<int64_t> expect_col_b_1;
  expect_col_b_1.push_back(1);

  auto expect_batch_1 =
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_col_a_1)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_col_b_1)
          .build();
  std::vector<int64_t> expect_col_a_2;
  expect_col_a_2.push_back(1);
  std::vector<int64_t> expect_col_b_2;
  expect_col_b_2.push_back(3);
  auto expect_batch_2 =
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_col_a_2)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), expect_col_b_2)
          .build();
  std::vector<std::shared_ptr<CiderBatch>> expected_batches;
  expected_batches.push_back(std::make_shared<CiderBatch>(std::move(expect_batch_1)));
  expected_batches.push_back(std::make_shared<CiderBatch>(std::move(expect_batch_2)));
  // based on non-groupby agg scenario, result returened based on each batch input
  assertQueryForCountDistinct(
      "SELECT COUNT(DISTINCT col_i8), COUNT(DISTINCT col_i32) FROM test",
      expected_batches);
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
