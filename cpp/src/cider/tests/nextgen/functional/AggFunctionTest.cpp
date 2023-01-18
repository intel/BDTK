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
#include "tests/utils/CiderNextgenTestBase.h"

#define NULL_VALUE_I32 std::numeric_limits<int32_t>::min()
#define NULL_VALUE_FLOAT std::numeric_limits<float>::min()
#define NULL_VALUE_DOUBLE std::numeric_limits<double>::min()

class CiderAggTest : public cider::test::util::CiderNextgenTestBase {
 public:
  CiderAggTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_i8 TINYINT NOT NULL, col_i16 SMALLINT NOT NULL, col_i32 "
        "INT NOT NULL, col_i64 "
        "BIGINT NOT NULL, col_fp32 FLOAT NOT NULL, col_fp64 DOUBLE NOT NULL, "
        "half_null_i8 "
        "TINYINT, half_null_i16 SMALLINT, half_null_i32 INT, half_null_i64 BIGINT, "
        "half_null_fp32 FLOAT, half_null_fp64 DOUBLE);";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
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
TEST_F(CiderAggTest, sumTest) {
  // SUM(tinyint)
  assertQuery("SELECT SUM(col_i8) FROM test");
  // SUM(smallint)
  assertQuery("SELECT SUM(col_i16) FROM test");
  // SUM(int)
  assertQuery("SELECT SUM(col_i32) FROM test");
  // SUM(bigint)
  assertQuery("SELECT SUM(col_i64) FROM test");

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

  // Skip test whose function is not supported
  GTEST_SKIP();
  // SUM(float)
  assertQuery("SELECT SUM(col_fp32) FROM test");
  // SUM(double)
  assertQuery("SELECT SUM(col_fp64) FROM test");
  // TODO: SUM(decimal)
  // SUM(float) with half null
  assertQuery("SELECT SUM(half_null_fp32) FROM test");
  assertQuery("SELECT SUM(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // SUM(double) with half null
  assertQuery("SELECT SUM(half_null_fp64) FROM test");
  assertQuery("SELECT SUM(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: SUM(decimal) with half null
}

TEST_F(CiderAggTest, countTest) {
  //   // In cider, COUNT(*) has same syntax as COUNT(1)
  // COUNT(*)
  assertQuery("SELECT COUNT(*) FROM test");

  // COUNT(1)
  assertQuery("SELECT COUNT(1) FROM test");

  // COUNT AGG
  assertQuery("SELECT COUNT(1), MAX(col_i8) FROM test");
  assertQuery("SELECT COUNT(col_i8), MAX(col_i8) FROM test");

  // COUNT(INT)
  assertQuery("SELECT COUNT(col_i32) FROM test");
  assertQuery("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NOT NULL");
  assertQuery("SELECT COUNT(col_i32) FROM test WHERE col_i32 IS NULL");
  // COUNT(BIGINT)
  assertQuery("SELECT COUNT(col_i64) FROM test");
  assertQuery("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NOT NULL");
  assertQuery("SELECT COUNT(col_i64) FROM test WHERE col_i64 IS NULL");

  // COUNT(INT) with half null
  assertQuery("SELECT COUNT(half_null_i32) FROM test");
  assertQuery("SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i32) FROM test WHERE half_null_i32 IS NULL");
  // COUNT(BIGINT) with half null
  assertQuery("SELECT COUNT(half_null_i64) FROM test");
  assertQuery("SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i64) FROM test WHERE half_null_i64 IS NULL");

  // COUNT(SMALLINT) with half null
  assertQuery("SELECT COUNT(half_null_i16) FROM test");
  assertQuery("SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i16) FROM test WHERE half_null_i16 IS NULL");
  // COUNT(TINYINT) with half null
  assertQuery("SELECT COUNT(half_null_i8) FROM test");
  assertQuery("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_i8) FROM test WHERE half_null_i8 IS NULL");

  // Skip test whose function is not supported
  GTEST_SKIP();

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

  //   COUNT AGG
  assertQuery("SELECT COUNT(*), MIN(half_null_fp64) FROM test");

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
  // COUNT(FLOAT)
  assertQuery("SELECT COUNT(col_fp32) FROM test");
  assertQuery("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NOT NULL");
  assertQuery("SELECT COUNT(col_fp32) FROM test WHERE col_fp32 IS NULL");
  // COUNT(DOUBLE)
  assertQuery("SELECT COUNT(col_fp64) FROM test");
  assertQuery("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NOT NULL");
  assertQuery("SELECT COUNT(col_fp64) FROM test WHERE col_fp64 IS NULL");

  // COUNT(FLOAT) with half null
  assertQuery("SELECT COUNT(half_null_fp32) FROM test");
  assertQuery("SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_fp32) FROM test WHERE half_null_fp32 IS NULL");
  // COUNT(DOUBLE) with half null
  assertQuery("SELECT COUNT(half_null_fp64) FROM test");
  assertQuery("SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NOT NULL");
  assertQuery("SELECT COUNT(half_null_fp64) FROM test WHERE half_null_fp64 IS NULL");

  // TODO: COUNT(decimal)
  // TODO: COUNT(decimal) with half null
}

TEST_F(CiderAggTest, countDistinctTest) {
  // Skip test whose function is not supported
  GTEST_SKIP();
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
  // MIN(int)
  assertQuery("SELECT MIN(col_i8) FROM test");
  assertQuery("SELECT MIN(col_i8) FROM test where col_i8 IS NOT NULL");
  // MIN(bigint)
  assertQuery("SELECT MIN(col_i64) FROM test");
  assertQuery("SELECT MIN(col_i64) FROM test where col_i64 IS NOT NULL");

  // MIN(int) with half null
  assertQuery("SELECT MIN(half_null_i32) FROM test");
  assertQuery("SELECT MIN(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MIN(bigint) with half null
  assertQuery("SELECT MIN(half_null_i64) FROM test");
  assertQuery("SELECT MIN(half_null_i64) FROM test where half_null_i64 IS NOT NULL");

  // Skip test whose function is not supported
  GTEST_SKIP();
  // agg min with different data type.
  assertQuery("SELECT MIN(col_i8), MIN(col_i64), MIN(col_fp32), MIN(col_fp64) FROM test");

  // agg min with data half null.
  assertQuery(
      "SELECT MIN(half_null_i32), MIN(half_null_i64), MIN(half_null_fp32), "
      "MIN(half_null_fp64) "
      "FROM test");

  // MIN(float)
  assertQuery("SELECT MIN(col_fp32) FROM test");
  assertQuery("SELECT MIN(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MIN(double)
  assertQuery("SELECT MIN(col_fp64) FROM test");
  assertQuery("SELECT MIN(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MIN(decimal)

  // MIN(float) with half null
  assertQuery("SELECT MIN(half_null_fp32) FROM test");
  assertQuery("SELECT MIN(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MIN(double) with half null
  assertQuery("SELECT MIN(half_null_fp64) FROM test");
  assertQuery("SELECT MIN(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MIN(decimal) with half null
}

TEST_F(CiderAggTest, maxOnColumnTest) {
  // MAX(int)
  assertQuery("SELECT MAX(col_i8) FROM test");
  assertQuery("SELECT MAX(col_i8) FROM test where col_i8 IS NOT NULL");
  // MAX(bigint)
  assertQuery("SELECT MAX(col_i64) FROM test");
  assertQuery("SELECT MAX(col_i64) FROM test where col_i64 IS NOT NULL");

  // MAX(int) with half null
  assertQuery("SELECT MAX(half_null_i32) FROM test");
  assertQuery("SELECT MAX(half_null_i32) FROM test where half_null_i32 IS NOT NULL");
  // MAX(bigint) with half null
  assertQuery("SELECT MAX(half_null_i64) FROM test");
  assertQuery("SELECT MAX(half_null_i64) FROM test where half_null_i64 IS NOT NULL");

  // Skip test whose function is not supported
  GTEST_SKIP();
  // agg max with different data type.
  assertQuery("SELECT MAX(col_i8), MAX(col_i64), MAX(col_fp32), MAX(col_fp64) FROM test");

  // agg max with data half null.
  assertQuery(
      "SELECT MAX(half_null_i32), MAX(half_null_i64), MAX(half_null_fp32), "
      "MAX(half_null_fp64) "
      "FROM test");

  // MAX(float)
  assertQuery("SELECT MAX(col_fp32) FROM test");
  assertQuery("SELECT MAX(col_fp32) FROM test where col_fp32 IS NOT NULL");
  // MAX(double)
  assertQuery("SELECT MAX(col_fp64) FROM test");
  assertQuery("SELECT MAX(col_fp64) FROM test where col_fp64 IS NOT NULL");
  // TODO: MAX(decimal)

  // MAX(float) with half null
  assertQuery("SELECT MAX(half_null_fp32) FROM test");
  assertQuery("SELECT MAX(half_null_fp32) FROM test where half_null_fp32 IS NOT NULL");
  // MAX(double) with half null
  assertQuery("SELECT MAX(half_null_fp64) FROM test");
  assertQuery("SELECT MAX(half_null_fp64) FROM test where half_null_fp64 IS NOT NULL");
  // TODO: MAX(decimal) with half null
}

TEST_F(CiderAggTest, aggWithConditionTest) {
  // Skip test whose function is not supported
  GTEST_SKIP();
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

// /*
//  =========================================================================
//  following tests are Aggregation on expression scenario
//  =========================================================================
// */

TEST_F(CiderAggTest, sumOnExpressionTest) {
  assertQuery("SELECT SUM(col_i32 - col_i8) FROM test");
  assertQuery("SELECT SUM(col_i32 * col_i8) FROM test");
  assertQuery("SELECT SUM(col_i32 + col_i8), SUM(col_i8 + col_i32) FROM test");

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
  // Skip because core dumped
  GTEST_SKIP();
  assertQuery("SELECT CAST(col_i32 as tinyint) FROM test");
  assertQuery("SELECT CAST(col_i32 as smallint) FROM test");
}

TEST_F(CiderAggTest, sumCastTest) {
  // Skip because core dumped
  GTEST_SKIP();
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

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
