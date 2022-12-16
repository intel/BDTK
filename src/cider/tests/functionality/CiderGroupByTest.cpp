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
#include "QueryArrowDataGenerator.h"
#include "tests/utils/CiderTestBase.h"

class CiderGroupByVarcharArrowTest : public CiderTestBase {
 public:
  // TODO(yizhong): make col_d to 50% chance nullable and change min length to 0
  // after null string is supported
  CiderGroupByVarcharArrowTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(col_a BIGINT NOT NULL, col_b BIGINT NOT NULL, col_c "
        "VARCHAR NOT NULL, col_d VARCHAR);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  500,
                                                  {"col_a", "col_b", "col_c", "col_d"},
                                                  {CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar),
                                                   CREATE_SUBSTRAIT_TYPE(Varchar)},
                                                  {0, 0, 0, 0},
                                                  GeneratePattern::Random,
                                                  1,
                                                  10);
  }
};

TEST_F(CiderGroupByVarcharArrowTest, varcharGroupByTest) {
  /*single not null group by key*/
  assertQueryArrowIgnoreOrder("SELECT SUM(col_a), col_c FROM table_test GROUP BY col_c");

  /*single null group by key*/
  assertQueryArrowIgnoreOrder("SELECT SUM(col_a), col_d FROM table_test GROUP BY col_d");

  /*one not null varchar group by key*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_c FROM table_test GROUP BY col_a, col_c");

  /*one null varchar group by key */
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d");

  /*two null and not null varchar group by keys*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_c, col_d FROM table_test GROUP BY col_a, col_c, "
      "col_d");

  /*four mixed group by keys*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d");

  // TODO(yizhong): Enable after string compare is supported.
  /*one not null varchar group by key with one condition*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d <> 'a'");

  /*one not null varchar group by key with one not null condition*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d IS NOT NULL");
  /*one null varchar group by key with one null condition*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d IS NULL");

  /*multiple group by keys with multiple having conditions*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_c FROM table_test GROUP BY col_a, col_c HAVING "
      "col_a IS NOT "
      "NULL AND col_c IS NOT NULL ");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_c, col_d FROM table_test GROUP BY col_a, col_c, "
      "col_d HAVING "
      "col_a IS NOT NULL AND col_c <> 'ABC' AND col_d <> 'abc'");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d HAVING col_a IS NOT NULL AND col_b IS NOT NULL AND "
      "col_c <> 'AAA' AND col_d IS NULL ");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d HAVING col_a IS NOT NULL AND col_b IS NOT NULL AND "
      "col_c <> 'AAA' AND col_d IS NOT NULL ");

  /*multiple group by keys with multiple where conditions*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test WHERE col_a IS NOT NULL AND col_d "
      "IS NOT NULL GROUP BY col_a, col_d");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c FROM table_test WHERE col_a IS "
      "NOT NULL AND col_b IS "
      "NOT NULL AND col_c <> 'AAA' GROUP BY col_a, col_b, col_c");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND col_b IS NOT NULL AND col_c <> 'AAA' AND col_d IS NULL "
      "GROUP BY col_a, col_b, col_c, col_d");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND col_b IS NOT NULL AND col_c <> 'AAA' AND col_d IS NOT NULL "
      "GROUP BY col_a, col_b, col_c, col_d");

  /*multiple group by keys with multiple where and having conditions*/
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_d FROM table_test WHERE col_a IS NOT NULL GROUP BY "
      "col_a, col_d HAVING col_d IS NOT NULL");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c FROM table_test WHERE col_a IS "
      "NOT NULL AND col_b IS NOT NULL GROUP BY col_a, col_b, col_c HAVING col_c <> "
      "'AAA'");
  assertQueryArrowIgnoreOrder(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND col_b IS NOT NULL GROUP BY col_a, col_b, col_c, col_d "
      "HAVING col_c <> 'AAA' AND col_d IS NOT NULL ");
}

/* Set to small data set will also cover all cases.
 * We could set to a larger data set after we fix outBatch schema issuem, because
 * of which case like tinyint will exceed the bound if we use large data set now.
 */
#define GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(                                 \
    C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME, TEST_MIN_VALUE, TEST_MAX_VALUE)          \
  class CiderGroupBy##C_TYPE_NAME##ArrowTest : public CiderTestBase {                \
   public:                                                                           \
    CiderGroupBy##C_TYPE_NAME##ArrowTest() {                                         \
      table_name_ = "table_test";                                                    \
      create_ddl_ = "CREATE TABLE table_test(col_a " #TYPE " NOT NULL, col_b " #TYPE \
                    " NOT NULL, col_c " #TYPE " NOT NULL, col_d " #TYPE ");";        \
      QueryArrowDataGenerator::generateBatchByTypes(                                 \
          schema_,                                                                   \
          array_,                                                                    \
          20,                                                                        \
          {"col_a", "col_b", "col_c", "col_d"},                                      \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                               \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                               \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                               \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)},                              \
          {0, 0, 0, 2},                                                              \
          GeneratePattern::Random,                                                   \
          TEST_MIN_VALUE,                                                            \
          TEST_MAX_VALUE);                                                           \
    }                                                                                \
  };

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Float, FLOAT, Fp32, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Double, DOUBLE, Fp64, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Tinyint, TINYINT, I8, -5, 5)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Smallint, SMALLINT, I16, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Integer, INTEGER, I32, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS_ARROW(Bigint, BIGINT, I64, -1000, 1000)

class CiderGroupByPrimitiveTypeMixArrowTest : public CiderTestBase {
 public:
  CiderGroupByPrimitiveTypeMixArrowTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(float_not_null_a FLOAT NOT NULL, float_half_null_b "
        "FLOAT, double_not_null_c DOUBLE NOT NULL, double_half_null_d DOUBLE, "
        "tinyint_not_null_e TINYINT NOT NULL, tinyint_half_null_f TINYINT, "
        "smallint_not_null_g SMALLINT NOT NULL, smallint_half_null_h SMALLINT, "
        "integer_not_null_i INTEGER NOT NULL, integer_half_null_j INTEGER, "
        "bigint_not_null_k BIGINT NOT NULL, bigint_half_null_l BIGINT, "
        "boolean_not_null_m BOOLEAN NOT NULL, boolean_half_null_n BOOLEAN);";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        20,
        {"float_not_null_a",
         "float_half_null_b",
         "double_not_null_c",
         "double_half_null_d",
         "tinyint_not_null_e",
         "tinyint_half_null_f",
         "smallint_not_null_g",
         "smallint_half_null_h",
         "integer_not_null_i",
         "integer_half_null_j",
         "bigint_not_null_k",
         "bigint_half_null_l",
         "boolean_not_null_m",
         "boolean_half_null_n"},
        {CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I16),
         CREATE_SUBSTRAIT_TYPE(I16),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(Bool)},
        {0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2},
        GeneratePattern::Random,
        0,
        100);
  }
};

class CiderGroupByAvgMixArrowTest : public CiderTestBase {
 public:
  CiderGroupByAvgMixArrowTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(float_not_null_a FLOAT NOT NULL, float_half_null_b "
        "FLOAT, float_all_null_c FLOAT, double_not_null_d DOUBLE NOT NULL, "
        "double_half_null_e DOUBLE, double_all_null_f DOUBLE, tinyint_not_null_g TINYINT "
        "NOT NULL, tinyint_half_null_h TINYINT, tinyint_all_null_i TINYINT, "
        "integer_not_null_j INTEGER NOT NULL, integer_half_null_k INTEGER, "
        "integer_all_null_l INTEGER);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  3,
                                                  {"float_not_null_a",
                                                   "float_half_null_b",
                                                   "float_all_null_c",
                                                   "double_not_null_d",
                                                   "double_half_null_e",
                                                   "double_all_null_f",
                                                   "tinyint_not_null_g",
                                                   "tinyint_half_null_h",
                                                   "tinyint_all_null_i",
                                                   "integer_not_null_j",
                                                   "integer_half_null_k",
                                                   "integer_all_null_l"},
                                                  {CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32)},
                                                  {0, 2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1},
                                                  GeneratePattern::Random,
                                                  0,
                                                  100);
  }
};

#define NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME)                     \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg*/                                                    \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test GROUP BY col_a");                \
    /*one group by key with two aggs*/                                                   \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, SUM(col_b), SUM(col_c) FROM table_test GROUP BY col_a");          \
    /*two group by keys with two aggs*/                                                  \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, "   \
        "col_b");                                                                        \
    /*two group by keys with one agg*/                                                   \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a) FROM table_test GROUP BY col_a, col_b");        \
    /*two group by keys with three aggs*/                                                \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test "       \
        "GROUP BY col_a, col_b");                                                        \
    /*three group by keys with three aggs*/                                              \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c");                                                 \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM table_test GROUP BY " \
        "col_a");                                                                        \
    GTEST_SKIP();                                                                        \
    /*TODO(yizhong): enable this case later since "mod" is not supported currently*/     \
    /*select mod in group by*/                                                           \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a % 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM table_test GROUP BY " \
        "col_a");                                                                        \
  }

#define HAVING_GROUP_BY_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME)                           \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg and one gt condition*/                               \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test GROUP BY col_a HAVING col_a > "  \
        "5");                                                                            \
    /*two group by keys with one agg and one gt condition*/                              \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a) FROM table_test GROUP BY col_a, col_b "         \
        "HAVING col_a > 5");                                                             \
    /*two group by keys with two aggs and one gt condition*/                             \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, "   \
        "col_b "                                                                         \
        "HAVING col_a > 5");                                                             \
    /*two group by keys with three aggs and one gt condition*/                           \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test "       \
        "GROUP BY col_a, col_b HAVING col_a > 5");                                       \
    /*three group by keys with three aggs and one gt condition*/                         \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test  "                                                                   \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5");                                \
    /*three group by keys with three aggs and two gt conditions*/                        \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b > 5");                  \
    /*three group by keys with three aggs and three gt conditions*/                      \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b > 5 AND col_c > 5");    \
    /*three group by keys with three aggs and one gt condition and one lt condition*/    \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b < 5");                  \
    /*two params group by with not equal condition*/                                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test GROUP " \
        "BY "                                                                            \
        "col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL");                     \
    /*two params group by with not equal condition*/                                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test GROUP " \
        "BY "                                                                            \
        "col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL");                  \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_d) AS col_d_sum FROM table_test GROUP BY col_d HAVING col_d IS " \
        "NOT NULL");                                                                     \
  }

#define WHERE_GROUP_BY_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME)                            \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg and one gt condition*/                               \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test WHERE col_a > 5 GROUP BY "       \
        "col_a");                                                                        \
    /*two group by keys with one agg and one gt condition*/                              \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a) FROM table_test WHERE col_a > 5 GROUP BY "      \
        "col_a, col_b");                                                                 \
    /*two group by keys with two aggs and one gt condition*/                             \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test WHERE col_a > 5 "   \
        "GROUP "                                                                         \
        "BY col_a, col_b");                                                              \
    /*two group by keys with three aggs and one gt condition*/                           \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test WHERE " \
        "col_a > 5 GROUP BY col_a, col_b");                                              \
    /*three group by keys with three aggs and one gt condition*/                         \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 GROUP BY col_a, col_b, col_c");                                 \
    /*three group by keys with three aggs and two gt conditions*/                        \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE  col_a > 5 AND col_b > 5 GROUP BY col_a, col_b, col_c");                  \
    /*three group by keys with three aggs and three gt conditions*/                      \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 AND col_b > 5 AND col_c > 5 GROUP BY col_a, col_b, col_c");     \
    /*three group by keys with three aggs and one gt condition and one lt condition*/    \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 AND col_b < 5 GROUP BY col_a, col_b, col_c");                   \
    /*two params group by with not equal condition*/                                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b");                             \
  }

#define WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one param group by with gt condition*/                                             \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT col_a, SUM(col_a) FROM table_test WHERE col_a < 8 GROUP BY col_a "       \
        "HAVING col_a > 2");                                                             \
    /*two params group by with not equal condition*/                                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b");       \
    /*two params group by with not equal condition and is not null*/                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND " \
        "col_b IS NOT NULL");                                                            \
    /*two params group by with gt and lt condition*/                                     \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test WHERE col_a < 500 GROUP BY "     \
        "col_a,col_b HAVING col_a > 0 AND col_b < 500");                                 \
    /*three params group by with equal and gt condition*/                                \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS "        \
        "col_c_sum "                                                                     \
        "FROM table_test where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = " \
        "col_b AND col_c > 500");                                                        \
  }

TEST_F(CiderGroupByPrimitiveTypeMixArrowTest, noConditionGroupByColTest) {
  assertQueryArrowIgnoreOrder(
      "SELECT SUM(tinyint_half_null_f), SUM(smallint_not_null_g), "
      "SUM(smallint_half_null_h), SUM(bigint_not_null_k) FROM table_test GROUP BY "
      "tinyint_half_null_f, smallint_not_null_g, smallint_half_null_h, "
      "bigint_not_null_k");

  // FLOAT not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT float_not_null_a, COUNT(*) FROM table_test GROUP BY float_not_null_a");
  assertQueryArrowIgnoreOrder(
      "SELECT float_not_null_a, SUM(double_not_null_c) FROM table_test GROUP BY "
      "float_not_null_a");
  assertQueryArrowIgnoreOrder(
      "SELECT float_not_null_a, AVG(double_not_null_c) FROM table_test GROUP BY "
      "float_not_null_a");
  // DOUBLE not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT double_not_null_c, COUNT(*) FROM table_test GROUP BY double_not_null_c");
  // TINYINT not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT tinyint_not_null_e, COUNT(*) FROM table_test GROUP BY tinyint_not_null_e");
  // SMALLINT not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT smallint_not_null_g, COUNT(*) FROM table_test GROUP BY "
      "smallint_not_null_g");
  // INTEGER not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT integer_not_null_i, COUNT(*) FROM table_test GROUP BY integer_not_null_i");
  // BIGINT not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT bigint_not_null_k, COUNT(*) FROM table_test GROUP BY bigint_not_null_k");
  // BOOLEAN not null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT boolean_not_null_m, COUNT(*) FROM table_test GROUP BY boolean_not_null_m");
  // FLOAT null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT float_half_null_b, COUNT(*) FROM table_test GROUP BY float_half_null_b");
  // DOUBLE null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT double_half_null_d, COUNT(*) FROM table_test GROUP BY double_half_null_d");
  // TINYINT null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT tinyint_half_null_f, COUNT(*) FROM table_test GROUP BY "
      "tinyint_half_null_f");
  // SMALLINT null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT smallint_half_null_h, COUNT(*) FROM table_test GROUP BY "
      "smallint_half_null_h");
  // INTEGER null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT integer_half_null_j, COUNT(*) FROM table_test GROUP BY "
      "integer_half_null_j");
  // BIGINT null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT bigint_half_null_l, COUNT(*) FROM table_test GROUP BY bigint_half_null_l");
  // BOOLEAN null col group by
  assertQueryArrowIgnoreOrder(
      "SELECT boolean_half_null_n, COUNT(*) FROM table_test GROUP BY "
      "boolean_half_null_n");
}

TEST_F(CiderGroupByPrimitiveTypeMixArrowTest, noConditionGroupByMultiColTest) {
  assertQueryArrowIgnoreOrder(
      "SELECT bigint_not_null_k, boolean_not_null_m, COUNT(*), SUM(bigint_not_null_k) "
      "FROM table_test GROUP BY bigint_not_null_k, boolean_not_null_m");
  assertQueryArrowIgnoreOrder(
      "SELECT integer_not_null_i, smallint_not_null_g, COUNT(*), "
      "sum(integer_not_null_i), sum(smallint_not_null_g) FROM table_test GROUP BY "
      "integer_not_null_i, smallint_not_null_g");
  assertQueryArrowIgnoreOrder(
      "SELECT integer_not_null_i, bigint_not_null_k, boolean_not_null_m, COUNT(*), "
      "sum(integer_not_null_i), sum(bigint_not_null_k) FROM table_test GROUP BY "
      "integer_not_null_i, bigint_not_null_k, boolean_not_null_m");
}

TEST_F(CiderGroupByAvgMixArrowTest, avgTest) {
  // AVG(double)
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(double_not_null_d) FROM table_test GROUP BY double_not_null_d");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(double_half_null_e) FROM table_test GROUP BY double_half_null_e");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(double_all_null_f) FROM table_test GROUP BY double_all_null_f");

  // AVG(tinyint)
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(tinyint_not_null_g) FROM table_test GROUP BY tinyint_not_null_g");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(tinyint_half_null_h) FROM table_test GROUP BY tinyint_half_null_h");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(tinyint_all_null_i) FROM table_test GROUP BY tinyint_all_null_i");

  // AVG(integer)
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(integer_not_null_j) FROM table_test GROUP BY integer_not_null_j");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(integer_half_null_k) FROM table_test GROUP BY integer_half_null_k");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(integer_all_null_l) FROM table_test GROUP BY integer_all_null_l");

  // AVG(float)
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(float_all_null_c) FROM table_test GROUP BY float_all_null_c");

  // GroupBy & AVG multiple columns
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(tinyint_not_null_g), AVG(integer_not_null_j) FROM table_test GROUP BY "
      "tinyint_not_null_g, integer_not_null_j");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(tinyint_not_null_g), AVG(integer_half_null_k), AVG(integer_not_null_j)"
      " FROM table_test GROUP BY tinyint_not_null_g, integer_half_null_k, "
      "integer_not_null_j");
  assertQueryArrowIgnoreOrder(
      "SELECT AVG(float_not_null_a), AVG(float_all_null_c), AVG(integer_half_null_k), "
      "AVG(integer_all_null_l) FROM table_test GROUP BY float_not_null_a, "
      "float_all_null_c, integer_half_null_k, integer_all_null_l");
}

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByBigintArrowTest,
                                      noConditionBigintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByBigintArrowTest, havingBigintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByBigintArrowTest, whereBigintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByBigintArrowTest,
                                          whereAndHavingBigintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByFloatArrowTest,
                                      noConditionFloatGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByFloatArrowTest, havingFloatGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByFloatArrowTest, whereFloatGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByFloatArrowTest,
                                          whereAndHavingFloatGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByDoubleArrowTest,
                                      noConditionDoubleGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByDoubleArrowTest, havingDoubleGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByDoubleArrowTest, whereDoubleGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByDoubleArrowTest,
                                          whereAndHavingDoubleGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByTinyintArrowTest,
                                      noConditionTinyintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByTinyintArrowTest, havingTinyintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByTinyintArrowTest, whereTinyintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByTinyintArrowTest,
                                          whereAndHavingTinyintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupBySmallintArrowTest,
                                      noConditionSmallintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupBySmallintArrowTest, havingSmallintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupBySmallintArrowTest, whereSmallintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupBySmallintArrowTest,
                                          whereAndHavingSmallintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByIntegerArrowTest,
                                      noConditionIntegerGroupByTest)
HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByIntegerArrowTest, havingIntegerGroupByTest)
WHERE_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByIntegerArrowTest, whereIntegerGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT_ARROW(CiderGroupByIntegerArrowTest,
                                          whereAndHavingIntegerGroupByTest)

// TODO(yizhong): below is old cider format, to be removed
/* Set to small data set will also cover all cases.
 * We could set to a larger data set after we fix outBatch schema issuem, because
 * of which case like tinyint will exceed the bound if we use large data set now.
 */
#define GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(                                         \
    C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME, TEST_MIN_VALUE, TEST_MAX_VALUE)            \
  class CiderGroupBy##C_TYPE_NAME##Test : public CiderTestBase {                       \
   public:                                                                             \
    CiderGroupBy##C_TYPE_NAME##Test() {                                                \
      table_name_ = "table_test";                                                      \
      create_ddl_ = "CREATE TABLE table_test(col_a " #TYPE " NOT NULL, col_b " #TYPE   \
                    " NOT NULL, col_c " #TYPE " NOT NULL, col_d " #TYPE ");";          \
      input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes( \
          20,                                                                          \
          {"col_a", "col_b", "col_c", "col_d"},                                        \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)},                                \
          {0, 0, 0, 2},                                                                \
          GeneratePattern::Random,                                                     \
          TEST_MIN_VALUE,                                                              \
          TEST_MAX_VALUE))};                                                           \
    }                                                                                  \
  };

class CiderGroupByVarcharTest : public CiderTestBase {
 public:
  // TODO(yizhong): make col_d to 50% chance nullable and change min length to 0
  // after null string is supported
  CiderGroupByVarcharTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(col_a BIGINT NOT NULL, col_b BIGINT NOT NULL, col_c "
        "VARCHAR NOT NULL, col_d VARCHAR);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(500,
                                                 {"col_a", "col_b", "col_c", "col_d"},
                                                 {CREATE_SUBSTRAIT_TYPE(I64),
                                                  CREATE_SUBSTRAIT_TYPE(I64),
                                                  CREATE_SUBSTRAIT_TYPE(Varchar),
                                                  CREATE_SUBSTRAIT_TYPE(Varchar)},
                                                 {0, 0, 0, 0},
                                                 GeneratePattern::Random,
                                                 1,
                                                 10))};
  }
};

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Float, FLOAT, Fp32, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Double, DOUBLE, Fp64, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Tinyint, TINYINT, I8, -5, 5)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Smallint, SMALLINT, I16, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Integer, INTEGER, I32, -1000, 1000)

GEN_PRIMITIVETYPE_GROUP_BY_TEST_CLASS(Bigint, BIGINT, I64, -1000, 1000)

class CiderGroupByPrimitiveTypeMixTest : public CiderTestBase {
 public:
  CiderGroupByPrimitiveTypeMixTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(float_not_null_a FLOAT, float_half_null_b FLOAT, "
        "double_not_null_c DOUBLE, double_half_null_d DOUBLE, "
        "tinyint_not_null_e TINYINT, tinyint_half_null_f TINYINT, smallint_not_null_g "
        "SMALLINT, smallint_half_null_h SMALLINT, integer_not_null_i INTEGER, "
        "integer_half_null_j INTEGER, bigint_not_null_k BIGINT, bigint_half_null_l "
        "BIGINT, boolean_not_null_m BOOLEAN, boolean_half_null_n BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        20,
        {"float_not_null_a",
         "float_half_null_b",
         "double_not_null_c",
         "double_half_null_d",
         "tinyint_not_null_e",
         "tinyint_half_null_f",
         "smallint_not_null_g",
         "smallint_half_null_h",
         "integer_not_null_i",
         "integer_half_null_j",
         "bigint_not_null_k",
         "bigint_half_null_l",
         "boolean_not_null_m",
         "boolean_half_null_n"},
        {CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I16),
         CREATE_SUBSTRAIT_TYPE(I16),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(Bool)},
        {0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2, 0, 2},
        GeneratePattern::Random,
        0,
        100))};
  }
};

class CiderGroupByAvgMixTest : public CiderTestBase {
 public:
  CiderGroupByAvgMixTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(float_not_null_a FLOAT, float_half_null_b FLOAT, "
        "float_all_null_c FLOAT, double_not_null_d DOUBLE, double_half_null_e DOUBLE, "
        "double_all_null_f DOUBLE, tinyint_not_null_g TINYINT, tinyint_half_null_h "
        "TINYINT, tinyint_all_null_i TINYINT, integer_not_null_j INTEGER, "
        "integer_half_null_k INTEGER, integer_all_null_l INTEGER);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(3,
                                                 {"float_not_null_a",
                                                  "float_half_null_b",
                                                  "float_all_null_c",
                                                  "double_not_null_d",
                                                  "double_half_null_e",
                                                  "double_all_null_f",
                                                  "tinyint_not_null_g",
                                                  "tinyint_half_null_h",
                                                  "tinyint_all_null_i",
                                                  "integer_not_null_j",
                                                  "integer_half_null_k",
                                                  "integer_all_null_l"},
                                                 {CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(I8),
                                                  CREATE_SUBSTRAIT_TYPE(I8),
                                                  CREATE_SUBSTRAIT_TYPE(I8),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(I32)},
                                                 {0, 2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1},
                                                 GeneratePattern::Random,
                                                 0,
                                                 100))};
  }
};

#define NO_CONDITION_GROUP_BY_TEST_UNIT(TEST_CLASS, UNIT_NAME)                           \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg*/                                                    \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test GROUP BY col_a", "", true);      \
    /*one group by key with two aggs*/                                                   \
    assertQuery("SELECT col_a, SUM(col_b), SUM(col_c) FROM table_test GROUP BY col_a",   \
                "",                                                                      \
                true);                                                                   \
    /*two group by keys with two aggs*/                                                  \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, "   \
        "col_b",                                                                         \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with one agg*/                                                   \
    assertQuery("SELECT col_a, col_b, SUM(col_a) FROM table_test GROUP BY col_a, col_b", \
                "",                                                                      \
                true);                                                                   \
    /*two group by keys with three aggs*/                                                \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test "       \
        "GROUP BY col_a, col_b",                                                         \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs*/                                              \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c",                                                  \
        "",                                                                              \
        true);                                                                           \
    assertQuery(                                                                         \
        "SELECT col_a / 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM table_test GROUP BY " \
        "col_a",                                                                         \
        "",                                                                              \
        true);                                                                           \
    GTEST_SKIP();                                                                        \
    /*TODO(yizhong): enable this case later since "mod" is not supported currently*/     \
    /*select mod in group by*/                                                           \
    assertQuery(                                                                         \
        "SELECT col_a % 2 AS col_a_2, SUM(col_a) AS col_a_sum FROM table_test GROUP BY " \
        "col_a",                                                                         \
        "",                                                                              \
        true);                                                                           \
  }

#define HAVING_GROUP_BY_TEST_UNIT(TEST_CLASS, UNIT_NAME)                                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg and one gt condition*/                               \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test GROUP BY col_a HAVING col_a > "  \
        "5",                                                                             \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with one agg and one gt condition*/                              \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a) FROM table_test GROUP BY col_a, col_b "         \
        "HAVING col_a > 5",                                                              \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with two aggs and one gt condition*/                             \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test GROUP BY col_a, "   \
        "col_b "                                                                         \
        "HAVING col_a > 5",                                                              \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with three aggs and one gt condition*/                           \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test "       \
        "GROUP BY col_a, col_b HAVING col_a > 5",                                        \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and one gt condition*/                         \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test  "                                                                   \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5",                                 \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and two gt conditions*/                        \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b > 5",                   \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and three gt conditions*/                      \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b > 5 AND col_c > 5",     \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and one gt condition and one lt condition*/    \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "GROUP BY col_a, col_b, col_c HAVING col_a > 5 AND col_b < 5",                   \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with not equal condition*/                                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test GROUP " \
        "BY "                                                                            \
        "col_a, col_b HAVING col_a <> col_b AND col_a IS NOT NULL",                      \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with not equal condition*/                                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test GROUP " \
        "BY "                                                                            \
        "col_a, col_b HAVING col_a IS NOT NULL AND col_b IS NOT NULL",                   \
        "",                                                                              \
        true);                                                                           \
    assertQuery(                                                                         \
        "SELECT SUM(col_d) AS col_d_sum FROM table_test GROUP BY col_d HAVING col_d IS " \
        "NOT NULL",                                                                      \
        "",                                                                              \
        true);                                                                           \
  }

#define WHERE_GROUP_BY_TEST_UNIT(TEST_CLASS, UNIT_NAME)                                  \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one group by key with one agg and one gt condition*/                               \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test WHERE col_a > 5 GROUP BY col_a", \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with one agg and one gt condition*/                              \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a) FROM table_test WHERE col_a > 5 GROUP BY "      \
        "col_a, "                                                                        \
        "col_b",                                                                         \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with two aggs and one gt condition*/                             \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b) FROM table_test WHERE col_a > 5 "   \
        "GROUP "                                                                         \
        "BY col_a, col_b",                                                               \
        "",                                                                              \
        true);                                                                           \
    /*two group by keys with three aggs and one gt condition*/                           \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, SUM(col_a), SUM(col_b), SUM(col_c) FROM table_test WHERE " \
        "col_a > 5 GROUP BY col_a, col_b",                                               \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and one gt condition*/                         \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 GROUP BY col_a, col_b, col_c",                                  \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and two gt conditions*/                        \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE  col_a > 5 AND col_b > 5 GROUP BY col_a, col_b, col_c",                   \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and three gt conditions*/                      \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 AND col_b > 5 AND col_c > 5 GROUP BY col_a, col_b, col_c",      \
        "",                                                                              \
        true);                                                                           \
    /*three group by keys with three aggs and one gt condition and one lt condition*/    \
    assertQuery(                                                                         \
        "SELECT col_a, col_b, col_c, SUM(col_a), SUM(col_b), SUM(col_c) FROM "           \
        "table_test "                                                                    \
        "WHERE col_a > 5 AND col_b < 5 GROUP BY col_a, col_b, col_c",                    \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with not equal condition*/                                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b",                              \
        "",                                                                              \
        true);                                                                           \
  }

#define WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(TEST_CLASS, UNIT_NAME)                       \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*one param group by with gt condition*/                                             \
    assertQuery(                                                                         \
        "SELECT col_a, SUM(col_a) FROM table_test WHERE col_a < 8 GROUP BY col_a "       \
        "HAVING col_a > 2",                                                              \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with not equal condition*/                                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a <> col_b",        \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with not equal condition and is not null*/                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum FROM table_test WHERE " \
        "col_a < 500 AND col_b <> 0 GROUP BY col_a, col_b HAVING col_a IS NOT NULL AND " \
        "col_b IS NOT NULL",                                                             \
        "",                                                                              \
        true);                                                                           \
    /*two params group by with gt and lt condition*/                                     \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum FROM table_test WHERE col_a < 500 GROUP BY "     \
        "col_a,col_b HAVING col_a > 0 AND col_b < 500",                                  \
        "",                                                                              \
        true);                                                                           \
    /*three params group by with equal and gt condition*/                                \
    assertQuery(                                                                         \
        "SELECT SUM(col_a) AS col_a_sum, SUM(col_b) AS col_b_sum, SUM(col_c) AS "        \
        "col_c_sum "                                                                     \
        "FROM table_test where col_a < 500 GROUP BY col_a, col_b, col_c HAVING col_a = " \
        "col_b AND col_c > 500",                                                         \
        "",                                                                              \
        true);                                                                           \
  }

TEST_F(CiderGroupByPrimitiveTypeMixTest, noConditionGroupByColTest) {
  assertQuery(
      "SELECT SUM(tinyint_half_null_f), SUM(smallint_not_null_g), "
      "SUM(smallint_half_null_h), SUM(bigint_not_null_k) FROM table_test GROUP BY "
      "tinyint_half_null_f, smallint_not_null_g, smallint_half_null_h, bigint_not_null_k",
      "",
      true);

  // FLOAT not null col group by
  assertQuery(
      "SELECT float_not_null_a, COUNT(*) FROM table_test GROUP BY float_not_null_a",
      "",
      true);
  assertQuery(
      "SELECT float_not_null_a, SUM(double_not_null_c) FROM table_test GROUP BY "
      "float_not_null_a",
      "",
      true);
  assertQuery(
      "SELECT float_not_null_a, AVG(double_not_null_c) FROM table_test GROUP BY "
      "float_not_null_a",
      "",
      true);
  // DOUBLE not null col group by
  assertQuery(
      "SELECT double_not_null_c, COUNT(*) FROM table_test GROUP BY double_not_null_c",
      "",
      true);
  // TINYINT not null col group by
  assertQuery(
      "SELECT tinyint_not_null_e, COUNT(*) FROM table_test GROUP BY tinyint_not_null_e",
      "",
      true);
  // SMALLINT not null col group by
  assertQuery(
      "SELECT smallint_not_null_g, COUNT(*) FROM table_test GROUP BY "
      "smallint_not_null_g",
      "",
      true);
  // INTEGER not null col group by
  assertQuery(
      "SELECT integer_not_null_i, COUNT(*) FROM table_test GROUP BY integer_not_null_i",
      "",
      true);
  // BIGINT not null col group by
  assertQuery(
      "SELECT bigint_not_null_k, COUNT(*) FROM table_test GROUP BY bigint_not_null_k",
      "",
      true);
  // BOOLEAN not null col group by
  assertQuery(
      "SELECT boolean_not_null_m, COUNT(*) FROM table_test GROUP BY boolean_not_null_m",
      "",
      true);
  // FLOAT null col group by
  assertQuery(
      "SELECT float_half_null_b, COUNT(*) FROM table_test GROUP BY float_half_null_b",
      "",
      true);
  // DOUBLE null col group by
  assertQuery(
      "SELECT double_half_null_d, COUNT(*) FROM table_test GROUP BY double_half_null_d",
      "",
      true);
  // TINYINT null col group by
  assertQuery(
      "SELECT tinyint_half_null_f, COUNT(*) FROM table_test GROUP BY "
      "tinyint_half_null_f",
      "",
      true);
  // SMALLINT null col group by
  assertQuery(
      "SELECT smallint_half_null_h, COUNT(*) FROM table_test GROUP BY "
      "smallint_half_null_h",
      "",
      true);
  // INTEGER null col group by
  assertQuery(
      "SELECT integer_half_null_j, COUNT(*) FROM table_test GROUP BY "
      "integer_half_null_j",
      "",
      true);
  // BIGINT null col group by
  assertQuery(
      "SELECT bigint_half_null_l, COUNT(*) FROM table_test GROUP BY bigint_half_null_l",
      "",
      true);
  // BOOLEAN null col group by
  assertQuery(
      "SELECT boolean_half_null_n, COUNT(*) FROM table_test GROUP BY "
      "boolean_half_null_n",
      "",
      true);
}

TEST_F(CiderGroupByPrimitiveTypeMixTest, noConditionGroupByMultiColTest) {
  assertQuery(
      "SELECT bigint_not_null_k, boolean_not_null_m, COUNT(*), SUM(bigint_not_null_k) "
      "FROM table_test GROUP BY bigint_not_null_k, boolean_not_null_m",
      "",
      true);
  assertQuery(
      "SELECT integer_not_null_i, smallint_not_null_g, COUNT(*), "
      "sum(integer_not_null_i), sum(smallint_not_null_g) FROM table_test GROUP BY "
      "integer_not_null_i, smallint_not_null_g",
      "",
      true);
  assertQuery(
      "SELECT integer_not_null_i, bigint_not_null_k, boolean_not_null_m, COUNT(*), "
      "sum(integer_not_null_i), sum(bigint_not_null_k) FROM table_test GROUP BY "
      "integer_not_null_i, bigint_not_null_k, boolean_not_null_m",
      "",
      true);
}

TEST_F(CiderGroupByAvgMixTest, avgTest) {
  // AVG(double)
  assertQuery("SELECT AVG(double_not_null_d) FROM table_test GROUP BY double_not_null_d",
              "",
              true);
  assertQuery(
      "SELECT AVG(double_half_null_e) FROM table_test GROUP BY double_half_null_e",
      "",
      true);
  assertQuery("SELECT AVG(double_all_null_f) FROM table_test GROUP BY double_all_null_f",
              "",
              true);

  // AVG(tinyint)
  assertQuery(
      "SELECT AVG(tinyint_not_null_g) FROM table_test GROUP BY tinyint_not_null_g",
      "",
      true);
  assertQuery(
      "SELECT AVG(tinyint_half_null_h) FROM table_test GROUP BY tinyint_half_null_h",
      "",
      true);
  assertQuery(
      "SELECT AVG(tinyint_all_null_i) FROM table_test GROUP BY tinyint_all_null_i",
      "",
      true);

  // AVG(integer)
  assertQuery(
      "SELECT AVG(integer_not_null_j) FROM table_test GROUP BY integer_not_null_j",
      "",
      true);
  assertQuery(
      "SELECT AVG(integer_half_null_k) FROM table_test GROUP BY integer_half_null_k",
      "",
      true);
  assertQuery(
      "SELECT AVG(integer_all_null_l) FROM table_test GROUP BY integer_all_null_l",
      "",
      true);

  // AVG(float)
  assertQuery(
      "SELECT AVG(float_all_null_c) FROM table_test GROUP BY float_all_null_c", "", true);
  // TODO(yizhong): Failed due to unexpected output schema from substrait-java.
  GTEST_SKIP();
  assertQuery(
      "SELECT AVG(float_not_null_a) FROM table_test GROUP BY float_not_null_a", "", true);
  assertQuery("SELECT AVG(float_half_null_b) FROM table_test GROUP BY float_half_null_b",
              "",
              true);

  assertQuery(
      "SELECT AVG(float_not_null_a), AVG(float_half_null_b) FROM table_test GROUP BY "
      "float_not_null_a, float_half_null_b",
      "",
      true);
  assertQuery(
      "SELECT AVG(float_not_null_a), AVG(float_half_null_b), AVG(float_all_null_c), "
      "AVG(double_not_null_d), AVG(double_half_null_e), AVG(double_all_null_f), "
      "AVG(tinyint_not_null_g), AVG(tinyint_half_null_h), AVG(tinyint_all_null_i), "
      "AVG(integer_not_null_j), AVG(integer_half_null_k), AVG(integer_all_null_l) FROM "
      "table_test GROUP BY float_not_null_a, float_half_null_b, float_all_null_c, "
      "double_not_null_d, double_half_null_e, double_all_null_f, tinyint_not_null_g, "
      "tinyint_half_null_h, tinyint_all_null_i, integer_not_null_j, integer_half_null_k, "
      "integer_all_null_l, ",
      "",
      true);
}

TEST_F(CiderGroupByVarcharTest, varcharGroupByTest) {
  /*single not null group by key*/
  assertQuery("SELECT SUM(col_a), col_c FROM table_test GROUP BY col_c", "", true);

  /*single null group by key*/
  assertQuery("SELECT SUM(col_a), col_d FROM table_test GROUP BY col_d", "", true);

  /*one not null varchar group by key*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_c FROM table_test GROUP BY col_a, col_c", "", true);

  /*one null varchar group by key */
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d", "", true);

  /*two null and not null varchar group by keys*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_c, col_d FROM table_test GROUP BY col_a, col_c, "
      "col_d",
      "",
      true);

  /*four mixed group by keys*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d",
      "",
      true);

  /*one not null varchar group by key with one condition*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d <> 'a'",
      "",
      true);

  /*one not null varchar group by key with one not null condition*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d IS NOT NULL",
      "",
      true);

  /*one null varchar group by key with one null condition*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test GROUP BY col_a, col_d HAVING "
      "col_d IS NULL",
      "",
      true);

  /*multiple group by keys with multiple having conditions*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_c FROM table_test GROUP BY col_a, col_c HAVING "
      "col_a IS NOT "
      "NULL AND col_c IS NOT NULL ",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_c, col_d FROM table_test GROUP BY col_a, col_c, "
      "col_d HAVING "
      "col_a IS NOT NULL AND col_c <> 'ABC' AND col_d <> 'abc'",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d HAVING col_a IS NOT NULL AND col_b IS NOT NULL AND "
      "col_c <> 'AAA' AND col_d IS NULL ",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test GROUP "
      "BY col_a, col_b, col_c, col_d HAVING col_a IS NOT NULL AND col_b IS NOT NULL AND "
      "col_c <> 'AAA' AND col_d IS NOT NULL ",
      "",
      true);

  /*multiple group by keys with multiple where conditions*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test WHERE col_a IS NOT NULL AND col_d "
      "IS NOT NULL GROUP BY col_a, col_d",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c FROM table_test WHERE col_a IS "
      "NOT NULL AND col_b IS "
      "NOT NULL AND col_c <> 'AAA' GROUP BY col_a, col_b, col_c",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND col_b IS NOT NULL AND col_c <> 'AAA' AND col_d IS NULL "
      "GROUP BY col_a, col_b, col_c, col_d",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND col_b IS NOT NULL AND col_c <> 'AAA' AND col_d IS NOT NULL "
      "GROUP BY col_a, col_b, col_c, col_d",
      "",
      true);

  /*multiple group by keys with multiple where and having conditions*/
  assertQuery(
      "SELECT col_a, SUM(col_a), col_d FROM table_test WHERE col_a IS NOT NULL GROUP BY "
      "col_a, col_d HAVING col_d IS NOT NULL",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c FROM table_test WHERE col_a IS "
      "NOT NULL AND col_b IS NOT NULL GROUP BY col_a, col_b, col_c HAVING col_c <> 'AAA'",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND "
      "col_b IS NOT NULL GROUP BY col_a, col_b, col_c, col_d HAVING col_c <> 'AAA' AND "
      "col_d IS NULL",
      "",
      true);
  assertQuery(
      "SELECT col_a, SUM(col_a), col_b, SUM(col_b), col_c, col_d FROM table_test WHERE "
      "col_a IS NOT NULL AND "
      "col_b IS NOT NULL GROUP BY col_a, col_b, col_c, col_d HAVING col_c <> 'AAA' AND "
      "col_d IS NOT NULL ",
      "",
      true);
}

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupByBigintTest, noConditionBigintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupByBigintTest, havingBigintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupByBigintTest, whereBigintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupByBigintTest,
                                    whereAndHavingBigintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupByFloatTest, noConditionFloatGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupByFloatTest, havingFloatGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupByFloatTest, whereFloatGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupByFloatTest, whereAndHavingFloatGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupByDoubleTest, noConditionDoubleGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupByDoubleTest, havingDoubleGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupByDoubleTest, whereDoubleGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupByDoubleTest,
                                    whereAndHavingDoubleGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupByTinyintTest, noConditionTinyintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupByTinyintTest, havingTinyintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupByTinyintTest, whereTinyintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupByTinyintTest,
                                    whereAndHavingTinyintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupBySmallintTest, noConditionSmallintGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupBySmallintTest, havingSmallintGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupBySmallintTest, whereSmallintGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupBySmallintTest,
                                    whereAndHavingSmallintGroupByTest)

NO_CONDITION_GROUP_BY_TEST_UNIT(CiderGroupByIntegerTest, noConditionIntegerGroupByTest)
HAVING_GROUP_BY_TEST_UNIT(CiderGroupByIntegerTest, havingIntegerGroupByTest)
WHERE_GROUP_BY_TEST_UNIT(CiderGroupByIntegerTest, whereIntegerGroupByTest)
WHERE_AND_HAVING_GROUP_BY_TEST_UNIT(CiderGroupByIntegerTest,
                                    whereAndHavingIntegerGroupByTest)

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  log_options.parse_command_line(argc, argv);
  log_options.max_files_ = 0;  // stderr only by default
  logger::init(log_options);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  
  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
