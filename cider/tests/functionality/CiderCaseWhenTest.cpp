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
#include "tests/utils/CiderTestBase.h"

// Extends CiderTestBase and create a (99 rows, 4 types columns) table for filter test.
class CiderCaseWhenSequenceTestBase : public CiderTestBase {
 public:
  CiderCaseWhenSequenceTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_int", "col_bigint", "col_double", "col_float", "col_str"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)}))};
  }
};

class CiderCaseWhenSequenceWithNullTestBase : public CiderTestBase {
 public:
  CiderCaseWhenSequenceWithNullTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_int", "col_bigint", "col_double", "col_float"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {3, 3, 3, 3, 3}))};
  }
};

class CiderCaseWhenRandomWithNullTestBase : public CiderTestBase {
 public:
  CiderCaseWhenRandomWithNullTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    // FIXME: (yma11) for col_str, filter result in some cases(if len is random from 0) will be
    // incorrect in DuckDB as the different handlement of len 0 varchar and null varchar,
    // refer POAE7-2385 which may not need fix after switch to arrow format
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"col_int", "col_bigint", "col_double", "col_float", "col_str"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        2,
        102))};
  }
};

#define COALESCE_FUNCTION_TEST(TEST_CLASS, UNIT_NAME)                                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                     \
    assertQuery("select coalesce(COL_INT, COL_BIGINT, 7) from test",                  \
                "functions/conditional/coalesce.json");                               \
    assertQuery("select coalesce(COL_INT, COL_BIGINT, COL_DOUBLE) from test",         \
                "functions/conditional/coalesce_null.json");                          \
    assertQuery("select sum(coalesce(COL_INT, COL_BIGINT, COL_DOUBLE, 7)) from test", \
                "functions/conditional/coalesce_sum.json");                           \
  }

COALESCE_FUNCTION_TEST(CiderCaseWhenSequenceTestBase, coalesceFunctionTest);
COALESCE_FUNCTION_TEST(CiderCaseWhenSequenceWithNullTestBase, coalesceFunctionTest);
COALESCE_FUNCTION_TEST(CiderCaseWhenRandomWithNullTestBase, coalesceFunctionTest);

// COALESCE expression is a syntactic shortcut for the CASE expression
// the code COALESCE(expression1,...n) is same as the following CASE expression:
// CASE
// WHEN (expression1 IS NOT NULL) THEN expression1
// WHEN (expression2 IS NOT NULL) THEN expression2
// ...
// ELSE expressionN
// END
#define COALESCE_TEST(TEST_CLASS, UNIT_NAME)                                             \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_int, col_bigint, 777) FROM test",                           \
        "SELECT CASE WHEN col_int is not null THEN col_int WHEN col_bigint is not "      \
        "null THEN col_bigint ELSE 777 END from test");                                  \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_int, col_double, 777) FROM test",                           \
        "SELECT CASE WHEN col_int is not null THEN col_int WHEN col_double is not "      \
        "null THEN col_double ELSE 777 END from test");                                  \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_int, col_bigint, col_double, 777) FROM test",               \
        "SELECT CASE WHEN col_int is not null THEN col_int WHEN col_bigint is not null " \
        "THEN col_bigint "                                                               \
        "WHEN col_double is not null THEN col_double ELSE 777 END from test");           \
    assertQuery("SELECT COALESCE(col_float) FROM test",                                  \
                "SELECT CASE WHEN col_float is not null THEN col_float ELSE null END "   \
                "from test");                                                            \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_double, col_int) FROM test",                                \
        "SELECT CASE WHEN col_double is not null THEN col_double WHEN col_int is not "   \
        "null THEN col_int ELSE null END from test");                                    \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_int, col_double) FROM test",                                \
        "SELECT CASE WHEN col_int is not null THEN col_int WHEN col_double is not "      \
        "null THEN col_double ELSE null END from test");                                 \
    assertQuery(                                                                         \
        "SELECT COALESCE(col_int, col_bigint, col_double, col_float) FROM test",         \
        "SELECT CASE WHEN col_int is not null THEN col_int WHEN col_bigint is not "      \
        "null THEN col_bigint WHEN col_double is not null THEN col_double WHEN "         \
        "col_float "                                                                     \
        "is not null THEN col_float ELSE null END from test");                           \
  }

COALESCE_TEST(CiderCaseWhenSequenceTestBase, coalesceTest);
COALESCE_TEST(CiderCaseWhenSequenceWithNullTestBase, coalesceTest);
COALESCE_TEST(CiderCaseWhenRandomWithNullTestBase, coalesceTest);

#define COALESCE_WITH_AGG_TEST(TEST_CLASS, UNIT_NAME)                                    \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    assertQuery("SELECT sum(COALESCE(col_int, 777)) FROM test",                          \
                "SELECT sum(CASE WHEN col_int is not null THEN col_int ELSE 777 END) "   \
                "from test");                                                            \
    assertQuery("SELECT sum(COALESCE(col_double)) FROM test",                            \
                "SELECT sum(CASE WHEN col_double is not null THEN col_double ELSE "      \
                "null END) from test");                                                  \
    assertQuery("SELECT count(COALESCE(col_double, 777)) FROM test",                     \
                "SELECT count(CASE WHEN col_double is not null THEN col_double "         \
                "ELSE 777 END) from test");                                              \
    assertQuery("SELECT sum(COALESCE(col_double, 777)) FROM test",                       \
                "SELECT sum(CASE WHEN col_double is not null THEN col_double ELSE "      \
                "777 END) from test");                                                   \
    assertQuery("SELECT sum(COALESCE(col_int, col_bigint, col_double, 777)) FROM test",  \
                "SELECT sum(CASE WHEN col_int is not null THEN col_int WHEN col_bigint " \
                "is not null THEN col_bigint WHEN col_double is not null THEN "          \
                "col_double ELSE 777 END) from test");                                   \
    assertQuery("SELECT sum(COALESCE(col_int, col_bigint, col_double)) FROM test",       \
                "SELECT sum(CASE WHEN col_int is not null THEN col_int WHEN col_bigint " \
                "is not null THEN col_bigint WHEN col_double is not null THEN "          \
                "col_double ELSE null END) from test");                                  \
  }

COALESCE_WITH_AGG_TEST(CiderCaseWhenSequenceTestBase, coalescewithAggTest);
COALESCE_WITH_AGG_TEST(CiderCaseWhenSequenceWithNullTestBase, coalescewithAggTest);
COALESCE_WITH_AGG_TEST(CiderCaseWhenRandomWithNullTestBase, coalescewithAggTest);

// The IF function is actually a language construct that is equivalent to the following
// CASE expression: CASE
//    WHEN condition THEN true_value
//    [ ELSE false_value ]
// END
// The Scalar Function IF from presto will be transalted to if-then expression in
// substrait plan to cider, so tests are between IF function and case when
#define IF_TEST(TEST_CLASS, UNIT_NAME)                                                  \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertQuery("SELECT IF(col_int = 1 ,10, 1) FROM test",                              \
                "SELECT CASE WHEN col_int = 1 THEN 10 ELSE 1 END FROM test");           \
    assertQuery(                                                                        \
        "SELECT IF(col_int > 20, col_bigint, col_double) FROM test",                    \
        "SELECT CASE WHEN col_int > 20 THEN col_bigint ELSE col_double END FROM test"); \
    assertQuery(                                                                        \
        "SELECT SUM(IF(col_int > 20, col_double, 5)) FROM test",                        \
        "SELECT SUM(CASE WHEN col_int > 20 THEN col_double ELSE 5 END) FROM test");     \
    assertQuery(                                                                        \
        "SELECT SUM(IF(col_double > 20, 5, col_double)) FROM test",                     \
        "SELECT SUM(CASE WHEN col_double > 20 THEN 5 ELSE col_double END) FROM test");  \
  }

IF_TEST(CiderCaseWhenSequenceTestBase, ifTest);
IF_TEST(CiderCaseWhenSequenceWithNullTestBase, ifTest);
IF_TEST(CiderCaseWhenRandomWithNullTestBase, ifTest);

#define CASE_WHEN_TEST(TEST_CLASS, UNIT_NAME)                                                                                               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                                                                           \
    assertQuery("SELECT col_int, CASE WHEN col_int > 3 THEN 1 ELSE 0 END FROM test");                                                       \
    assertQuery("SELECT col_int, CASE WHEN col_bigint < 9 THEN 2 ELSE 1 END FROM test");                                                    \
    assertQuery(                                                                                                                            \
        "SELECT col_bigint, CASE WHEN col_double > 5 THEN 3 ELSE 2 END FROM test");                                                         \
    assertQuery("SELECT col_double, CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM test");                                                    \
    assertQuery("SELECT col_int, CASE WHEN col_int > 30 THEN 1 ELSE 0 END FROM test");                                                      \
    assertQuery(                                                                                                                            \
        "SELECT col_bigint, CASE WHEN col_double > 50 THEN 3 ELSE 2 END FROM test");                                                        \
    assertQuery("SELECT SUM(CASE WHEN col_int >50 THEN 10 ELSE 1 END) FROM test");                                                          \
    assertQuery(                                                                                                                            \
        R"(SELECT SUM(CASE WHEN col_bigint < 30 THEN 10 WHEN col_bigint > 70 THEN 20 WHEN col_bigint = 50 THEN 30 ELSE 1 END) FROM test)"); \
    assertQuery(                                                                                                                            \
        "SELECT col_int, CASE WHEN col_int = 1 THEN 10 WHEN col_int = 2 THEN 20 ELSE 0 "                                                    \
        "END "                                                                                                                              \
        "FROM test");                                                                                                                       \
    assertQuery(                                                                                                                            \
        "SELECT col_int, CASE WHEN col_int = 1 THEN 10 WHEN col_int = 2 THEN 20 WHEN "                                                      \
        "col_int = 3 THEN 30 ELSE 0 END FROM test");                                                                                        \
    assertQuery("SELECT col_int, CASE WHEN col_int = 1 THEN 10 END FROM test");                                                             \
    assertQuery(                                                                                                                            \
        "SELECT col_int, CASE WHEN col_int = 1 THEN 10 WHEN col_int = 3 THEN 20 WHEN "                                                      \
        "col_int = 3 THEN 30 END FROM test");                                                                                               \
  }

#define CASE_WHEN_AGG_TEST(TEST_CLASS, UNIT_NAME)                                                                               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                                                               \
    assertQuery("SELECT SUM(CASE WHEN col_int = 1 THEN 10 ELSE 1 END) FROM test");                                              \
    assertQuery(                                                                                                                \
        R"(SELECT SUM(CASE WHEN col_int = 1 THEN 10 WHEN col_int = 3 THEN 20 WHEN col_int = 3 THEN 30 ELSE 1 END) FROM test)"); \
    assertQuery(                                                                                                                \
        R"(SELECT SUM(CASE WHEN col_int = 3 THEN 11 ELSE 2 END), SUM(CASE WHEN col_double = 3 THEN 12 ELSE 3 END) FROM test)"); \
    assertQuery(                                                                                                                \
        R"(SELECT SUM(CASE col_int WHEN 3 THEN 11 ELSE 2 END), SUM(CASE col_double WHEN 3 THEN 12 ELSE 3 END) FROM test)");     \
    assertQuery(                                                                                                                \
        "SELECT sum(col_double), CASE WHEN col_int > 5 THEN 4 ELSE 3 END FROM test "                                            \
        "GROUP BY CASE "                                                                                                        \
        "WHEN col_int > 5 THEN 4 ELSE 3 END");                                                                                  \
    assertQuery(                                                                                                                \
        "SELECT sum(col_double), col_bigint, CASE WHEN col_int > 5 THEN 4 ELSE 3 END "                                          \
        "FROM test GROUP BY "                                                                                                   \
        "col_bigint, (CASE WHEN col_int > 5 THEN 4 ELSE 3 END)",                                                                \
        "SELECT sum(col_double), col_bigint, CASE WHEN col_int > 5 THEN 4 ELSE 3 END "                                          \
        "FROM test GROUP BY "                                                                                                   \
        "col_bigint, (CASE WHEN col_int > 5 THEN 4 ELSE 3 END)",                                                                \
        true);                                                                                                                  \
    assertQuery(                                                                                                                \
        R"(SELECT SUM(CASE WHEN col_int < 10 THEN 11 END), SUM(CASE WHEN col_double > 10 THEN 12 END) FROM test)");             \
    assertQuery(                                                                                                                \
        "SELECT SUM(CASE WHEN col_int > 5 THEN 4 ELSE 3 END), CASE WHEN col_int > "                                             \
        "5 THEN 4 ELSE 3 END FROM test GROUP BY CASE WHEN col_int > 5 THEN 4 ELSE 3 "                                           \
        "END");                                                                                                                 \
    assertQuery(                                                                                                                \
        "SELECT count(1), SUM(CASE WHEN col_int = 7 THEN col_bigint END), SUM(CASE "                                            \
        "WHEN col_int = 5 THEN col_bigint + 3 END), SUM(CASE WHEN col_int = 7 THEN "                                            \
        "col_double * 2 "                                                                                                       \
        "ELSE col_double / 2 END) FROM test");                                                                                  \
    assertQuery(                                                                                                                \
        "SELECT col_int,  SUM(CASE WHEN col_int = 5 THEN col_bigint + 3 END), SUM(CASE "                                        \
        "WHEN col_int = 7 THEN col_double * 2 ELSE col_double / 2 END) FROM test GROUP "                                        \
        "BY col_int");                                                                                                          \
  }
CASE_WHEN_TEST(CiderCaseWhenSequenceTestBase, caseWhenTest);
CASE_WHEN_TEST(CiderCaseWhenSequenceWithNullTestBase, caseWhenTest);
CASE_WHEN_TEST(CiderCaseWhenRandomWithNullTestBase, caseWhenTest);
CASE_WHEN_AGG_TEST(CiderCaseWhenSequenceTestBase, caseWhenAggTest);
CASE_WHEN_AGG_TEST(CiderCaseWhenSequenceWithNullTestBase, caseWhenAggTest);
CASE_WHEN_AGG_TEST(CiderCaseWhenRandomWithNullTestBase, caseWhenAggTest);

#define STRING_TEST(TEST_CLASS, UNIT_NAME)                                               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    assertQuery("SELECT COALESCE(col_str) FROM test",                                    \
                "SELECT CASE WHEN col_str IS NOT NULL THEN col_str ELSE NULL END "       \
                "FROM test");                                                            \
    assertQuery(                                                                         \
        "SELECT col_int, CASE WHEN col_str IS NULL THEN 10 WHEN col_int > 3 THEN 20 "    \
        "ELSE 0 "                                                                        \
        "END FROM test");                                                                \
    assertQuery(                                                                         \
        "SELECT col_int, CASE WHEN col_str < 'uuu' THEN 10 WHEN col_int > 3 THEN 20 "    \
        "ELSE 0 END FROM test");                                                         \
    assertQuery(                                                                         \
        "SELECT col_str, CASE WHEN col_str like '%mm%' THEN 10 WHEN col_int > 3 THEN "   \
        "20 "                                                                            \
        "ELSE 0 "                                                                        \
        "END FROM test");                                                                \
    assertQuery(                                                                         \
        "SELECT sum(col_double) FROM test GROUP BY CASE WHEN col_str IS NULL THEN 4 "    \
        "ELSE 3 "                                                                        \
        "END");                                                                          \
    assertQuery(                                                                         \
        "SELECT sum(col_double) FROM test GROUP BY CASE WHEN col_str > 'ttt' THEN 4 "    \
        "ELSE 3 "                                                                        \
        "END",                                                                           \
        "",                                                                              \
        true);                                                                           \
    assertQuery(                                                                         \
        "SELECT sum(col_double), CASE WHEN col_str > 'ttt' THEN 4 ELSE 3 END FROM test " \
        "GROUP BY CASE "                                                                 \
        "WHEN col_str > 'ttt' THEN 4 ELSE 3 END",                                        \
        "",                                                                              \
        true);                                                                           \
  }

STRING_TEST(CiderCaseWhenSequenceTestBase, stringTest);
STRING_TEST(CiderCaseWhenSequenceWithNullTestBase, stringTest);
STRING_TEST(CiderCaseWhenRandomWithNullTestBase, stringTest);

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
