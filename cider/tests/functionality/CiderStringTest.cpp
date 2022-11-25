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

class CiderStringTest : public CiderTestBase {
 public:
  CiderStringTest() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL);)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)}))};
  }
};

TEST_F(CiderStringTest, LikeEscapeTest) {
  GTEST_SKIP_("Substrait does not support ESCAPE yet.");
  assertQuery("SELECT col_2 FROM test where col_2 LIKE '%aaaa' ESCAPE '$' ");
}

class CiderRandomStringTest : public CiderTestBase {
 public:
  CiderRandomStringTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {},
        GeneratePattern::Random,
        0,
        20))};
  }
};

class CiderNullableStringTest : public CiderTestBase {
 public:
  CiderNullableStringTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        20,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 2},
        GeneratePattern::Random,
        0,
        20))};
  }
};

class CiderStringTestArrow : public CiderTestBase {
 public:
  CiderStringTestArrow() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL);)";

    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        50,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

class CiderStringRandomTestArrow : public CiderTestBase {
 public:
  CiderStringRandomTestArrow() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";

    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        30,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Random,
        0,
        10);
  }
};

class CiderStringNullableTestArrow : public CiderTestBase {
 public:
  CiderStringNullableTestArrow() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER , col_2 VARCHAR(10) );)";

    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        50,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

class CiderStringToDateTest : public CiderTestBase {
 public:
  CiderStringToDateTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_int INTEGER, col_str VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Special_Date_format_String))};
  }
};

class CiderStringToDateTestForArrow : public CiderTestBase {
 public:
  CiderStringToDateTestForArrow() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_int INTEGER, col_str VARCHAR(10));)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        100,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Special_Date_format_String);
  }
};

TEST_F(CiderStringToDateTest, NestedTryCastStringOpTest) {
  // FIXME(kaidi): support cast string to date with arrow format
  GTEST_SKIP();
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) > date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) < date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) IS NOT NULL");
  assertQuery("SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > 2000");
  assertQuery(
      "SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > col_int");
}

TEST_F(CiderStringToDateTestForArrow, NestedTryCastStringOpTest) {
  assertQueryArrow("SELECT * FROM test where CAST(col_str AS DATE) > date '1990-01-11'");
  assertQueryArrow("SELECT * FROM test where CAST(col_str AS DATE) < date '1990-01-11'");
  assertQueryArrow("SELECT * FROM test where CAST(col_str AS DATE) IS NOT NULL");
  assertQueryArrow(
      "SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > 2000");
  assertQueryArrow(
      "SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > col_int");
}

// encoded string's bin_oper support is still in progress in heavydb.
// TEST_F(CiderNullableStringTest, NestedSubstrStringOpBinOperTest) {
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) = '0000000000'");
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) IS NOT NULL");
// }

TEST_F(CiderStringToDateTest, DateStrTest) {
  // FIXME(kaidi): support cast string to date with arrow format
  GTEST_SKIP();
  assertQuery(
      "select col_str from test where col_str between date '1970-01-01' and date "
      "'2077-12-31'",
      "cast_str_to_date_implictly.json");
  assertQuery("SELECT CAST(col_str AS DATE) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test",
              "functions/date/year_cast_string_to_date.json");
}

TEST_F(CiderStringToDateTestForArrow, DateStrTest) {
  assertQueryArrow(
      "select col_str from test where col_str between date '1970-01-01' and date "
      "'2077-12-31'",
      "cast_str_to_date_implictly.json");
  assertQueryArrow("SELECT CAST(col_str AS DATE) FROM test");
  assertQueryArrow("SELECT extract(year from CAST(col_str AS DATE)) FROM test");
  assertQueryArrow("SELECT extract(year from CAST(col_str AS DATE)) FROM test",
                   "functions/date/year_cast_string_to_date.json");
}

TEST_F(CiderStringTest, Substr_Test) {
  assertQuery("SELECT SUBSTRING(col_2, 1, 10) FROM test ", "substr.json");
}

TEST_F(CiderStringTest, SubstrTest) {
  // variable source string
  assertQuery("SELECT SUBSTRING(col_2, 1, 10) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 1, 8) FROM test ");

  // out of range
  assertQuery("SELECT SUBSTRING(col_2, 4, 8) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 0, 12) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 12, 0) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 12, 2) FROM test ");

  // from for
  assertQuery("SELECT SUBSTRING(col_2 from 2 for 8) FROM test ");

  // zero length
  assertQuery("SELECT SUBSTRING(col_2, 4, 0) FROM test ");

  // negative wrap
  assertQuery("SELECT SUBSTRING(col_2, -4, 2) FROM test ");

  // not supported column input for parameter 2/3
  // assertQuery("SELECT SUBSTRING(col_2, col_1, 1) FROM test ");
  // assertQuery("SELECT SUBSTRING(col_2, 1, col_1) FROM test ");

  // The first position in string is 1, 0 is undefined behavior
  // assertQuery("SELECT SUBSTRING(col_2, 0, 8) FROM test ");

  // substrait and isthmus do not accept 2 parameters call while cider support, like
  // substring(str, 1)
}

TEST_F(CiderStringTest, NestedSubstrTest) {
  // TODO: enable this after netsted substring is supported and add more nested cases like
  // substring(toLowerCase("aBc"))
  GTEST_SKIP();
  // variable source string
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 1, 10), 1, 8) FROM test ");

  // out of range
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 4, 8), 0, 12) FROM test ");
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 12, 2), 12, 0) FROM test ");
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 0, 0), 0, 2) FROM test ");

  // from for
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2 from 0 for 10) from 2 for 8) FROM test ");

  // zero length
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 4, 0), 0, 0) FROM test ");

  // negative wrap
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, -4, 2), -1, 1) FROM test ");
}

#define BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)       \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                             \
    ASSERT_FUNC("SELECT col_2 FROM test ");                                   \
    ASSERT_FUNC("SELECT col_1, col_2 FROM test ");                            \
    ASSERT_FUNC("SELECT * FROM test ");                                       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 = 'aaaa'");               \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 = '0000000000'");         \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 <> '0000000000'");        \
    ASSERT_FUNC("SELECT col_1 FROM test where col_2 <> '1111111111'");        \
    ASSERT_FUNC("SELECT col_1, col_2 FROM test where col_2 <> '2222222222'"); \
    ASSERT_FUNC("SELECT * FROM test where col_2 <> 'aaaaaaaaaaa'");           \
    ASSERT_FUNC("SELECT * FROM test where col_2 <> 'abcdefghijklmn'");        \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 IS NOT NULL");            \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 < 'uuu'");                \
  }

#define LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)                   \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '1111%'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111%'");                     \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1234%'");                     \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '22%22'");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '_33%'");                       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '44_%'");                       \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666'");        \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888'");       \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%1111'", "like_wo_cast.json"); \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 NOT LIKE '1111%'");                  \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444'");             \
    ASSERT_FUNC(                                                                         \
        "SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE "        \
        "'%111%'");                                                                      \
  }

#define ESCAPE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)        \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                               \
    GTEST_SKIP_("Substrait does not support ESCAPE yet.");                      \
    ASSERT_FUNC("SELECT col_2 FROM test where col_2 LIKE '%aaaa' ESCAPE '$' "); \
  }

/**
  // not supported for different type info of substr and literal
  assertQuery(
    "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) = '0000'");
  assertQuery(
      "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222',
      '3333')");
**/

#define IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)                    \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_2 IN ('0000000000', '1111111111', '2222222222')", \
        "in_string_array.json");                                                        \
    ASSERT_FUNC("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substr.json");                                  \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substr.json");                                            \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_1 >= 0 and SUBSTRING(col_2, 1, 4) IN "            \
        "('0000', '1111', '2222', '3333')",                                             \
        "in_string_nest_with_binop.json");                                              \
  }

#define BASIC_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME) \
  BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)
#define BASIC_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQueryArrow)

#define LIKE_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME) \
  LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)
#define LIKE_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQueryArrow)

#define ESCAPE_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME) \
  ESCAPE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)
#define ESCAPE_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  ESCAPE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQueryArrow)

#define IN_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME) \
  IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)
#define IN_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQueryArrow)

BASIC_STRING_TEST_UNIT(CiderStringTest, basicStringTest)
LIKE_STRING_TEST_UNIT(CiderStringTest, likeStringTest)
ESCAPE_STRING_TEST_UNIT(CiderStringTest, escapeStringTest)
IN_STRING_TEST_UNIT(CiderStringTest, inStringTest)

BASIC_STRING_TEST_UNIT_ARROW(CiderStringTestArrow, basicStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringTestArrow, likeStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringTestArrow, escapeStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringTestArrow, inStringTest)

BASIC_STRING_TEST_UNIT(CiderRandomStringTest, basicRandomStringTest)
LIKE_STRING_TEST_UNIT(CiderRandomStringTest, likeRandomStringTest)
ESCAPE_STRING_TEST_UNIT(CiderRandomStringTest, escapeRandomStringTest)
IN_STRING_TEST_UNIT(CiderRandomStringTest, inRandomStringTest)

BASIC_STRING_TEST_UNIT_ARROW(CiderStringRandomTestArrow, basicRandomStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringRandomTestArrow, likeRandomStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringRandomTestArrow, escapeRandomStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringRandomTestArrow, inRandomStringTest)

BASIC_STRING_TEST_UNIT(CiderNullableStringTest, basicNullableStringTest)
LIKE_STRING_TEST_UNIT(CiderNullableStringTest, likeNullableStringTest)
ESCAPE_STRING_TEST_UNIT(CiderNullableStringTest, escapeNullableStringTest)
IN_STRING_TEST_UNIT(CiderNullableStringTest, inNullableStringTest)

BASIC_STRING_TEST_UNIT_ARROW(CiderStringNullableTestArrow, basicStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringNullableTestArrow, likeStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringNullableTestArrow, escapeStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringNullableTestArrow, inStringTest)

TEST_F(CiderStringTestArrow, ArrowSubstringNestTest) {
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");
  assertQueryArrow("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

TEST_F(CiderStringRandomTestArrow, ArrowSubstringNestTest) {
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");
  assertQueryArrow("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

TEST_F(CiderStringNullableTestArrow, ArrowBasicStringTest) {
  assertQueryArrow("SELECT col_2 FROM test ");
  assertQueryArrow("SELECT col_1, col_2 FROM test ");
  assertQueryArrow("SELECT * FROM test ");
  assertQueryArrow("SELECT col_2 FROM test where col_2 = 'aaaa'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 = '0000000000'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 <> '0000000000'");
  assertQueryArrow("SELECT col_1 FROM test where col_2 <> '1111111111'");
  assertQueryArrow("SELECT col_1, col_2 FROM test where col_2 <> '2222222222'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 <> 'aaaaaaaaaaa'");
  assertQueryArrow("SELECT * FROM test where col_2 <> 'abcdefghijklmn'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 IS NOT NULL");
  assertQueryArrow("SELECT col_2 FROM test where col_2 < 'uuu'");
}

TEST_F(CiderStringNullableTestArrow, ArrowBasicStringLikeTest) {
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '%1111'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '1111%'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '%1111%'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '%1234%'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '22%22'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '_33%'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '44_%'");

  assertQueryArrow(
      "SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666'");
  assertQueryArrow(
      "SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 LIKE '%1111'",
                   "like_wo_cast.json");
  assertQueryArrow("SELECT col_2 FROM test where col_2 NOT LIKE '1111%'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444'");
  assertQueryArrow(
      "SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE '%111%'");
}

TEST_F(CiderStringNullableTestArrow, ArrowSubstringTest) {
  // variable source string
  assertQueryArrow("SELECT SUBSTRING(col_2, 1, 10) FROM test ");
  assertQueryArrow("SELECT SUBSTRING(col_2, 1, 5) FROM test ");

  // out of range
  assertQueryArrow("SELECT SUBSTRING(col_2, 4, 8) FROM test ");
  assertQueryArrow("SELECT SUBSTRING(col_2, 0, 12) FROM test ");
  assertQueryArrow("SELECT SUBSTRING(col_2, 12, 0) FROM test ");
  assertQueryArrow("SELECT SUBSTRING(col_2, 12, 2) FROM test ");

  // from for
  assertQueryArrow("SELECT SUBSTRING(col_2 from 2 for 8) FROM test ");

  // zero length
  assertQueryArrow("SELECT SUBSTRING(col_2, 4, 0) FROM test ");

  // negative wrap
  assertQueryArrow("SELECT SUBSTRING(col_2, -4, 2) FROM test ");
}

TEST_F(CiderStringTestArrow, ArrowBasicStringTest) {
  assertQueryArrow("SELECT col_2 FROM test ");
  assertQueryArrow("SELECT col_1, col_2 FROM test ");
  assertQueryArrow("SELECT * FROM test ");
  assertQueryArrow("SELECT col_2 FROM test where col_2 = 'aaaa'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 = '0000000000'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 <> '0000000000'");
  assertQueryArrow("SELECT col_1 FROM test where col_2 <> '1111111111'");
  assertQueryArrow("SELECT col_1, col_2 FROM test where col_2 <> '2222222222'");
  assertQueryArrow("SELECT * FROM test where col_2 <> 'aaaaaaaaaaa'");
  assertQueryArrow("SELECT * FROM test where col_2 <> 'abcdefghijklmn'");
  assertQueryArrow("SELECT col_2 FROM test where col_2 IS NOT NULL");
  assertQueryArrow("SELECT col_2 FROM test where col_2 < 'uuu'");
}

TEST_F(CiderStringNullableTestArrow, ArrowSubstringNestTest) {
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQueryArrow("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");

  assertQueryArrow("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

TEST_F(CiderStringTestArrow, ArrowCaseConvertionTest) {
  // select column from table
  assertQueryArrow("SELECT col_2, LOWER(col_2) FROM test;", "stringop_lower.json");
  assertQueryArrow("SELECT col_2, UPPER(col_2) FROM test;", "stringop_upper.json");

  // select literal from table
  assertQueryArrow("SELECT LOWER('aAbBcCdD12') FROM test;",
                   "stringop_lower_literal.json");
  assertQueryArrow("SELECT UPPER('aAbBcCdD12') FROM test;",
                   "stringop_upper_literal.json");

  // string op on filter clause
  assertQueryArrow("SELECT col_2 FROM test WHERE LOWER(col_2) = 'aaaaaaaaaa'",
                   "stringop_lower_condition.json");
  assertQueryArrow("SELECT col_2 FROM test WHERE UPPER(col_2) = 'AAAAAAAAAA'",
                   "stringop_upper_condition.json");

  // nested stringops
  assertQueryArrow(
      "SELECT col_2 FROM test "
      "WHERE UPPER(SUBSTRING(col_2, 1, 4)) = LOWER(SUBSTRING(col_2, 1, 4));",
      "stringop_upper_nested_1.json");
  assertQueryArrow("SELECT col_2 FROM test WHERE UPPER(LOWER(col_2)) = col_2;",
                   "stringop_upper_nested_2.json");

  /// NOTE: (YBRua) Skipped for now because we dont expect queries without FROM clauses.
  /// 1. Behaviors of Cider and DuckDb are different w.r.t. this query.
  ///    DuckDb produces only 1 row, while Cider produces input_row_num rows.
  ///    Because the compiled row_func IR always runs for input_row_num times
  ///    at runtime in current implementation of Cider.
  /// 2. If no input table (no FROM clause) is given, the generated Substrait plan will
  ///    have a "virtualTable" (instead of a "namedTable") as a placeholder input.
  ///    <https://substrait.io/relations/logical_relations/#virtual-table>
  // select literal
  // assertQueryArrow("SELECT LOWER('ABCDEFG');", "stringop_lower_constexpr_null.json");
  // assertQueryArrow("SELECT UPPER('abcdefg');", "stringop_upper_constexpr_null.json");
}

TEST_F(CiderStringNullableTestArrow, ArrowCaseConvertionTest) {
  // select column from table
  assertQueryArrow("SELECT col_2, LOWER(col_2) FROM test;", "stringop_lower_null.json");
  assertQueryArrow("SELECT col_2, UPPER(col_2) FROM test;", "stringop_upper_null.json");

  // select literal from table
  assertQueryArrow("SELECT LOWER('aAbBcCdD12') FROM test;",
                   "stringop_lower_literal_null.json");
  assertQueryArrow("SELECT UPPER('aAbBcCdD12') FROM test;",
                   "stringop_upper_literal_null.json");

  // string op on filter clause
  assertQueryArrow("SELECT col_2 FROM test WHERE LOWER(col_2) = 'aaaaaaaaaa'",
                   "stringop_lower_condition_null.json");
  assertQueryArrow("SELECT col_2 FROM test WHERE UPPER(col_2) = 'AAAAAAAAAA'",
                   "stringop_upper_condition_null.json");
}

TEST_F(CiderStringTestArrow, ConcatTest) {
  // Skipped because Isthmus does not support concatenating two literals
  // assertQueryArrow("SELECT 'foo' || 'bar' FROM test;");

  assertQueryArrow("SELECT col_2 || 'foobar' FROM test;");
  assertQueryArrow("SELECT 'foobar' || col_2 FROM test;");

  // assertQueryArrow("SELECT 'foo' || 'bar' || col_2 FROM test;");
  assertQueryArrow("SELECT 'foo' || col_2 || 'bar' FROM test;");
  assertQueryArrow("SELECT col_2 || 'foo' || 'bar' FROM test;");

  assertQueryArrow("SELECT SUBSTRING(col_2, 1, 3) || 'yo' FROM test;");
  assertQueryArrow("SELECT col_2 FROM test WHERE UPPER('yo' || col_2) <> col_2;",
                   "stringop_concat_filter.json");
}

TEST_F(CiderStringNullableTestArrow, ConcatTest) {
  // assertQueryArrow("SELECT 'foo' || 'bar' FROM test;");

  assertQueryArrow("SELECT col_2 || 'foobar' FROM test;");
  assertQueryArrow("SELECT 'foobar' || col_2 FROM test;");

  // assertQueryArrow("SELECT 'foo' || 'bar' || col_2 FROM test;");
  assertQueryArrow("SELECT 'foo' || col_2 || 'bar' FROM test;");
  assertQueryArrow("SELECT col_2 || 'foo' || 'bar' FROM test;");

  assertQueryArrow("SELECT SUBSTRING(col_2, 1, 3) || 'yo' FROM test;");
  assertQueryArrow("SELECT col_2 FROM test WHERE UPPER(col_2 || 'yo') <> col_2;",
                   "stringop_concat_filter_null.json");
}

TEST_F(CiderStringTestArrow, CharLengthTest) {
  assertQueryArrow("SELECT LENGTH(col_2) FROM test;", "stringop_charlen_project_1.json");
  assertQueryArrow("SELECT LENGTH(col_2) FROM test WHERE SUBSTRING(col_2, 1, 5) = 'bar'",
                   "stringop_charlen_project_2.json");

  assertQueryArrow("SELECT col_2 FROM test WHERE LENGTH(col_2) <> 0;",
                   "stringop_charlen_filter.json");

  assertQueryArrow(
      "SELECT LENGTH(SUBSTRING(col_2, 1, 5)) FROM test "
      "WHERE LENGTH(col_2 || 'boo') = 13;",
      "stringop_charlen_nested.json");
}

TEST_F(CiderStringNullableTestArrow, CharLengthTest) {
  assertQueryArrow("SELECT LENGTH(col_2) FROM test;",
                   "stringop_charlen_project_1_null.json");
  assertQueryArrow("SELECT LENGTH(col_2) FROM test WHERE SUBSTRING(col_2, 1, 5) = 'bar'",
                   "stringop_charlen_project_2_null.json");

  assertQueryArrow("SELECT col_2 FROM test WHERE LENGTH(col_2) <> 0;",
                   "stringop_charlen_filter_null.json");

  assertQueryArrow(
      "SELECT LENGTH(SUBSTRING(col_2, 1, 5)) FROM test "
      "WHERE LENGTH(col_2 || 'boo') = 13;",
      "stringop_charlen_nested_null.json");
}

class CiderTrimOpTestArrow : public CiderTestBase {
 public:
  CiderTrimOpTestArrow() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL, col_3 VARCHAR(10)))";

    auto int_vec = std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    auto string_vec = std::vector<std::string>{"xxxxxxxxxx",
                                               "xxxxxxxxxx",
                                               "   3456789",
                                               "   3456789",
                                               "   3456   ",
                                               "   3456   ",
                                               "0123456   ",
                                               "0123456   ",
                                               "xxx3456   ",
                                               "xxx3456   ",
                                               "",
                                               ""};
    auto is_null = std::vector<bool>{
        false, true, false, true, false, true, false, true, false, true, false, true};
    auto [vc_data, vc_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);

    std::tie(schema_, array_) =
        ArrowArrayBuilder()
            .addColumn("col_1", CREATE_SUBSTRAIT_TYPE(I32), int_vec)
            .addUTF8Column("col_2", vc_data, vc_offsets)
            .addUTF8Column("col_3", vc_data, vc_offsets, is_null)
            .build();
  }
};

TEST_F(CiderTrimOpTestArrow, LiteralTrimTest) {
  // DuckDb syntax: TRIM(string, characters) trims <characters> from <string>
  // basic trim (defaults to trim spaces)
  assertQueryArrow("SELECT TRIM('   3456   ') FROM test", "stringop_trim_literal_1.json");
  // trim other characters
  assertQueryArrow("SELECT TRIM('xxx3456   ', ' x') FROM test",
                   "stringop_trim_literal_2.json");
  assertQueryArrow("SELECT LTRIM('xxx3456xxx', 'x') FROM test",
                   "stringop_ltrim_literal.json");
  assertQueryArrow("SELECT RTRIM('xxx3456xxx', 'x') FROM test",
                   "stringop_rtrim_literal.json");
}

TEST_F(CiderTrimOpTestArrow, ColumnTrimTest) {
  assertQueryArrow("SELECT TRIM(col_2), TRIM(col_3) FROM test", "stringop_trim_1.json");
  assertQueryArrow("SELECT TRIM(col_2, ' x'), TRIM(col_3, ' x') FROM test",
                   "stringop_trim_2.json");

  assertQueryArrow("SELECT LTRIM(col_2), LTRIM(col_3) FROM test",
                   "stringop_ltrim_1.json");
  assertQueryArrow("SELECT LTRIM(col_2, ' x'), LTRIM(col_3, ' x') FROM test",
                   "stringop_ltrim_2.json");

  assertQueryArrow("SELECT RTRIM(col_2), RTRIM(col_3) FROM test",
                   "stringop_rtrim_1.json");
  assertQueryArrow("SELECT RTRIM(col_2, ' x'), RTRIM(col_3, ' x') FROM test",
                   "stringop_rtrim_2.json");
}

TEST_F(CiderTrimOpTestArrow, NestedTrimTest) {
  assertQueryArrow("SELECT TRIM(UPPER(col_2), ' X'), UPPER(TRIM(col_3, 'x')) FROM test",
                   "stringop_trim_nested_1.json");
  assertQueryArrow(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR TRIM(col_3) = 'xxx3456'",
      "stringop_trim_nested_2.json");

  assertQueryArrow("SELECT LTRIM(UPPER(col_2), ' X'), UPPER(LTRIM(col_3, 'x')) FROM test",
                   "stringop_ltrim_nested_1.json");
  assertQueryArrow(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR LTRIM(col_3) = 'xxx3456'",
      "stringop_ltrim_nested_2.json");

  assertQueryArrow("SELECT RTRIM(UPPER(col_2), ' X'), UPPER(RTRIM(col_3, 'x')) FROM test",
                   "stringop_rtrim_nested_1.json");
  assertQueryArrow(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR RTRIM(col_3) = 'xxx3456'",
      "stringop_rtrim_nested_2.json");

  assertQueryArrow("SELECT col_3 FROM test WHERE TRIM(TRIM(col_3, ' '), 'x') = '3456'",
                   "stringop_trim_nested_3.json");
}

class CiderRegexpTestArrow : public CiderTestBase {
 public:
  CiderRegexpTestArrow() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(15) NOT NULL, col_3 VARCHAR(15)))";

    auto int_vec = std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    auto string_vec = std::vector<std::string>{"hello",
                                               "hello",
                                               "helloworldddodo",
                                               "helloworldddodo",
                                               "foo@example.com",
                                               "foo@example.com",
                                               "112@example.com",
                                               "112@example.com",
                                               "123qwerty123",
                                               "123qwerty123",
                                               "",
                                               ""};
    auto is_null = std::vector<bool>{
        false, true, false, true, false, true, false, true, false, true, false, true};
    auto [vc_data, vc_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);

    std::tie(schema_, array_) =
        ArrowArrayBuilder()
            .addColumn("col_1", CREATE_SUBSTRAIT_TYPE(I32), int_vec)
            .addUTF8Column("col_2", vc_data, vc_offsets)
            .addUTF8Column("col_3", vc_data, vc_offsets, is_null)
            .build();
  }
};

TEST_F(CiderRegexpTestArrow, RegexpReplaceBasicTest) {
  // in duckdb, regexp_replace only supports replacing the FIRST or ALL occurrences
  // the behaviour is specified by an optional 'g' argument

  // replace first
  assertQueryArrow(
      "SELECT "
      "REGEXP_REPLACE(col_2, '[wert]', 'yo'), "
      "REGEXP_REPLACE(col_3, '[wert]', 'yo') "
      "FROM test;",
      "stringop_regexp_replace_first.json");
  // replace all
  assertQueryArrow(
      "SELECT "
      "REGEXP_REPLACE(col_2, '[wert]', 'yo', 'g'), "
      "REGEXP_REPLACE(col_3, '[wert]', 'yo', 'g') "
      "FROM test;",
      "stringop_regexp_replace_all.json");

  const auto is_null = std::vector<bool>{
      false, true, false, true, false, true, false, true, false, true, false, true};

  // replace second
  // REGEXP_REPLACE(col, '[0-9]+', '<digits>');
  auto replace_second = std::vector<std::string>{"hello",
                                                 "hello",
                                                 "helloworldddodo",
                                                 "helloworldddodo",
                                                 "foo@example.com",
                                                 "foo@example.com",
                                                 "112@example.com",
                                                 "112@example.com",
                                                 "123qwerty<digits>",
                                                 "123qwerty<digits>",
                                                 "",
                                                 ""};
  const auto [replace_second_data, replace_second_offsets] =
      ArrowBuilderUtils::createDataAndOffsetFromStrVector(replace_second);
  auto replace_second_expected = ArrowBuilderUtils::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addUTF8Column("col_2", replace_second_data, replace_second_offsets)
          .addUTF8Column("col_3", replace_second_data, replace_second_offsets, is_null)
          .build());
  assertQueryArrow("stringop_regexp_replace_second.json", replace_second_expected);
}

TEST_F(CiderRegexpTestArrow, RegexpReplaceExtendedTest) {
  /// NOTE: (YBRua) substrait requires occurrence >= 0 & position > 0
  /// but currently implementation also handled cases where occurence < 0 or position < 0
  /// these cases are also tested here for completeness
  /// although strictly speaking these substrait plans are invalid
}

class CiderConstantStringTest : public CiderTestBase {
 public:
  CiderConstantStringTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 VARCHAR(10));)";

    std::vector<CiderByteArray> vec;
    vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("1111111")));
    vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("1112222")));
    vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
    vec.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("bbbbbbbb")));
    vec.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
    input_.push_back(std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(5)
            .addColumn<CiderByteArray>("col_1", CREATE_SUBSTRAIT_TYPE(Varchar), vec)
            .build()));
  }
};

TEST_F(CiderConstantStringTest, likeStringTest) {
  std::vector<CiderByteArray> expected_vec;
  expected_vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
  expected_vec.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
  auto expected_batch_2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(2)
          .addColumn<CiderByteArray>(
              "col_1", CREATE_SUBSTRAIT_TYPE(Varchar), expected_vec)
          .build());
  assertQuery("SELECT col_1 FROM test where col_1 LIKE '[aa]%'", expected_batch_2);

  expected_vec.clear();
  expected_vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("1111111")));
  expected_vec.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("bbbbbbbb")));
  auto expected_batch_3 = CiderBatchBuilder()
                              .setRowNum(2)
                              .addColumn<CiderByteArray>(
                                  "col_1", CREATE_SUBSTRAIT_TYPE(Varchar), expected_vec)
                              .build();
  // FIXME(jikunshang): Cider only support [],%,_ pattern, !/^ is not supported yet.
  // listed on document.
  // assertQuery("SELECT col_1 FROM test where col_1 LIKE '[!aa]%'",
  // expected_batch_3);
}

TEST_F(CiderRandomStringTest, SubstrTest) {
  assertQuery("SELECT SUBSTRING(col_2, 1, 10) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 1, 8) FROM test ");

  // out of range
  assertQuery("SELECT SUBSTRING(col_2, 4, 8) FROM test ");
  // from for
  assertQuery("SELECT SUBSTRING(col_2 from 2 for 8) FROM test ");
  // zero length
  assertQuery("SELECT SUBSTRING(col_2, 4, 0) FROM test ");

  // not supported column input for parameter 2/3
  // assertQuery("SELECT SUBSTRING(col_2, col_1, 1) FROM test ");
  // assertQuery("SELECT SUBSTRING(col_2, 1, col_1) FROM test ");

  // The first position in string is 1, 0 is undefined behavior
  // assertQuery("SELECT SUBSTRING(col_2, 0, 8) FROM test ");

  // substrait and isthmus do not accept 2 parameters call while cider support, like
  // substring(str, 1)
}

class CiderDuplicateStringTest : public CiderTestBase {
 public:
  CiderDuplicateStringTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 VARCHAR(10), col_2 VARCHAR(10));)";

    std::vector<CiderByteArray> vec1;
    vec1.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
    vec1.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
    vec1.push_back(CiderByteArray(0, reinterpret_cast<const uint8_t*>("")));
    vec1.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
    vec1.push_back(CiderByteArray(3, reinterpret_cast<const uint8_t*>("ddd")));
    vec1.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
    vec1.push_back(CiderByteArray(0, reinterpret_cast<const uint8_t*>("")));
    vec1.push_back(CiderByteArray(0, nullptr));

    std::vector<CiderByteArray> vec2;
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("1")));
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("2")));
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("3")));
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("4")));
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("5")));
    vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("6")));
    vec2.push_back(CiderByteArray(0, reinterpret_cast<const uint8_t*>("")));
    vec2.push_back(CiderByteArray(0, nullptr));
    input_.push_back(std::make_shared<CiderBatch>(
        CiderBatchBuilder()
            .setRowNum(8)
            .addColumn<CiderByteArray>("col_1", CREATE_SUBSTRAIT_TYPE(Varchar), vec1)
            .addColumn<CiderByteArray>("col_2", CREATE_SUBSTRAIT_TYPE(Varchar), vec2)
            .build()));
  }
};

TEST_F(CiderDuplicateStringTest, SingleGroupKeyTest) {
  std::vector<CiderByteArray> expected_vec;
  expected_vec.push_back(CiderByteArray(0, nullptr));
  expected_vec.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
  expected_vec.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
  expected_vec.push_back(CiderByteArray(3, reinterpret_cast<const uint8_t*>("ddd")));
  auto expected_batch_2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(4)
          .addColumn<CiderByteArray>(
              "col_1", CREATE_SUBSTRAIT_TYPE(Varchar), expected_vec)
          .addColumn<int64_t>("col_2", CREATE_SUBSTRAIT_TYPE(I64), {3, 2, 2, 1})
          .build());
  assertQuery("SELECT col_1, COUNT(*) FROM test GROUP BY col_1", expected_batch_2);
}

TEST_F(CiderDuplicateStringTest, MultiGroupKeyTest) {
  std::vector<CiderByteArray> vec1;
  vec1.push_back(CiderByteArray(0, nullptr));
  vec1.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
  vec1.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));
  vec1.push_back(CiderByteArray(0, nullptr));
  vec1.push_back(CiderByteArray(7, reinterpret_cast<const uint8_t*>("aaaaaaa")));
  vec1.push_back(CiderByteArray(3, reinterpret_cast<const uint8_t*>("ddd")));
  vec1.push_back(CiderByteArray(8, reinterpret_cast<const uint8_t*>("aabbccdd")));

  std::vector<CiderByteArray> vec2;
  vec2.push_back(CiderByteArray(0, nullptr));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("1")));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("2")));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("3")));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("4")));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("5")));
  vec2.push_back(CiderByteArray(1, reinterpret_cast<const uint8_t*>("6")));
  auto expected_batch_2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(7)
          .addColumn<CiderByteArray>("col_1", CREATE_SUBSTRAIT_TYPE(Varchar), vec1)
          .addColumn<int64_t>("col_2", CREATE_SUBSTRAIT_TYPE(I64), {2, 1, 1, 1, 1, 1, 1})
          .addColumn<CiderByteArray>("col_1", CREATE_SUBSTRAIT_TYPE(Varchar), vec2)
          .build());
  assertQuery("SELECT col_1, COUNT(*), col_2 FROM test GROUP BY col_1, col_2",
              expected_batch_2);
}

class CiderDuplicateStringArrowTest : public CiderTestBase {
 public:
  CiderDuplicateStringArrowTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 VARCHAR(10), col_2 VARCHAR(10));)";

    std::string str1 = "aaaaaaaaabbccddaaaaaaadddaabbccdd";
    std::vector<int> offset1{0, 7, 15, 15, 22, 25, 33, 33, 33};

    std::string str2 = "123456";
    std::vector<int> offset2{0, 1, 2, 3, 4, 5, 6, 7, 8};

    std::tie(schema_, array_) = ArrowArrayBuilder()
                                    .setRowNum(8)
                                    .addUTF8Column("col_1", str1, offset1)
                                    .addUTF8Column("col_2", str2, offset2)
                                    .build();
  }
};

TEST_F(CiderDuplicateStringArrowTest, SingleGroupKeyTest) {
  std::string res_str = "aaaaaaaaabbccddddd";
  std::vector<int> res_offset{0, 0, 7, 15, 18};
  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;

  std::tie(schema, array) =
      ArrowArrayBuilder()
          .setRowNum(4)
          .addUTF8Column("res_str", res_str, res_offset)
          .addColumn<int64_t>("res_cnt", CREATE_SUBSTRAIT_TYPE(I64), {3, 2, 2, 1})
          .build();
  std::shared_ptr<CiderBatch> res_batch = std::make_shared<CiderBatch>(
      schema, array, std::make_shared<CiderDefaultAllocator>());

  assertQueryArrow("SELECT col_1, COUNT(*) FROM test GROUP BY col_1", res_batch, true);
}

TEST_F(CiderDuplicateStringArrowTest, MultiGroupKeyTest) {
  std::string res_str1 = "aaaaaaaaabbccddaaaaaaadddaabbccdd";
  std::vector<int> res_offset1{0, 0, 7, 15, 15, 22, 25, 33};

  std::string res_str2 = "123456";
  std::vector<int> res_offset2{0, 1, 2, 3, 4, 5, 6, 7};

  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;

  std::tie(schema, array) =
      ArrowArrayBuilder()
          .setRowNum(7)
          .addUTF8Column("res_str1", res_str1, res_offset1)
          .addColumn<int64_t>(
              "res_cnt", CREATE_SUBSTRAIT_TYPE(I64), {2, 1, 1, 1, 1, 1, 1})
          .addUTF8Column("res_str2", res_str2, res_offset2)
          .build();
  std::shared_ptr<CiderBatch> res_batch = std::make_shared<CiderBatch>(
      schema, array, std::make_shared<CiderDefaultAllocator>());
  assertQuery(
      "SELECT col_1, COUNT(*), col_2 FROM test GROUP BY col_1, col_2", res_batch, true);
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
