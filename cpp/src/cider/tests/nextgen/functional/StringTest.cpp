/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#include "exec/nextgen/Nextgen.h"
#include "tests/utils/CiderNextgenTestBase.h"

using namespace cider::test::util;
using namespace cider::exec::nextgen;

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

#define IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, ASSERT_FUNC)                    \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_2 IN ('0000000000', '1111111111', '2222222222')", \
        "in_string_array.json");                                                        \
    ASSERT_FUNC("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substr.json");                                  \
    ASSERT_FUNC("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substring.json");                               \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substr.json");                                            \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substring.json");                                         \
    ASSERT_FUNC(                                                                        \
        "SELECT * FROM test WHERE col_1 >= 0 and SUBSTRING(col_2, 1, 4) IN "            \
        "('0000', '1111', '2222', '3333')",                                             \
        "in_string_nest_with_binop.json");                                              \
  }

#define BASIC_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  BASIC_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)

#define LIKE_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  LIKE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)

#define ESCAPE_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  ESCAPE_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)

#define IN_STRING_TEST_UNIT_ARROW(TEST_CLASS, UNIT_NAME) \
  IN_STRING_TEST_UNIT_BASE(TEST_CLASS, UNIT_NAME, assertQuery)

// basic string functionalities

class CiderStringTestNextGen : public CiderNextgenTestBase {
 public:
  CiderStringTestNextGen() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL);)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        50,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

class CiderStringRandomTestNextGen : public CiderNextgenTestBase {
 public:
  CiderStringRandomTestNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        30,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Random,
        0,
        10);
  }
};

class CiderStringNullableTestNextGen : public CiderNextgenTestBase {
 public:
  CiderStringNullableTestNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER , col_2 VARCHAR(10) );)";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        50,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Sequence,
        0,
        10);
  }
};

BASIC_STRING_TEST_UNIT_ARROW(CiderStringTestNextGen, BasicStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringTestNextGen, LikeStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringTestNextGen, EscapeStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringTestNextGen, InStringTest)

BASIC_STRING_TEST_UNIT_ARROW(CiderStringRandomTestNextGen, BasicRandomStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringRandomTestNextGen, LikeRandomStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringRandomTestNextGen, EscapeRandomStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringRandomTestNextGen, InRandomStringTest)

BASIC_STRING_TEST_UNIT_ARROW(CiderStringNullableTestNextGen, BasicStringTest)
LIKE_STRING_TEST_UNIT_ARROW(CiderStringNullableTestNextGen, LikeStringTest)
ESCAPE_STRING_TEST_UNIT_ARROW(CiderStringNullableTestNextGen, EscapeStringTest)
IN_STRING_TEST_UNIT_ARROW(CiderStringNullableTestNextGen, InStringTest)

// duplicate string

class CiderDuplicateStringTestNextGen : public CiderNextgenTestBase {
 public:
  CiderDuplicateStringTestNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 VARCHAR(10), col_2 VARCHAR(10));)";

    std::string str1 = "aaaaaaaaabbccddaaaaaaadddaabbccdd";
    std::vector<int> offset1{0, 7, 15, 15, 22, 25, 33, 33, 33};

    std::string str2 = "123456";
    std::vector<int> offset2{0, 1, 2, 3, 4, 5, 6, 7, 8};

    std::tie(input_schema_, input_array_) = ArrowArrayBuilder()
                                                .setRowNum(8)
                                                .addUTF8Column("col_1", str1, offset1)
                                                .addUTF8Column("col_2", str2, offset2)
                                                .build();
  }
};

TEST_F(CiderDuplicateStringTestNextGen, SingleGroupKeyTest) {
  // TODO: (YBRua) Enable this after nextgen supports GROUP BY
  GTEST_SKIP_("string group-by is not supported yet in nextgen");
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

  assertQuery("SELECT col_1, COUNT(*) FROM test GROUP BY col_1", array, schema, true);
}

TEST_F(CiderDuplicateStringTestNextGen, MultiGroupKeyTest) {
  // TODO: (YBRua) Enable this after nextgen supports GROUP BY
  GTEST_SKIP_("string group-by is not supported yet in nextgen");
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
  assertQuery("SELECT col_1, COUNT(*), col_2 FROM test GROUP BY col_1, col_2",
              array,
              schema,
              true);
}

// constant string

class CiderConstantStringTestNextGen : public CiderNextgenTestBase {
 public:
  CiderConstantStringTestNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 VARCHAR(10));)";

    auto vec =
        std::vector<std::string>{"1111111", "1112222", "aaaaaaa", "bbbbbbbb", "aabbccdd"};

    auto [data, offset] = ArrowBuilderUtils::createDataAndOffsetFromStrVector(vec);
    std::tie(input_schema_, input_array_) =
        ArrowArrayBuilder().setRowNum(5).addUTF8Column("col_1", data, offset).build();
  }
};

TEST_F(CiderConstantStringTestNextGen, LikeStringTest) {
  auto expected_vec = std::vector<std::string>{"aaaaaaa", "aabbccdd"};
  auto [data, offset] = ArrowBuilderUtils::createDataAndOffsetFromStrVector(expected_vec);

  struct ArrowArray* expect_array{nullptr};
  struct ArrowSchema* expect_schema{nullptr};
  std::tie(expect_schema, expect_array) =
      ArrowArrayBuilder().setRowNum(2).addUTF8Column("col_1", data, offset).build();

  assertQuery(
      "SELECT col_1 FROM test where col_1 LIKE '[aa]%'", expect_array, expect_schema);

  // FIXME(jikunshang): Cider only support [],%,_ pattern, !/^ is not supported yet.
  // listed on document.
  // expected_vec.clear();
  // expected_vec.push_back(CiderByteArray(7, reinterpret_cast<const
  // uint8_t*>("1111111"))); expected_vec.push_back(CiderByteArray(8,
  // reinterpret_cast<const uint8_t*>("bbbbbbbb"))); auto expected_batch_3 =
  // CiderBatchBuilder()
  //                             .setRowNum(2)
  //                             .addColumn<CiderByteArray>(
  //                                 "col_1", CREATE_SUBSTRAIT_TYPE(Varchar),
  //                                 expected_vec)
  //                             .build();
  // assertQuery("SELECT col_1 FROM test where col_1 LIKE '[!aa]%'",
  // expected_batch_3);
}

// stringop: substring

TEST_F(CiderStringNullableTestNextGen, SubstringTest) {
  // variable source string
  assertQuery("SELECT SUBSTRING(col_2, 1, 10) FROM test ");
  assertQuery("SELECT SUBSTRING(col_2, 1, 5) FROM test ");

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
}

TEST_F(CiderStringTestNextGen, NestedSubstringTest) {
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

TEST_F(CiderStringNullableTestNextGen, NestedSubstringTest) {
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

TEST_F(CiderStringRandomTestNextGen, NestedSubstringTest) {
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) = 'aaa'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) <> 'bbb'");
  assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 3) > 'aaa'");
  assertQuery("SELECT SUBSTRING(SUBSTRING(col_2, 1, 8), 1, 4) FROM test ");
}

// stringop: upper/lower

TEST_F(CiderStringTestNextGen, CaseConvertionTest) {
  // select column from table
  assertQuery("SELECT col_2, LOWER(col_2) FROM test;", "stringop_lower.json");
  assertQuery("SELECT col_2, UPPER(col_2) FROM test;", "stringop_upper.json");

  // select literal from table
  assertQuery("SELECT LOWER('aAbBcCdD12') FROM test;", "stringop_lower_literal.json");
  assertQuery("SELECT UPPER('aAbBcCdD12') FROM test;", "stringop_upper_literal.json");

  // string op on filter clause
  assertQuery("SELECT col_2 FROM test WHERE LOWER(col_2) = 'aaaaaaaaaa'",
              "stringop_lower_condition.json");
  assertQuery("SELECT col_2 FROM test WHERE UPPER(col_2) = 'AAAAAAAAAA'",
              "stringop_upper_condition.json");

  // nested stringops
  assertQuery(
      "SELECT col_2 FROM test "
      "WHERE UPPER(SUBSTRING(col_2, 1, 4)) = LOWER(SUBSTRING(col_2, 1, 4));",
      "stringop_upper_nested_1.json");
  assertQuery("SELECT col_2 FROM test WHERE UPPER(LOWER(col_2)) = col_2;",
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
  // assertQuery("SELECT LOWER('ABCDEFG');", "stringop_lower_constexpr_null.json");
  // assertQuery("SELECT UPPER('abcdefg');", "stringop_upper_constexpr_null.json");
}

TEST_F(CiderStringNullableTestNextGen, CaseConvertionTest) {
  // select column from table
  assertQuery("SELECT col_2, LOWER(col_2) FROM test;", "stringop_lower_null.json");
  assertQuery("SELECT col_2, UPPER(col_2) FROM test;", "stringop_upper_null.json");

  // select literal from table
  assertQuery("SELECT LOWER('aAbBcCdD12') FROM test;",
              "stringop_lower_literal_null.json");
  assertQuery("SELECT UPPER('aAbBcCdD12') FROM test;",
              "stringop_upper_literal_null.json");

  // string op on filter clause
  assertQuery("SELECT col_2 FROM test WHERE LOWER(col_2) = 'aaaaaaaaaa'",
              "stringop_lower_condition_null.json");
  assertQuery("SELECT col_2 FROM test WHERE UPPER(col_2) = 'AAAAAAAAAA'",
              "stringop_upper_condition_null.json");
}

// stringop: concat

TEST_F(CiderStringTestNextGen, ConcatTest) {
  // Skipped because Isthmus does not support concatenating two literals
  // assertQuery("SELECT 'foo' || 'bar' FROM test;");

  assertQuery("SELECT col_2 || 'foobar' FROM test;");
  assertQuery("SELECT 'foobar' || col_2 FROM test;");

  // assertQuery("SELECT 'foo' || 'bar' || col_2 FROM test;");
  assertQuery("SELECT 'foo' || col_2 || 'bar' FROM test;");
  assertQuery("SELECT col_2 || 'foo' || 'bar' FROM test;");

  assertQuery("SELECT SUBSTRING(col_2, 1, 3) || 'yo' FROM test;");
  assertQuery("SELECT col_2 FROM test WHERE UPPER('yo' || col_2) <> col_2;",
              "stringop_concat_filter.json");

  // nextgen also supports concatenating two variable columns
  assertQuery("SELECT col_2 || col_2 FROM test;");
  assertQuery("SELECT col_2 FROM test WHERE col_2 || col_2 <> col_2;");
}

TEST_F(CiderStringNullableTestNextGen, ConcatTest) {
  // assertQuery("SELECT 'foo' || 'bar' FROM test;");

  assertQuery("SELECT col_2 || 'foobar' FROM test;");
  assertQuery("SELECT 'foobar' || col_2 FROM test;");

  // assertQuery("SELECT 'foo' || 'bar' || col_2 FROM test;");
  assertQuery("SELECT 'foo' || col_2 || 'bar' FROM test;");
  assertQuery("SELECT col_2 || 'foo' || 'bar' FROM test;");

  assertQuery("SELECT SUBSTRING(col_2, 1, 3) || 'yo' FROM test;");
  assertQuery("SELECT col_2 FROM test WHERE UPPER(col_2 || 'yo') <> col_2;",
              "stringop_concat_filter_null.json");

  // nextgen also supports concatenating two variable columns
  assertQuery("SELECT col_2 || col_2 FROM test;");
  assertQuery("SELECT col_2 FROM test WHERE col_2 || col_2 <> col_2;");
}

// stringop: char_length

TEST_F(CiderStringTestNextGen, CharLengthTest) {
  assertQuery("SELECT LENGTH(col_2) FROM test;", "stringop_charlen_project_1.json");
  assertQuery("SELECT LENGTH(col_2) FROM test WHERE SUBSTRING(col_2, 1, 5) = 'bar'",
              "stringop_charlen_project_2.json");

  assertQuery("SELECT col_2 FROM test WHERE LENGTH(col_2) <> 0;",
              "stringop_charlen_filter.json");

  assertQuery(
      "SELECT LENGTH(SUBSTRING(col_2, 1, 5)) FROM test "
      "WHERE LENGTH(col_2 || 'boo') = 13;",
      "stringop_charlen_nested.json");
}

TEST_F(CiderStringNullableTestNextGen, CharLengthTest) {
  assertQuery("SELECT LENGTH(col_2) FROM test;", "stringop_charlen_project_1_null.json");
  assertQuery("SELECT LENGTH(col_2) FROM test WHERE SUBSTRING(col_2, 1, 5) = 'bar'",
              "stringop_charlen_project_2_null.json");

  assertQuery("SELECT col_2 FROM test WHERE LENGTH(col_2) <> 0;",
              "stringop_charlen_filter_null.json");

  assertQuery(
      "SELECT LENGTH(SUBSTRING(col_2, 1, 5)) FROM test "
      "WHERE LENGTH(col_2 || 'boo') = 13;",
      "stringop_charlen_nested_null.json");
}

// stringop: trim

class CiderTrimOpTestNextGen : public CiderNextgenTestBase {
 public:
  CiderTrimOpTestNextGen() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(10) NOT NULL, col_3 VARCHAR(10)))";

    auto int_vec = std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    auto string_vec = std::vector<std::string>{"xxxxxxxxxx",
                                               "xxxxxxxxxx",
                                               "   3456789",
                                               "   3456789",
                                               "   3      ",
                                               "   3      ",
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

    std::tie(input_schema_, input_array_) =
        ArrowArrayBuilder()
            .addColumn("col_1", CREATE_SUBSTRAIT_TYPE(I32), int_vec)
            .addUTF8Column("col_2", vc_data, vc_offsets)
            .addUTF8Column("col_3", vc_data, vc_offsets, is_null)
            .build();
  }
};

TEST_F(CiderTrimOpTestNextGen, LiteralTrimTest) {
  // DuckDb syntax: TRIM(string, characters) trims <characters> from <string>
  // basic trim (defaults to trim spaces)
  assertQuery("SELECT TRIM('   3456   ') FROM test", "stringop_trim_literal_1.json");
  // trim other characters
  assertQuery("SELECT TRIM('xxx3456   ', ' x') FROM test",
              "stringop_trim_literal_2.json");
  assertQuery("SELECT LTRIM('xxx3456xxx', 'x') FROM test", "stringop_ltrim_literal.json");
  assertQuery("SELECT RTRIM('xxx3456xxx', 'x') FROM test", "stringop_rtrim_literal.json");
}

TEST_F(CiderTrimOpTestNextGen, ColumnTrimTest) {
  assertQuery("SELECT TRIM(col_2), TRIM(col_3) FROM test", "stringop_trim_1.json");
  assertQuery("SELECT TRIM(col_2, ' x'), TRIM(col_3, ' x') FROM test",
              "stringop_trim_2.json");

  assertQuery("SELECT LTRIM(col_2), LTRIM(col_3) FROM test", "stringop_ltrim_1.json");
  assertQuery("SELECT LTRIM(col_2, ' x'), LTRIM(col_3, ' x') FROM test",
              "stringop_ltrim_2.json");

  assertQuery("SELECT RTRIM(col_2), RTRIM(col_3) FROM test", "stringop_rtrim_1.json");
  assertQuery("SELECT RTRIM(col_2, ' x'), RTRIM(col_3, ' x') FROM test",
              "stringop_rtrim_2.json");
}

TEST_F(CiderTrimOpTestNextGen, NestedTrimTest) {
  assertQuery("SELECT TRIM(UPPER(col_2), ' X'), UPPER(TRIM(col_3, 'x')) FROM test",
              "stringop_trim_nested_1.json");
  assertQuery(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR TRIM(col_3) = 'xxx3456'",
      "stringop_trim_nested_2.json");

  assertQuery("SELECT LTRIM(UPPER(col_2), ' X'), UPPER(LTRIM(col_3, 'x')) FROM test",
              "stringop_ltrim_nested_1.json");
  assertQuery(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR LTRIM(col_3) = 'xxx3456'",
      "stringop_ltrim_nested_2.json");

  assertQuery("SELECT RTRIM(UPPER(col_2), ' X'), UPPER(RTRIM(col_3, 'x')) FROM test",
              "stringop_rtrim_nested_1.json");
  assertQuery(
      "SELECT col_2, col_3 FROM test "
      "WHERE LOWER(col_2) = 'xxxxxxxxxx' OR RTRIM(col_3) = 'xxx3456'",
      "stringop_rtrim_nested_2.json");

  assertQuery("SELECT col_3 FROM test WHERE TRIM(TRIM(col_3, ' '), 'x') = '3456'",
              "stringop_trim_nested_3.json");
}

// stringop: split

class CiderSplitPartTestNextGen : public CiderNextgenTestBase {
 public:
  CiderSplitPartTestNextGen() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(12) NOT NULL, col_3 VARCHAR(12)))";

    auto int_vec = std::vector<int32_t>{0, 1, 2, 3, 4, 5};
    auto string_vec = std::vector<std::string>{
        "foobar,boo", "foobar,boo", "foo,bar,boo", "foo,bar,boo", ",", ","};
    auto is_null = std::vector<bool>{false, true, false, true, false, true};
    auto [data, offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);

    std::tie(input_schema_, input_array_) =
        ArrowArrayBuilder()
            .addColumn("col_1", CREATE_SUBSTRAIT_TYPE(I32), int_vec)
            .addUTF8Column("col_2", data, offsets)
            .addUTF8Column("col_3", data, offsets, is_null)
            .build();
  }
};

TEST_F(CiderSplitPartTestNextGen, SplitAndIndexingTest) {
  // note that duckdb array indexing is one-based, so [2] references the second element
  assertQuery(
      "SELECT STRING_SPLIT(col_2, ',')[2], STRING_SPLIT(col_3, ',')[2] FROM test;",
      "stringop_split_index_indexing.json");
  assertQuery(
      "SELECT STRING_SPLIT(col_2, ',')[-1], STRING_SPLIT(col_3, ',')[-1] FROM test;",
      "stringop_split_index_indexing_reversed.json");

  // filter
  assertQuery("SELECT col_2 FROM test WHERE STRING_SPLIT(col_2, ',')[1] = 'foo';",
              "stringop_split_index_filter.json");
  assertQuery("SELECT col_3 FROM test WHERE STRING_SPLIT(col_3, ',')[1] = 'foo';",
              "stringop_split_index_filter_null.json");

  // out-of-range
  // duckdb returns null ("null") but cider returns empty string ("")
  // assertQuery(
  //     "SELECT STRING_SPLIT(col_2, ',')[5], STRING_SPLIT(col_3, ',')[5] FROM test",
  //     "stringop_split_index_oob.json");

  // multi-char
  assertQuery(
      "SELECT STRING_SPLIT(col_2, 'oo')[1], STRING_SPLIT(col_3, 'oo')[1] FROM test;",
      "stringop_split_index_multi.json");

  // not found
  assertQuery(
      "SELECT STRING_SPLIT(col_2, 'z')[1], STRING_SPLIT(col_3, 'z')[1] FROM test;",
      "stringop_split_index_not_found.json");
}

TEST_F(CiderSplitPartTestNextGen, SplitWithLimitTest) {
  // test for prestodb extension split(input, delimiter, limit)
  auto is_null = std::vector<bool>{false, true, false, true, false, true};
  {
    // split(input, delimiter, 1)[1]
    auto string_vec = std::vector<std::string>{
        "foobar,boo", "foobar,boo", "foo,bar,boo", "foo,bar,boo", ",", ","};
    auto [data, offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", data, offsets)
            .addUTF8Column("col_3", data, offsets, is_null)
            .build();
    assertQuery("stringop_split_index_limit_1.json", expect_array, expect_schema);
  }
  {
    // split(input, delimiter, 2)[2]
    auto string_vec =
        std::vector<std::string>{"boo", "boo", "bar,boo", "bar,boo", "", ""};
    auto [data, offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", data, offsets)
            .addUTF8Column("col_3", data, offsets, is_null)
            .build();
    assertQuery("stringop_split_index_limit_2.json", expect_array, expect_schema);
  }
}

TEST_F(CiderSplitPartTestNextGen, SplitPartTest) {
  // test for prestodb extension split_part(input, delimiter, part)
  // the underlying codegen and runtime function are the same as split-with-index
  // so a basic test for verifying runnability should suffice for now
  auto is_null = std::vector<bool>{false, true, false, true, false, true};
  {
    // split_part(input, 'bar', 1)
    auto string_vec = std::vector<std::string>{"foo", "foo", "foo,", "foo,", ",", ","};
    auto [data, offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", data, offsets)
            .addUTF8Column("col_3", data, offsets, is_null)
            .build();
    assertQuery("stringop_split_part.json", expect_array, expect_schema);
  }
}

// stringop: regular expressions

class CiderRegexpTestNextGen : public CiderNextgenTestBase {
 public:
  CiderRegexpTestNextGen() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER NOT NULL, col_2 VARCHAR(15) NOT NULL, col_3 VARCHAR(15)))";

    auto int_vec = std::vector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    auto string_vec = std::vector<std::string>{"hello",
                                               "hello",
                                               "helloworldhello",
                                               "helloworldhello",
                                               "pqrsttsrqp",
                                               "pqrsttsrqp",
                                               "112@mail123.com",
                                               "112@mail123.com",
                                               "123qwerty123",
                                               "123qwerty123",
                                               "",
                                               ""};
    auto is_null = std::vector<bool>{
        false, true, false, true, false, true, false, true, false, true, false, true};
    auto [vc_data, vc_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(string_vec);

    std::tie(input_schema_, input_array_) =
        ArrowArrayBuilder()
            .addColumn("col_1", CREATE_SUBSTRAIT_TYPE(I32), int_vec)
            .addUTF8Column("col_2", vc_data, vc_offsets)
            .addUTF8Column("col_3", vc_data, vc_offsets, is_null)
            .build();
  }
};

TEST_F(CiderRegexpTestNextGen, RegexpReplaceBasicTest) {
  // replace first
  assertQuery(
      "SELECT "
      "REGEXP_REPLACE(col_2, '[wert]', 'yo'), "
      "REGEXP_REPLACE(col_3, '[wert]', 'yo') "
      "FROM test;",
      "stringop_regexp_replace_first.json");
  // replace all
  // in duckdb, regexp_replace only supports replacing the FIRST or ALL occurrences
  // the behaviour is controlled by an optional 'g' argument
  assertQuery(
      "SELECT "
      "REGEXP_REPLACE(col_2, '[wert]', 'yo', 'g'), "
      "REGEXP_REPLACE(col_3, '[wert]', 'yo', 'g') "
      "FROM test;",
      "stringop_regexp_replace_all.json");

  const auto is_null = std::vector<bool>{
      false, true, false, true, false, true, false, true, false, true, false, true};

  {
    // replace second
    // REGEXP_REPLACE(col, '[0-9]+', '<digits>');
    auto replaced = std::vector<std::string>{"hello",
                                             "hello",
                                             "helloworldhello",
                                             "helloworldhello",
                                             "pqrsttsrqp",
                                             "pqrsttsrqp",
                                             "112@mail<digits>.com",
                                             "112@mail<digits>.com",
                                             "123qwerty<digits>",
                                             "123qwerty<digits>",
                                             "",
                                             ""};
    const auto [replaced_data, replaced_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(replaced);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", replaced_data, replaced_offsets)
            .addUTF8Column("col_3", replaced_data, replaced_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_replace_second.json", expect_array, expect_schema);
  }
  {
    // replace with starting position
    // REGEXP_REPLACE(col, '[l]{2}', <two-l>, position=10)
    auto replaced = std::vector<std::string>{"hello",
                                             "hello",
                                             "helloworldhe<two-l>o",
                                             "helloworldhe<two-l>o",
                                             "pqrsttsrqp",
                                             "pqrsttsrqp",
                                             "112@mail123.com",
                                             "112@mail123.com",
                                             "123qwerty123",
                                             "123qwerty123",
                                             "",
                                             ""};
    const auto [replaced_data, replaced_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(replaced);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", replaced_data, replaced_offsets)
            .addUTF8Column("col_3", replaced_data, replaced_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_replace_position.json", expect_array, expect_schema);
  }
  {
    GTEST_SKIP_("re2 lib do not support such semantic($n)");
    // replace with capturing groups
    // substrait specification states that the replacement can refer to capturing groups
    // the n-th capturing group can be refererenced by $n in the replacement
    // REGEXP_REPLACE(col, '(h)([a-z])', 'Ha$2', occurrence=0)
    auto replaced = std::vector<std::string>{"Haello",
                                             "Haello",
                                             "HaelloworldHaello",
                                             "HaelloworldHaello",
                                             "pqrsttsrqp",
                                             "pqrsttsrqp",
                                             "112@mail123.com",
                                             "112@mail123.com",
                                             "123qwerty123",
                                             "123qwerty123",
                                             "",
                                             ""};
    const auto [replaced_data, replaced_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(replaced);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", replaced_data, replaced_offsets)
            .addUTF8Column("col_3", replaced_data, replaced_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_replace_capture.json", expect_array, expect_schema);
  }
}

TEST_F(CiderRegexpTestNextGen, RegexpReplaceExtendedTest) {
  /// NOTE: (YBRua) substrait requires occurrence >= 0 & position > 0
  /// but currently implementation also handled cases where occurence < 0 or position < 0
  /// these cases are also tested here for completeness
  /// but note that, strictly speaking, these substrait plans are invalid
  const auto is_null = std::vector<bool>{
      false, true, false, true, false, true, false, true, false, true, false, true};
  {
    // negative occurrence: replace the last second occurrence
    // REGEXP_REPLACE(col, [0-9]+, <digits>, occurrence=-2);
    auto replaced = std::vector<std::string>{"hello",
                                             "hello",
                                             "helloworldhello",
                                             "helloworldhello",
                                             "pqrsttsrqp",
                                             "pqrsttsrqp",
                                             "<digits>@mail123.com",
                                             "<digits>@mail123.com",
                                             "<digits>qwerty123",
                                             "<digits>qwerty123",
                                             "",
                                             ""};
    const auto [replaced_data, replaced_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(replaced);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", replaced_data, replaced_offsets)
            .addUTF8Column("col_3", replaced_data, replaced_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_replace_neg_occ.json", expect_array, expect_schema);
  }
  {
    // negative position: start search from the last 5 characters
    // REGEXP_REPLACE(col, [0-9]+, <digits>, position=-5);
    auto replaced = std::vector<std::string>{"he<two-l>o",
                                             "he<two-l>o",
                                             "helloworldhe<two-l>o",
                                             "helloworldhe<two-l>o",
                                             "pqrsttsrqp",
                                             "pqrsttsrqp",
                                             "112@mail123.com",
                                             "112@mail123.com",
                                             "123qwerty123",
                                             "123qwerty123",
                                             "",
                                             ""};
    const auto [replaced_data, replaced_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(replaced);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", replaced_data, replaced_offsets)
            .addUTF8Column("col_3", replaced_data, replaced_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_replace_neg_pos.json", expect_array, expect_schema);
  }
}

TEST_F(CiderRegexpTestNextGen, RegexpSubstrTest) {
  const auto is_null = std::vector<bool>{
      false, true, false, true, false, true, false, true, false, true, false, true};
  {
    // extract first match
    // SELECT REGEXP_SUBSTR(col, '[0-9]+', occurrence=1) FROM test;
    auto substr = std::vector<std::string>{
        "", "", "", "", "", "", "112", "112", "123", "123", "", ""};
    const auto [substr_data, susbtr_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(substr);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", substr_data, susbtr_offsets)
            .addUTF8Column("col_3", substr_data, susbtr_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_substr_first.json", expect_array, expect_schema);
  }
  {
    // extract last match
    // SELECT REGEXP_SUBSTR(col, '[0-9]+', occurrence=-1) FROM test;
    auto substr = std::vector<std::string>{
        "", "", "", "", "", "", "123", "123", "123", "123", "", ""};
    const auto [substr_data, susbtr_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(substr);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", substr_data, susbtr_offsets)
            .addUTF8Column("col_3", substr_data, susbtr_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_substr_last.json", expect_array, expect_schema);
  }
  {
    // extract first match starting from pos 5
    // SELECT REGEXP_SUBSTR(col, '[a-z]{2}', position=5, occurrence=1) FROM test;
    auto substr = std::vector<std::string>{
        "", "", "ow", "ow", "tt", "tt", "ma", "ma", "we", "we", "", ""};
    const auto [substr_data, susbtr_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(substr);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", substr_data, susbtr_offsets)
            .addUTF8Column("col_3", substr_data, susbtr_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_substr_pos.json", expect_array, expect_schema);
  }
}

TEST_F(CiderRegexpTestNextGen, RegexpExtractTest) {
  const auto is_null = std::vector<bool>{
      false, true, false, true, false, true, false, true, false, true, false, true};
  {
    // extract second group of first match
    // SELECT REGEXP_EXTRACT(col, '([0-9]*)([a-z]+)', group=2) FROM test;
    auto substr = std::vector<std::string>{"hello",
                                           "hello",
                                           "helloworldhello",
                                           "helloworldhello",
                                           "pqrsttsrqp",
                                           "pqrsttsrqp",
                                           "mail",
                                           "mail",
                                           "qwerty",
                                           "qwerty",
                                           "",
                                           ""};
    const auto [substr_data, susbtr_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(substr);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", substr_data, susbtr_offsets)
            .addUTF8Column("col_3", substr_data, susbtr_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_extract_group.json", expect_array, expect_schema);
  }
  {
    // extract entire first match
    // SELECT REGEXP_EXTRACT(col, '([0-9]*)([a-z]+)', group=0) FROM test;
    auto substr = std::vector<std::string>{"hello",
                                           "hello",
                                           "helloworldhello",
                                           "helloworldhello",
                                           "pqrsttsrqp",
                                           "pqrsttsrqp",
                                           "mail",
                                           "mail",
                                           "123qwerty",
                                           "123qwerty",
                                           "",
                                           ""};
    const auto [substr_data, susbtr_offsets] =
        ArrowBuilderUtils::createDataAndOffsetFromStrVector(substr);
    struct ArrowArray* expect_array{nullptr};
    struct ArrowSchema* expect_schema{nullptr};
    std::tie(expect_schema, expect_array) =
        ArrowArrayBuilder()
            .addUTF8Column("col_2", substr_data, susbtr_offsets)
            .addUTF8Column("col_3", substr_data, susbtr_offsets, is_null)
            .build();
    assertQuery("stringop_regexp_extract_full.json", expect_array, expect_schema);
  }
}

// string to date

class CiderStringToDateTestNextGen : public CiderNextgenTestBase {
 public:
  CiderStringToDateTestNextGen() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_int INTEGER, col_str VARCHAR(10));)";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        100,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2},
        GeneratePattern::Special_Date_format_String);
  }
};

TEST_F(CiderStringToDateTestNextGen, NestedTryCastStringOpTest) {
  // TODO: (YBRua) Enable this after nextgen supports CAST string AS date
  GTEST_SKIP_("casting strings to other types (date) is not supported yet in nextgen");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) > date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) < date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) IS NOT NULL");
  assertQuery("SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > 2000");
  assertQuery(
      "SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > col_int");
}

TEST_F(CiderStringToDateTestNextGen, DateStrTest) {
  // TODO: (YBRua) Enable this after nextgen supports CAST string AS date
  GTEST_SKIP_("casting strings to other types (date) is not supported yet in nextgen");
  assertQuery(
      "select col_str from test where col_str between date '1970-01-01' and date "
      "'2077-12-31'",
      "cast_str_to_date_implictly.json");
  assertQuery("SELECT CAST(col_str AS DATE) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test",
              "functions/date/year_cast_string_to_date.json");
}

// encoded string's bin_oper support is still in progress in heavydb.
// TEST_F(CiderNullableStringTest, NestedSubstrStringOpBinOperTest) {
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) = '0000000000'");
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) IS NOT NULL");
// }

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
