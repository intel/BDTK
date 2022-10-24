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
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
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

TEST_F(CiderStringToDateTest, NestedTryCastStringOpTest) {
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) > date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) < date '1990-01-11'");
  assertQuery("SELECT * FROM test where CAST(col_str AS DATE) IS NOT NULL");
  assertQuery("SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > 2000");
  assertQuery(
      "SELECT * FROM test where extract(year from CAST(col_str AS DATE)) > col_int");
}

// encoded string's bin_oper support is still in progress in heavydb.
// TEST_F(CiderNullableStringTest, NestedSubstrStringOpBinOperTest) {
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) = '0000000000'");
// assertQuery("SELECT * FROM test where SUBSTRING(col_2, 1, 10) IS NOT NULL");
// }

TEST_F(CiderStringToDateTest, DateStrTest) {
  assertQuery(
      "select col_str from test where col_str between date '1970-01-01' and date "
      "'2077-12-31'",
      "cast_str_to_date_implictly.json");
  assertQuery("SELECT CAST(col_str AS DATE) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test");
  assertQuery("SELECT extract(year from CAST(col_str AS DATE)) FROM test",
              "functions/date/year_cast_string_to_date.json");
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

#define BASIC_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME)                         \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                             \
    assertQuery("SELECT col_2 FROM test ");                                   \
    assertQuery("SELECT col_1, col_2 FROM test ");                            \
    assertQuery("SELECT * FROM test ");                                       \
    assertQuery("SELECT col_2 FROM test where col_2 = 'aaaa'");               \
    assertQuery("SELECT col_2 FROM test where col_2 = '0000000000'");         \
    assertQuery("SELECT col_2 FROM test where col_2 <> '0000000000'");        \
    assertQuery("SELECT col_1 FROM test where col_2 <> '1111111111'");        \
    assertQuery("SELECT col_1, col_2 FROM test where col_2 <> '2222222222'"); \
    assertQuery("SELECT * FROM test where col_2 <> 'aaaaaaaaaaa'");           \
    assertQuery("SELECT * FROM test where col_2 <> 'abcdefghijklmn'");        \
    assertQuery("SELECT col_2 FROM test where col_2 IS NOT NULL");            \
    assertQuery("SELECT col_2 FROM test where col_2 < 'uuu'");                \
  }

#define LIKE_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME)                                     \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '%1111'");                      \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '1111%'");                      \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '%1111%'");                     \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '%1234%'");                     \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '22%22'");                      \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '_33%'");                       \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '44_%'");                       \
    assertQuery(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '5555%' OR col_2 LIKE '%6666'");        \
    assertQuery(                                                                         \
        "SELECT col_2 FROM test where col_2 LIKE '7777%' AND col_2 LIKE '%8888'");       \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '%1111'", "like_wo_cast.json"); \
    assertQuery("SELECT col_2 FROM test where col_2 NOT LIKE '1111%'");                  \
    assertQuery("SELECT col_2 FROM test where col_2 NOT LIKE '44_4444444'");             \
    assertQuery(                                                                         \
        "SELECT col_2 FROM test where col_2 NOT LIKE '44_4%' and col_2 NOT LIKE "        \
        "'%111%'");                                                                      \
  }

#define ESCAPE_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME)                          \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                               \
    GTEST_SKIP_("Substrait does not support ESCAPE yet.");                      \
    assertQuery("SELECT col_2 FROM test where col_2 LIKE '%aaaa' ESCAPE '$' "); \
  }

/**
  // not supported for different type info of substr and literal
  assertQuery(
    "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) = '0000'");
  assertQuery(
      "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222',
      '3333')");
**/
#define IN_STRING_TEST_UNIT(TEST_CLASS, UNIT_NAME)                                      \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertQuery(                                                                        \
        "SELECT * FROM test WHERE col_2 IN ('0000000000', '1111111111', '2222222222')", \
        "in_string_array.json");                                                        \
    assertQuery("SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111')",  \
                "in_string_2_array_with_substr.json");                                  \
    assertQuery(                                                                        \
        "SELECT * FROM test WHERE SUBSTRING(col_2, 1, 4) IN ('0000', '1111', '2222', "  \
        "'3333')",                                                                      \
        "in_string_array_with_substr.json");                                            \
    assertQuery(                                                                        \
        "SELECT * FROM test WHERE col_1 >= 0 and SUBSTRING(col_2, 1, 4) IN "            \
        "('0000', '1111', '2222', '3333')",                                             \
        "in_string_nest_with_binop.json");                                              \
  }

BASIC_STRING_TEST_UNIT(CiderStringTest, basicStringTest)
LIKE_STRING_TEST_UNIT(CiderStringTest, likeStringTest)
ESCAPE_STRING_TEST_UNIT(CiderStringTest, escapeStringTest)
IN_STRING_TEST_UNIT(CiderStringTest, inStringTest)

BASIC_STRING_TEST_UNIT(CiderRandomStringTest, basicRandomStringTest)
LIKE_STRING_TEST_UNIT(CiderRandomStringTest, likeRandomStringTest)
ESCAPE_STRING_TEST_UNIT(CiderRandomStringTest, escapeRandomStringTest)
IN_STRING_TEST_UNIT(CiderRandomStringTest, inRandomStringTest)

BASIC_STRING_TEST_UNIT(CiderNullableStringTest, basicNullableStringTest)
LIKE_STRING_TEST_UNIT(CiderNullableStringTest, likeNullableStringTest)
ESCAPE_STRING_TEST_UNIT(CiderNullableStringTest, escapeNullableStringTest)
IN_STRING_TEST_UNIT(CiderNullableStringTest, inNullableStringTest)

TEST_F(CiderStringTest, ArrowBasicStringTest) {
  prepareArrowBatch();
  //  assertQueryArrow("SELECT col_1 FROM test ");
  //  assertQueryArrow("SELECT col_2 FROM test ");
  //  assertQueryArrow("SELECT col_1, col_2 FROM test ");

  assertQueryArrow("SELECT col_1 FROM test where col_2 = 'aaaa'");
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
