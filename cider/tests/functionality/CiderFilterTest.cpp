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
#include "ArrowArrayBuilder.h"
#include "QueryArrowDataGenerator.h"

#include "tests/utils/CiderTestBase.h"

// Extends CiderTestBase and create a (99 rows, 4 types columns) table for filter test.
class CiderFilterSequenceTestArrow : public CiderTestBase {
 public:
  CiderFilterSequenceTestArrow() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT,
        col_3 FLOAT, col_4 DOUBLE);)";

    QueryArrowDataGenerator::generateBatchByTypes(schema,
                                                  array,
                                                  99,
                                                  {"col_1", "col_2", "col_3", "col_4"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64)});

    input_ = {std::make_shared<CiderBatch>(
        *(new CiderBatch(schema, array, std::make_shared<CiderDefaultAllocator>())))};
  }
};

class CiderFilterRandomTestBase : public CiderTestBase {
 public:
  CiderFilterRandomTestBase() {
    table_name_ = "test";
    // TODO(yizhong): Enable this after string is supported in arrow.
    // create_ddl_ =
    //     R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE,
    //        col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE, col_9 VARCHAR(10),
    //        col_10 VARCHAR(10));)";
    // QueryArrowDataGenerator::generateBatchByTypes(schema,
    //                                               array,
    //                                               999,
    //                                               {"col_1",
    //                                                "col_2",
    //                                                "col_3",
    //                                                "col_4",
    //                                                "col_5",
    //                                                "col_6",
    //                                                "col_7",
    //                                                "col_8",
    //                                                "col_9",
    //                                                "col_10"},
    //                                               {CREATE_SUBSTRAIT_TYPE(I32),
    //                                                CREATE_SUBSTRAIT_TYPE(I64),
    //                                                CREATE_SUBSTRAIT_TYPE(Fp32),
    //                                                CREATE_SUBSTRAIT_TYPE(Fp64),
    //                                                CREATE_SUBSTRAIT_TYPE(I32),
    //                                                CREATE_SUBSTRAIT_TYPE(I64),
    //                                                CREATE_SUBSTRAIT_TYPE(Fp32),
    //                                                CREATE_SUBSTRAIT_TYPE(Fp64),
    //                                                CREATE_SUBSTRAIT_TYPE(Varchar),
    //                                                CREATE_SUBSTRAIT_TYPE(Varchar)},
    //                                               {2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
    //                                               GeneratePattern::Random,
    //                                               1,
    //                                               100);

    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE,
           col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE);)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema,
        array,
        999,
        {"col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64)},
        {2, 2, 2, 2, 2, 2, 2, 2},
        GeneratePattern::Random,
        1,
        100);

    input_ = {std::make_shared<CiderBatch>(
        *(new CiderBatch(schema, array, std::make_shared<CiderDefaultAllocator>())))};
  }
};

class CiderProjectAllTestBase : public CiderTestBase {
 public:
  CiderProjectAllTestBase() {
    table_name_ = "test";
    // TODO(yizhong): Enable this after date and bool is supported in arrow.
    // create_ddl_ =
    //     "CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 TINYINT, col_4 SMALLINT,
    //     " "col_5 FLOAT, col_6 DOUBLE, col_7 DATE, col_8 BOOLEAN);";
    // input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
    //     100,
    //     {"col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"},
    //     {CREATE_SUBSTRAIT_TYPE(I32),
    //      CREATE_SUBSTRAIT_TYPE(I64),
    //      CREATE_SUBSTRAIT_TYPE(I8),
    //      CREATE_SUBSTRAIT_TYPE(I16),
    //      CREATE_SUBSTRAIT_TYPE(Fp32),
    //      CREATE_SUBSTRAIT_TYPE(Fp64),
    //      CREATE_SUBSTRAIT_TYPE(Date),
    //      CREATE_SUBSTRAIT_TYPE(Bool)},
    //     {1, 2, 2, 2, 3, 3, 4, 2},
    //     GeneratePattern::Random))};

    create_ddl_ =
        R"(CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 TINYINT, col_4 SMALLINT, col_5 FLOAT, col_6 DOUBLE);)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema,
        array,
        100,
        {"col_1", "col_2", "col_3", "col_4", "col_5", "col_6"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I16),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Fp64)},
        {1, 2, 2, 2, 3, 3},
        GeneratePattern::Random);
    input_ = {std::make_shared<CiderBatch>(
        *(new CiderBatch(schema, array, std::make_shared<CiderDefaultAllocator>())))};
  }
};

TEST_F(CiderProjectAllTestBase, filterProjectAllTest) {
  assertQueryArrow("SELECT * FROM test");
  assertQueryArrow("SELECT * FROM test where TRUE");
  assertQueryArrow(
      "SELECT * FROM test where (col_3 > 0 and col_4 > 0) or (col_5 < 0 and col_6 < 0) ");

  // TODO(yizhong): Enable this after date and bool is supported in arrow.
  GTEST_SKIP_("date codegen is not ready.");
  assertQueryArrow("SELECT * FROM test where col_2 <> 0 and col_7 > '1972-02-01'");
  assertQueryArrow("SELECT *, 3 >= 2 FROM test where col_8 = true");
  assertQueryArrow(
      "SELECT * , (2*col_1) as col_8, (col_7 + interval '1' year) as col_9 FROM test "
      "where  col_7 > '1972-02-01'");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowInTest) {
  // TODO(yizhong): Enable this after string is supported in arrow.
  GTEST_SKIP_("in codegen is not ready.");
  assertQueryArrow("SELECT * FROM test WHERE col_1 in (24, 25, 26)",
                   "in_int32_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_2 in (24, 25, 26)",
                   "in_int64_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_3 in (24, 25, 26)",
                   "in_fp32_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_4 in (24, 25, 26)",
                   "in_fp64_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_3 not in (24, 25, 26)",
                   "not_in_fp32_array.json");
  // TODO: (yma1) add in (str_1, str_2, str_3)
  assertQueryArrow("SELECT * FROM test WHERE col_1 in (24, 25, 26) and col_2 > 20");
  assertQueryArrow("SELECT * FROM test WHERE col_1 in (24 * 2 + 2, (25 + 2) * 10, 26)");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowIntegerFilterTest) {
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 < 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 > 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 = 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 <= 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 >= 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 <> 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 IS NULL");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowConstantComparions) {
  assertQueryArrow("SELECT col_1 FROM test WHERE TRUE");
  assertQueryArrow("SELECT col_1 FROM test WHERE FALSE");

  assertQueryArrow("SELECT col_1 FROM test WHERE 2 = 2");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 > 2");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <> 2");

  assertQueryArrow("SELECT col_1 FROM test WHERE 2 = 3");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <= 3");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <> 3");

  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <= 3 AND 2 >= 1");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <= 3 OR 2 >= 1");

  assertQueryArrow("SELECT col_1 FROM test WHERE 2 <= 3 AND col_1 <> 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE 2 = 3 OR col_1 <> 77");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowComplexFilterExpressions) {
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 - 10 <= 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 + 10 >= 77");

  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 * 2 < 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 / 2 > 77");
  // FIXME(jikunshang): substrait-java does not support % yet, pending.
  // assertQueryArrow("SELECT col_1 FROM test WHERE col_1 % 2 = 1");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowBigintFilterTest) {
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 < 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 > 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 = 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 <= 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 >= 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 <> 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 IS NULL");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowFloatFilterTest) {
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 < 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 > 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 = 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 <= 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 >= 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 <> 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 IS NULL");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowDoubleFilterTest) {
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 < 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 > 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 = 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 <= 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 >= 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 <> 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 IS NULL");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 IS NOT NULL");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowMultiFilterWithOrTest) {
  assertQueryIgnoreOrder("SELECT col_1 FROM test WHERE col_1 > 50 or col_1 < 5");
  assertQueryIgnoreOrder("SELECT col_1 FROM test WHERE col_1 IS NULL or col_1 < 5");
  assertQueryIgnoreOrder("SELECT col_2 FROM test WHERE col_2 > 50 or col_2 < 5");
  assertQueryIgnoreOrder("SELECT col_2 FROM test WHERE col_2 IS NULL or col_2 < 5");
  assertQueryIgnoreOrder("SELECT col_3 FROM test WHERE col_3 > 50 or col_3 < 5");
  assertQueryIgnoreOrder("SELECT col_3 FROM test WHERE col_3 IS NULL or col_3 < 5");
  assertQueryIgnoreOrder("SELECT col_4 FROM test WHERE col_4 > 50 or col_4 < 5");
  assertQueryIgnoreOrder("SELECT col_4 FROM test WHERE col_4 IS NULL or col_4 < 5");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowMultiFilterWithAndTest) {
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 < 50 and col_1 > 5");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_1 IS NOT NULL and col_1 > 5");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 < 50 and col_2 > 5");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_2 IS NOT NULL and col_2 > 5");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 < 50 and col_3 > 5");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_3 IS NOT NULL and col_3 > 5");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 < 50 and col_4 > 5");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 IS NOT NULL and col_4 > 5");
}

TEST_F(CiderFilterSequenceTestArrow, ArrowMultiColEqualTest) {
  assertQueryArrow("SELECT col_1, col_2 FROM test WHERE col_1 = col_2");
  assertQueryArrow("SELECT col_2, col_3 FROM test WHERE col_2 = col_3");
  assertQueryArrow("SELECT col_2, col_4 FROM test WHERE col_2 = col_4");
  assertQueryArrow("SELECT col_3, col_4 FROM test WHERE col_3 = col_4");
}

TEST_F(CiderFilterRandomTestBase, multiColRandomTest) {
  // TODO ?
  GTEST_SKIP_("It seems not support column gt or lt column");
  assertQueryArrow("SELECT col_1, col_5 FROM test WHERE col_1 < col_5");
  assertQueryArrow("SELECT col_2, col_6 FROM test WHERE col_2 < col_6");
  assertQueryArrow("SELECT col_3, col_7 FROM test WHERE col_3 <= col_7");
  assertQueryArrow("SELECT col_4, col_8 FROM test WHERE col_4 <= col_8");
  assertQueryArrow("SELECT col_1, col_5 FROM test WHERE col_1 <> col_5");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_2 <= col_3 and col_2 >= "
      "col_1");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3 FROM test WHERE col_2 >= col_3 or col_2 <= col_1", "");
  assertQueryArrow("SELECT col_1, col_5 FROM test WHERE col_1 < col_5 AND col_5 > 0");
}

TEST_F(CiderFilterRandomTestBase, complexFilter) {
  assertQueryIgnoreOrder(
      "SELECT * FROM test WHERE (col_1 > 0 AND col_2 < 0) OR (col_1 < 0 AND col_2 > 0)");
}

// isthmus will convert to lt and gt.
TEST_F(CiderFilterRandomTestBase, BetweenAnd) {
  assertQueryIgnoreOrder("SELECT * FROM test WHERE col_1 between 0 AND 1000 ");
}

TEST_F(CiderFilterRandomTestBase, inTest) {
  // TODO(yizhong): Enable this after in is supported in arrow.
  GTEST_SKIP_("in codegen is not ready.");
  // select these columns instead of *, due to schema is not aligned.
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_1 in (24, 25, 26)",
      "in_int32_array.json");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_2 in (24, 25, 26)",
      "in_int64_array.json");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_3 in (24, 25, 26)",
      "in_fp32_array.json");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_4 in (24, 25, 26)",
      "in_fp64_array.json");
  assertQueryArrow(
      "SELECT col_1, col_2, col_3, col_4 FROM test WHERE col_3 not in (24, 25, 26)",
      "not_in_fp32_array.json");
  // TODO: add in (str_1, str_2, str_3)
}

TEST_F(CiderFilterSequenceTestArrow, integerNullFilterTest) {
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 < 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 > 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 <= 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 >= 77");
  assertQueryArrow("SELECT col_1 FROM test WHERE col_1 IS NOT NULL AND col_1 < 77");
  assertQueryArrow("SELECT col_2 FROM test WHERE col_2 IS NOT NULL AND col_2 > 77");
  assertQueryArrow("SELECT col_3 FROM test WHERE col_3 IS NOT NULL AND col_3 <= 77");
  assertQueryArrow("SELECT col_4 FROM test WHERE col_4 IS NOT NULL AND col_4 >= 77");
}

// TODO: Comment this test out due to unsupported decimal, string, varchar
// Update[0913, jikunshang]: decimal type is WIP. String and varchar filter have been
// supported in CiderStringTest.

TEST_F(CiderFilterSequenceTestArrow, inTest) {
  // TODO(yizhong): Enable this after in is supported in arrow.
  GTEST_SKIP_("in codegen is not ready.");
  assertQueryArrow("SELECT * FROM test WHERE col_1 in (24, 25, 26)",
                   "in_int32_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_2 in (24, 25, 26)",
                   "in_int64_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_3 in (24, 25, 26)",
                   "in_fp32_array.json");
  assertQueryArrow("SELECT * FROM test WHERE col_4 in (24, 25, 26)",
                   "in_fp64_array.json");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_1 IS NOT NULL AND col_1 in (24, 25, 26)");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_2 IS NOT NULL AND col_2 in (24, 25, 26)");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_3 IS NOT NULL AND col_3 in (24, 25, 26)");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_4 IS NOT NULL AND col_4 in (24, 25, 26)");
  assertQueryArrow("SELECT * FROM test WHERE col_3 not in (24, 25, 26)",
                   "not_in_fp32_array.json");
  // TODO: add in (str_1, str_2, str_3)
}

TEST_F(CiderFilterRandomTestBase, DistinctFromTest) {
  // TODO(yizhong): Enable this after string is supported in arrow.
  GTEST_SKIP_("string not supported in arrow");
  // IS DISTINCT FROM
  assertQueryIgnoreOrder(
      "SELECT * FROM test WHERE col_3 IS DISTINCT FROM col_7 OR col_4 IS DISTINCT FROM "
      "col_8",
      "is_distinct_from.json");

  // IS NOT DISTINCT FROM
  assertQueryIgnoreOrder(
      "SELECT * FROM test WHERE col_2 IS NOT DISTINCT FROM col_6 OR col_1 IS NOT "
      "DISTINCT FROM col_5",
      "is_not_distinct_from.json");

  // mixed case
  assertQueryIgnoreOrder(
      "SELECT * FROM test WHERE col_3 IS DISTINCT FROM col_7 OR col_1 IS NOT DISTINCT "
      "FROM col_5",
      "mixed_distinct_from.json");

  // mixed case with string
  assertQueryIgnoreOrder(
      "SELECT * FROM test WHERE col_9 IS DISTINCT FROM col_10 OR col_10 IS NOT DISTINCT "
      "FROM col_9",
      "mixed_distinct_from_string.json");
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
