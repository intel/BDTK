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

#define GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(C_TYPE_NAME, SUBSTRAIT_TYPE_NAME)             \
  class Cast##C_TYPE_NAME##TypeTestForArrow : public CiderTestBase {                     \
   public:                                                                               \
    Cast##C_TYPE_NAME##TypeTestForArrow() {                                              \
      table_name_ = "test";                                                              \
      create_ddl_ = "CREATE TABLE test(col_a " #C_TYPE_NAME ", col_b " #C_TYPE_NAME ")"; \
      QueryArrowDataGenerator::generateBatchByTypes(                                     \
          schema_,                                                                       \
          array_,                                                                        \
          20,                                                                            \
          {"col_a", "col_b"},                                                            \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                   \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)},                                  \
          {0, 2},                                                                        \
          GeneratePattern::Random,                                                       \
          -100,                                                                          \
          100);                                                                          \
    }                                                                                    \
  };

#define TEST_UNIT_FOR_ARROW(TEST_CLASS, UNIT_NAME)                                       \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    assertQueryArrow("SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM test"); \
    assertQueryArrow(                                                                    \
        "SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM test");            \
    assertQueryArrow("SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM test"); \
    assertQueryArrow("SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM test");   \
    assertQueryArrow("SELECT CAST(col_a as FLOAT), CAST(col_b as FLOAT) FROM test");     \
    assertQueryArrow("SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM test");   \
    assertQueryArrow(                                                                    \
        "SELECT CAST(col_a as DOUBLE)  FROM test where CAST(col_b as INTEGER) > 20 ");   \
    assertQueryArrow(                                                                    \
        "SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM test");             \
    GTEST_SKIP() << "Test skipped since groupby not ready";                              \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT CAST(col_a as INTEGER), count(col_b) FROM test GROUP BY col_a", "");     \
  }

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(Float, Fp32)

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(Double, Fp64)

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(TinyInt, I8)

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(SmallInt, I16)

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(Integer, I32)

GEN_CAST_TYPE_TEST_CLASS_FOR_ARROW(BigInt, I64)

TEST_UNIT_FOR_ARROW(CastIntegerTypeTestForArrow, integerCastTestForArrow)

TEST_UNIT_FOR_ARROW(CastTinyIntTypeTestForArrow, tinyIntCastTestForArrow)

TEST_UNIT_FOR_ARROW(CastSmallIntTypeTestForArrow, smallIntCastTestForArrow)

TEST_UNIT_FOR_ARROW(CastDoubleTypeTestForArrow, doubleCastTestForArrow)

TEST_UNIT_FOR_ARROW(CastFloatTypeTestForArrow, floatCastTestForArrow)

TEST_UNIT_FOR_ARROW(CastBigIntTypeTestForArrow, bigIntCastTestForArrow)

class CastTypeQueryForArrowTest : public CiderTestBase {
 public:
  CastTypeQueryForArrowTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_tinyint TINYINT, col_int INTEGER, col_bigint BIGINT,"
        "col_bool BOOLEAN, col_date DATE, col_float FLOAT,"
        "col_double DOUBLE);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  20,
                                                  {"col_tinyint",
                                                   "col_int",
                                                   "col_bigint",
                                                   "col_bool",
                                                   "col_date",
                                                   "col_float",
                                                   "col_double"},
                                                  {CREATE_SUBSTRAIT_TYPE(I8),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(Bool),
                                                   CREATE_SUBSTRAIT_TYPE(Date),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64)});
  }
};

TEST_F(CastTypeQueryForArrowTest, castToStringTest) {
  assertQueryArrow("SELECT CAST(col_int as VARCHAR) FROM test");
  assertQueryArrow("SELECT CAST(col_date as VARCHAR) FROM test");
  assertQueryArrow("SELECT CAST(col_float as VARCHAR) FROM test");
  assertQueryArrow("SELECT CAST(col_double as VARCHAR) FROM test");
  assertQueryArrow("SELECT SUBSTRING(CAST(col_date as VARCHAR(10)), 1, 4) FROM test");
  GTEST_SKIP() << "Test skipped since case when not ready";
  assertQueryArrow("SELECT CAST(col_bool as VARCHAR) FROM test");
  assertQueryArrow("SELECT CAST(col_bool as TINYINT) FROM test");
  assertQueryArrow("SELECT CAST(col_bool as INTEGER) FROM test");
}

TEST_F(CastTypeQueryForArrowTest, castFromStringTest) {
  assertQueryArrow("SELECT CAST(CAST(col_int as VARCHAR) as INTEGER) FROM test");
  assertQueryArrow("SELECT CAST(CAST(col_tinyint as VARCHAR) as TINYINT) FROM test");
  assertQueryArrow("SELECT CAST(CAST(col_bigint as VARCHAR) as BIGINT) FROM test");
  assertQueryArrow("SELECT CAST(CAST(col_float as VARCHAR) as FLOAT) FROM test");
  assertQueryArrow("SELECT CAST(CAST(col_double as VARCHAR) as DOUBLE) FROM test");
  assertQueryArrow("SELECT CAST(CAST(col_date as VARCHAR) as DATE) FROM test");
}

TEST_F(CastTypeQueryForArrowTest, castOverflowCheckTest) {
  EXPECT_TRUE(
      executeIncorrectQueryArrow("SELECT CAST(col_int + 500  as TINYINT)  FROM test"));
  EXPECT_TRUE(
      executeIncorrectQueryArrow("SELECT CAST(col_float + 500 as TINYINT) FROM test"));
  EXPECT_TRUE(executeIncorrectQueryArrow(
      "SELECT CAST(col_bigint - 5000000000 as INTEGER) FROM test"));
}

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
    LOG(ERROR) << e.what();
  }
  return err;
}
