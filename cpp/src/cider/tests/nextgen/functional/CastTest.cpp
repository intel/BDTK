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

#include <gflags/gflags.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include "tests/utils/CiderNextgenTestBase.h"
using namespace cider::test::util;

#define GEN_CAST_TYPE_TEST_CLASS(C_TYPE_NAME, SUBSTRAIT_TYPE_NAME)                       \
  class Cast##C_TYPE_NAME##TypeTest : public CiderNextgenTestBase {                      \
   public:                                                                               \
    Cast##C_TYPE_NAME##TypeTest() {                                                      \
      table_name_ = "test";                                                              \
      create_ddl_ = "CREATE TABLE test(col_a " #C_TYPE_NAME ", col_b " #C_TYPE_NAME ")"; \
      QueryArrowDataGenerator::generateBatchByTypes(                                     \
          input_schema_,                                                                 \
          input_array_,                                                                  \
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

#define TEST_UNIT(TEST_CLASS, UNIT_NAME)                                               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                      \
    assertQuery("SELECT CAST(col_a as TINYINT), CAST(col_b as TINYINT) FROM test");    \
    assertQuery("SELECT CAST(col_a as SMALLINT), CAST(col_b as SMALLINT) FROM test");  \
    assertQuery("SELECT CAST(col_a as INTEGER), CAST(col_b as INTEGER) FROM test");    \
    assertQuery("SELECT CAST(col_a as BIGINT), CAST(col_b as BIGINT) FROM test");      \
    assertQuery("SELECT CAST(col_a as FLOAT), CAST(col_b as FLOAT) FROM test");        \
    assertQuery("SELECT CAST(col_a as DOUBLE), CAST(col_b as DOUBLE) FROM test");      \
    assertQuery(                                                                       \
        "SELECT CAST(col_a as DOUBLE)  FROM test where CAST(col_b as INTEGER) > 20 "); \
    assertQuery("SELECT CAST(col_a as INTEGER) + CAST(col_b as INTEGER) FROM test");   \
    GTEST_SKIP() << "Test skipped since groupby not ready";                            \
    assertQueryIgnoreOrder(                                                            \
        "SELECT CAST(col_a as INTEGER), count(col_b) FROM test GROUP BY col_a", "");   \
  }

GEN_CAST_TYPE_TEST_CLASS(Float, Fp32)

GEN_CAST_TYPE_TEST_CLASS(Double, Fp64)

GEN_CAST_TYPE_TEST_CLASS(TinyInt, I8)

GEN_CAST_TYPE_TEST_CLASS(SmallInt, I16)

GEN_CAST_TYPE_TEST_CLASS(Integer, I32)

GEN_CAST_TYPE_TEST_CLASS(BigInt, I64)

TEST_UNIT(CastIntegerTypeTest, integerCastTest)

TEST_UNIT(CastTinyIntTypeTest, tinyIntCastTest)

TEST_UNIT(CastSmallIntTypeTest, smallIntCastTest)

TEST_UNIT(CastDoubleTypeTest, doubleCastTest)

TEST_UNIT(CastFloatTypeTest, floatCastTest)

TEST_UNIT(CastBigIntTypeTest, bigIntCastTest)

class CastTypeQueryTest : public CiderNextgenTestBase {
 public:
  CastTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_tinyint TINYINT, col_int INTEGER, col_bigint BIGINT,"
        "col_bool BOOLEAN, col_date DATE, col_float FLOAT,"
        "col_double DOUBLE);";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
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

TEST_F(CastTypeQueryTest, castToStringTest) {
  assertQuery("SELECT CAST(col_int as VARCHAR) FROM test");
  assertQuery("SELECT CAST(col_date as VARCHAR) FROM test");
  assertQuery("SELECT CAST(col_float as VARCHAR) FROM test");
  assertQuery("SELECT CAST(col_double as VARCHAR) FROM test");
  assertQuery("SELECT SUBSTRING(CAST(col_date as VARCHAR(10)), 1, 4) FROM test");
  GTEST_SKIP() << "Test skipped since case when not ready";
  assertQuery("SELECT CAST(col_bool as VARCHAR) FROM test");
  assertQuery("SELECT CAST(col_bool as TINYINT) FROM test");
  assertQuery("SELECT CAST(col_bool as INTEGER) FROM test");
}

TEST_F(CastTypeQueryTest, castFromStringTest) {
  assertQuery("SELECT CAST(CAST(col_int as VARCHAR) as INTEGER) FROM test");
  assertQuery("SELECT CAST(CAST(col_tinyint as VARCHAR) as TINYINT) FROM test");
  assertQuery("SELECT CAST(CAST(col_bigint as VARCHAR) as BIGINT) FROM test");
  assertQuery("SELECT CAST(CAST(col_float as VARCHAR) as FLOAT) FROM test");
  assertQuery("SELECT CAST(CAST(col_double as VARCHAR) as DOUBLE) FROM test");
  assertQuery("SELECT CAST(CAST(col_date as VARCHAR) as DATE) FROM test");
}

TEST_F(CastTypeQueryTest, castOverflowCheckTest) {
  cider::exec::nextgen::context::CodegenOptions codegen_options{};
  codegen_options.needs_error_check = true;
  setCodegenOptions(codegen_options);
  EXPECT_TRUE(executeIncorrectQuery("SELECT CAST(col_int + 500  as TINYINT)  FROM test"));
  EXPECT_TRUE(executeIncorrectQuery("SELECT CAST(col_float + 500 as TINYINT) FROM test"));
  EXPECT_TRUE(
      executeIncorrectQuery("SELECT CAST(col_bigint - 5000000000 as INTEGER) FROM test"));
}

TEST_F(CastTypeQueryTest, castFromBooleanTest) {
  assertQuery("SELECT CAST(col_bool as INTEGER) FROM test", "cast_bool_to_integer.json");
  assertQuery("SELECT CAST(col_bool as TINYINT) FROM test", "cast_bool_to_tinyint.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
