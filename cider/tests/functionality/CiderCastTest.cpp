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

#define GEN_CAST_TYPE_TEST_CLASS(C_TYPE_NAME, SUBSTRAIT_TYPE_NAME)                       \
  class Cast##C_TYPE_NAME##TypeTest : public CiderTestBase {                             \
   public:                                                                               \
    Cast##C_TYPE_NAME##TypeTest() {                                                      \
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

#define TEST_UNIT(TEST_CLASS, UNIT_NAME)                                                 \
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
    GTEST_SKIP();                                                                        \
    assertQueryArrowIgnoreOrder(                                                         \
        "SELECT CAST(col_a as INTEGER), count(col_b) FROM test GROUP BY col_a", "");     \
  }

GEN_CAST_TYPE_TEST_CLASS(Float, Fp32)

GEN_CAST_TYPE_TEST_CLASS(Double, Fp64)

GEN_CAST_TYPE_TEST_CLASS(TinyInt, I8)

GEN_CAST_TYPE_TEST_CLASS(SmallInt, I16)

GEN_CAST_TYPE_TEST_CLASS(Integer, I32)

GEN_CAST_TYPE_TEST_CLASS(BigInt, I64)

TEST_UNIT(CastTinyIntTypeTest, tinyIntCastTest)

TEST_UNIT(CastSmallIntTypeTest, smallIntCastTest)

TEST_UNIT(CastDoubleTypeTest, doubleCastTest)

TEST_UNIT(CastFloatTypeTest, floatCastTest)

TEST_UNIT(CastBigIntTypeTest, bigIntCastTest)

TEST_UNIT(CastIntegerTypeTest, integerCastTest)
// FIXME(kaidi): pass test with arrow format
class CastTypeQueryTest : public CiderTestBase {
 public:
  CastTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_1 TINYINT, col_2 INTEGER, col_3 VARCHAR(10), col_4 "
        "BOOLEAN NOT NULL, col_5 DATE);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        20,
        {"col_1", "col_2", "col_3", "col_4", "col_5"},
        {CREATE_SUBSTRAIT_TYPE(I8),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Varchar),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(Date)}))};
  }
};

TEST_F(CastTypeQueryTest, castTypeTest) {
  // cast_boolean_into_integer will convert into if/then expr, already not support
  assertQuery("SELECT CAST(col_4 as TINYINT) FROM test");
  assertQuery("SELECT CAST(col_4 as INTEGER) FROM test");
  // TODO: cast numeric type to string type now not supported.
  // assertQuery("SELECT CAST(col_2 as VARCHAR(10)) FROM test");
}

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
