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
#include "tests/utils/CiderTestBase.h"

#define GEN_PRIMITIVETYPE_BASE_TEST_CLASS(C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME)      \
  class PrimitiveType##C_TYPE_NAME##QueryTest : public CiderTestBase {                 \
   public:                                                                             \
    PrimitiveType##C_TYPE_NAME##QueryTest() {                                          \
      table_name_ = "test";                                                            \
      create_ddl_ =                                                                    \
          "CREATE TABLE test(col_a " #TYPE ", col_b " #TYPE ", col_c " #TYPE ");";     \
      input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes( \
          100,                                                                         \
          {"col_a", "col_b", "col_c"},                                                 \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)}))};                             \
    }                                                                                  \
  };

#define GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(                                         \
    C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME, BOUNDARY_VALUE)                            \
  class PrimitiveType##C_TYPE_NAME##QueryTest : public CiderTestBase {                 \
   public:                                                                             \
    PrimitiveType##C_TYPE_NAME##QueryTest() {                                          \
      table_name_ = "test";                                                            \
      create_ddl_ =                                                                    \
          "CREATE TABLE test(col_a " #TYPE ", col_b " #TYPE ", col_c " #TYPE ");";     \
      input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes( \
          100,                                                                         \
          {"col_a", "col_b", "col_c"},                                                 \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                                 \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)},                                \
          {0, 0, 0},                                                                   \
          GeneratePattern::Random,                                                     \
          BOUNDARY_VALUE,                                                              \
          BOUNDARY_VALUE))};                                                           \
    }                                                                                  \
  };

#define TEST_UNIT(TEST_CLASS, UNIT_NAME)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                        \
    assertQuery("SELECT * FROM test");                   \
    assertQuery("SELECT col_a, col_b, col_c FROM test"); \
    assertQuery("SELECT col_c, col_b, col_a FROM test"); \
    assertQuery("SELECT col_b, col_c FROM test");        \
    assertQuery("SELECT col_b, col_a FROM test");        \
    assertQuery("SELECT col_a FROM test");               \
    assertQuery("SELECT col_b FROM test");               \
    assertQuery("SELECT col_c FROM test");               \
  }

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Float, FLOAT, Fp32)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Double, DOUBLE, Fp64)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Tinyint, TINYINT, I8)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Smallint, SMALLINT, I16)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Integer, INTEGER, I32)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Bigint, BIGINT, I64)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(FloatMin, FLOAT, Fp32, FLT_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(FloatMax, FLOAT, Fp32, FLT_MAX)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(DoubleMin, DOUBLE, Fp64, DBL_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(DoubleMax, DOUBLE, Fp64, DBL_MAX)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(TinyintMin, TINYINT, I8, INT8_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(TinyintMax, TINYINT, I8, INT8_MAX)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(SmallintMin, SMALLINT, I16, INT16_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(SmallintMax, SMALLINT, I16, INT16_MAX)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(IntegerMin, INTEGER, I32, INT32_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(IntegerMax, INTEGER, I32, INT32_MAX)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(BigintMin, BIGINT, I64, INT64_MIN)

GEN_PRIMITIVETYPE_BOUNDARY_TEST_CLASS(BigintMax, BIGINT, I64, INT64_MAX)

class PrimitiveTypeBooleanQueryTest : public CiderTestBase {
 public:
  PrimitiveTypeBooleanQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BOOLEAN, col_b BOOLEAN, col_c BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(100,
                                                 {"col_a", "col_b", "col_c"},
                                                 {CREATE_SUBSTRAIT_TYPE(Bool),
                                                  CREATE_SUBSTRAIT_TYPE(Bool),
                                                  CREATE_SUBSTRAIT_TYPE(Bool)},
                                                 {0, 1, 2},
                                                 GeneratePattern::Random,
                                                 0,
                                                 1))};
  }
};

class PrimitiveTypeMixQueryTest : public CiderTestBase {
 public:
  PrimitiveTypeMixQueryTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_a FLOAT, col_b DOUBLE, col_c INTEGER, col_d BIGINT);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(100,
                                                 {"col_a", "col_b", "col_c", "col_d"},
                                                 {CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(I64)}))};
  }
};

TEST_UNIT(PrimitiveTypeBooleanQueryTest, booleanDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeFloatQueryTest, floatDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeDoubleQueryTest, doubleDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeTinyintQueryTest, tinyIntDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeSmallintQueryTest, smallIntDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeIntegerQueryTest, integerDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeBigintQueryTest, bigintDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeFloatMinQueryTest, floatMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeFloatMaxQueryTest, floatMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeDoubleMinQueryTest, doubleMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeDoubleMaxQueryTest, doubleMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeTinyintMinQueryTest, tinyIntMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeTinyintMaxQueryTest, tinyIntMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeSmallintMinQueryTest, smallIntMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeSmallintMaxQueryTest, smallIntMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeIntegerMinQueryTest, integerMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeIntegerMaxQueryTest, integerMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeBigintMinQueryTest, bigintMinDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeBigintMaxQueryTest, bigintMaxDuckDBCompareTest)

TEST_UNIT(PrimitiveTypeMixQueryTest, mixTypeDuckDBCompareTest)

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
