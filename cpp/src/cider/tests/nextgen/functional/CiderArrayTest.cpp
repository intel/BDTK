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
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::test::util;

#define GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME)      \
  class PrimitiveTypeArray##C_TYPE_NAME##Test : public CiderNextgenTestBase {           \
   public:                                                                              \
    PrimitiveTypeArray##C_TYPE_NAME##Test() {                                           \
      table_name_ = "test";                                                             \
      duckdb_create_ddl_ = "CREATE TABLE test(col_a " #TYPE "[] NOT NULL, col_b " #TYPE \
                           "[], col_c " #TYPE "[]);";                                   \
      create_ddl_ = "CREATE TABLE test(col_a " #TYPE " ARRAY NOT NULL, col_b " #TYPE    \
                    " ARRAY, col_c " #TYPE " ARRAY);";                                  \
      QueryArrowDataGenerator::generateBatchByTypes(                                    \
          input_schema_,                                                                \
          input_array_,                                                                 \
          10,                                                                           \
          {"col_a", "col_b", "col_c"},                                                  \
          {CREATE_SUBSTRAIT_LIST_TYPE(SUBSTRAIT_TYPE_NAME),                             \
           CREATE_SUBSTRAIT_LIST_TYPE(SUBSTRAIT_TYPE_NAME),                             \
           CREATE_SUBSTRAIT_LIST_TYPE(SUBSTRAIT_TYPE_NAME)},                            \
          {0, 1, 2},                                                                    \
          GeneratePattern::Random,                                                      \
          0,                                                                            \
          0);                                                                           \
    }                                                                                   \
  };

#define TEST_UNIT_PRIMITIVE_COL(TEST_CLASS, UNIT_NAME)   \
  TEST_F(TEST_CLASS, UNIT_NAME) {                        \
    assertQuery("SELECT col_a FROM test");               \
    assertQuery("SELECT col_b FROM test");               \
    assertQuery("SELECT col_c FROM test");               \
    assertQuery("SELECT * FROM test");                   \
    assertQuery("SELECT col_a, col_b, col_c FROM test"); \
  }

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Float, FLOAT, Fp32)

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Double, DOUBLE, Fp64)

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Tinyint, TINYINT, I8)

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Smallint, SMALLINT, I16)

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Integer, INTEGER, I32)

GEN_PRIMITIVETYPE_ARRAY_TEST_CLASS(Bigint, BIGINT, I64)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArrayFloatTest, floatBaseMultiColTest)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArrayDoubleTest, doubleBaseMultiColTest)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArrayTinyintTest, tinyIntBaseMultiColTest)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArraySmallintTest, smallIntBaseMultiColTest)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArrayIntegerTest, integerBaseMultiColTest)

TEST_UNIT_PRIMITIVE_COL(PrimitiveTypeArrayBigintTest, bigintBaseMultiColTest)

class PrimitiveTypeArrayMixed1Test : public CiderNextgenTestBase {
 public:
  PrimitiveTypeArrayMixed1Test() {
    table_name_ = "test";
    duckdb_create_ddl_ =
        "CREATE TABLE test(col_a TINYINT[], col_b SMALLINT[], col_c INTEGER[]);";
    create_ddl_ =
        "CREATE TABLE test(col_a TINYINT ARRAY, col_b SMALLINT ARRAY, col_c INTEGER "
        "ARRAY);";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_LIST_TYPE(I8),
                                                   CREATE_SUBSTRAIT_LIST_TYPE(I16),
                                                   CREATE_SUBSTRAIT_LIST_TYPE(I32)},
                                                  {2, 2, 2},
                                                  GeneratePattern::Random,
                                                  0,
                                                  0);
  }
};

TEST_F(PrimitiveTypeArrayMixed1Test, ArrayMixed1Test) {
  assertQuery("SELECT * FROM test");
  assertQuery("SELECT col_a, col_b, col_c FROM test");
}

class PrimitiveTypeArrayMixed2Test : public CiderNextgenTestBase {
 public:
  PrimitiveTypeArrayMixed2Test() {
    table_name_ = "test";
    duckdb_create_ddl_ =
        "CREATE TABLE test(col_a BIGINT[], col_b FLOAT[], col_c DOUBLE[]);";
    create_ddl_ =
        "CREATE TABLE test(col_a BIGINT ARRAY, col_b FLOAT ARRAY, col_c DOUBLE "
        "ARRAY);";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_LIST_TYPE(I64),
                                                   CREATE_SUBSTRAIT_LIST_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_LIST_TYPE(Fp64)},
                                                  {2, 2, 2},
                                                  GeneratePattern::Random,
                                                  0,
                                                  0);
  }
};

TEST_F(PrimitiveTypeArrayMixed2Test, ArrayMixed2Test) {
  assertQuery("SELECT * FROM test");
  assertQuery("SELECT col_a, col_b, col_c FROM test");
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
