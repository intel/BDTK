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
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/CiderTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"

#define GEN_PRIMITIVETYPE_BASE_TEST_CLASS(C_TYPE_NAME, TYPE, SUBSTRAIT_TYPE_NAME)  \
  class PrimitiveType##C_TYPE_NAME##Test : public CiderTestBase {                  \
   public:                                                                         \
    PrimitiveType##C_TYPE_NAME##Test() {                                           \
      table_name_ = "test";                                                        \
      create_ddl_ =                                                                \
          "CREATE TABLE test(col_a " #TYPE " NOT NULL, col_b " #TYPE ", col_c " #TYPE ");"; \
      QueryArrowDataGenerator::generateBatchByTypes(                               \
          schema_,                                                                 \
          array_,                                                                  \
          10,                                                                      \
          {"col_a", "col_b", "col_c"},                                             \
          {CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                             \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME),                             \
           CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE_NAME)},                            \
          {0, 1, 2},                                                               \
          GeneratePattern::Random,                                                 \
          0,                                                                       \
          0);                                                                      \
    }                                                                              \
  };

#define TEST_UNIT(TEST_CLASS, UNIT_NAME)                                              \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                     \
    assertQueryArrow("SELECT * FROM test");                                           \
    assertQueryArrow("SELECT col_a, col_b, col_c FROM test");                         \
    assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_b IS NOT NULL"); \
    assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_b IS NULL");     \
  }

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Boolean, BOOLEAN, Bool)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Float, FLOAT, Fp32)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Double, DOUBLE, Fp64)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Tinyint, TINYINT, I8)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Smallint, SMALLINT, I16)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Integer, INTEGER, I32)

GEN_PRIMITIVETYPE_BASE_TEST_CLASS(Bigint, BIGINT, I64)

TEST_UNIT(PrimitiveTypeBooleanTest, booleanBaseTest)

TEST_UNIT(PrimitiveTypeFloatTest, floatBaseTest)

TEST_UNIT(PrimitiveTypeDoubleTest, doubleBaseTest)

TEST_UNIT(PrimitiveTypeTinyintTest, tinyIntBaseTest)

TEST_UNIT(PrimitiveTypeSmallintTest, smallIntBaseTest)

TEST_UNIT(PrimitiveTypeIntegerTest, integerBaseTest)

TEST_UNIT(PrimitiveTypeBigintTest, bigintBaseTest)

class PrimitiveTypeIntegerMaxTest : public CiderTestBase {
 public:
  PrimitiveTypeIntegerMaxTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a INTEGER NOT NULL, col_b INTEGER, col_c INTEGER);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32)},
                                                  {0, 1, 2},
                                                  GeneratePattern::Random,
                                                  INT32_MAX,
                                                  INT32_MAX);
  }
};

class PrimitiveTypeIntegerMINTest : public CiderTestBase {
 public:
  PrimitiveTypeIntegerMINTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a INTEGER NOT NULL, col_b INTEGER, col_c INTEGER);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32),
                                                   CREATE_SUBSTRAIT_TYPE(I32)},
                                                  {0, 1, 2},
                                                  GeneratePattern::Random,
                                                  INT32_MIN,
                                                  INT32_MIN);
  }
};

class PrimitiveTypeMixedTypeNotNullTest : public CiderTestBase {
 public:
  PrimitiveTypeMixedTypeNotNullTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_a BOOLEAN NOT NULL, col_b FLOAT NOT NULL, col_c "
        "TINYINT NOT NULL)";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_TYPE(Bool),
                                                   CREATE_SUBSTRAIT_TYPE(Fp32),
                                                   CREATE_SUBSTRAIT_TYPE(I8)},
                                                  {0, 0, 0},
                                                  GeneratePattern::Random,
                                                  0,
                                                  0);
  }
};

class PrimitiveTypeMixedTypeNullTest : public CiderTestBase {
 public:
  PrimitiveTypeMixedTypeNullTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BOOLEAN, col_b DOUBLE, col_c INTEGER)";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_TYPE(Bool),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(I32)},
                                                  {1, 1, 1},
                                                  GeneratePattern::Random,
                                                  0,
                                                  0);
  }
};

class PrimitiveTypeMixedTypeHalfNullTest : public CiderTestBase {
 public:
  PrimitiveTypeMixedTypeHalfNullTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BOOLEAN, col_b DOUBLE, col_c INTEGER)";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  10,
                                                  {"col_a", "col_b", "col_c"},
                                                  {CREATE_SUBSTRAIT_TYPE(Bool),
                                                   CREATE_SUBSTRAIT_TYPE(Fp64),
                                                   CREATE_SUBSTRAIT_TYPE(I32)},
                                                  {2, 2, 2},
                                                  GeneratePattern::Random,
                                                  0,
                                                  0);
  }
};

TEST_UNIT(PrimitiveTypeIntegerMaxTest, integerMaxTest)

TEST_UNIT(PrimitiveTypeIntegerMINTest, integerMinTest)

TEST_UNIT(PrimitiveTypeMixedTypeNotNullTest, mixedTypeNotNULLTest)

TEST_UNIT(PrimitiveTypeMixedTypeNullTest, mixedTypeNULLTest)

TEST_UNIT(PrimitiveTypeMixedTypeHalfNullTest, mixedTypeHalfNULLTest)

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
