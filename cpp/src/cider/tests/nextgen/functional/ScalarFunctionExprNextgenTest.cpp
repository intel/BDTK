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

#define SQL_BETWEEN_AND_INT "SELECT c0 FROM tmp WHERE c0 between 0 and 5"
#define SQL_BETWEEN_AND_FP "SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0"
#define SQL_BETWEEN_AND_DATE \
  "SELECT c0 FROM tmp WHERE c0 between date '1970-02-01' and date '1970-03-01'"

#define GEN_BETWEEN_AND_ARROW_CLASS(S_TYPE, SQL_TYPE)                     \
  class BetweenAndArrow##S_TYPE##Test : public CiderTestBase {            \
   public:                                                                \
    BetweenAndArrow##S_TYPE##Test() {                                     \
      table_name_ = "tmp";                                                \
      create_ddl_ = "CREATE TABLE tmp(c0 " #SQL_TYPE " NOT NULL);";       \
      QueryArrowDataGenerator::generateBatchByTypes(                      \
          schema_, array_, 100, {"c0"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}); \
    }                                                                     \
  };

GEN_BETWEEN_AND_ARROW_CLASS(I8, TINYINT)
GEN_BETWEEN_AND_ARROW_CLASS(I16, SMALLINT)
GEN_BETWEEN_AND_ARROW_CLASS(I32, INTEGER)
GEN_BETWEEN_AND_ARROW_CLASS(I64, BIGINT)
GEN_BETWEEN_AND_ARROW_CLASS(Fp32, FLOAT)
GEN_BETWEEN_AND_ARROW_CLASS(Fp64, DOUBLE)
GEN_BETWEEN_AND_ARROW_CLASS(Date, DATE)

#define GEN_BETWEEN_AND_ARROW_NULL_CLASS(S_TYPE, SQL_TYPE)                     \
  class BetweenAndArrow##S_TYPE##NullTest : public CiderTestBase {             \
   public:                                                                     \
    BetweenAndArrow##S_TYPE##NullTest() {                                      \
      table_name_ = "tmp";                                                     \
      create_ddl_ = "CREATE TABLE tmp(c0 " #SQL_TYPE ");";                     \
      QueryArrowDataGenerator::generateBatchByTypes(                           \
          schema_, array_, 100, {"c0"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}, {2}); \
    }                                                                          \
  };

GEN_BETWEEN_AND_ARROW_NULL_CLASS(I8, TINYINT)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(I16, SMALLINT)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(I32, INTEGER)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(I64, BIGINT)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(Fp32, FLOAT)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(Fp64, DOUBLE)
GEN_BETWEEN_AND_ARROW_NULL_CLASS(Date, DATE)

TEST_F(BetweenAndArrowI8Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i8_velox.json");
}

TEST_F(BetweenAndArrowI8NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i8_velox.json");
}

TEST_F(BetweenAndArrowI16Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i16_velox.json");
}

TEST_F(BetweenAndArrowI16NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i16_velox.json");
}

TEST_F(BetweenAndArrowI32Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i32_velox.json");
}

TEST_F(BetweenAndArrowI32NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i32_velox.json");
}

TEST_F(BetweenAndArrowI64Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i64_velox.json");
}

TEST_F(BetweenAndArrowI64NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_INT, "between_and_i64_velox.json");
}

TEST_F(BetweenAndArrowFp32Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_FP, "between_and_fp32_velox.json");
}

TEST_F(BetweenAndArrowFp32NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_FP, "between_and_fp32_velox.json");
}

TEST_F(BetweenAndArrowFp64Test, NotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_FP, "between_and_fp64_velox.json");
}

TEST_F(BetweenAndArrowFp64NullTest, NullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_FP, "between_and_fp64_velox.json");
}

TEST_F(BetweenAndArrowDateNullTest, DateNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_DATE, "between_and_date_velox.json");
}

TEST_F(BetweenAndArrowDateTest, DateNotNullTest) {
  assertQueryArrow(SQL_BETWEEN_AND_DATE, "between_and_date_velox.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  generator::registerExtensionFunctions();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
