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
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::test::util;
using namespace cider::exec::nextgen;

#define SQL_BETWEEN_AND_INT "SELECT c0 FROM tmp WHERE c0 between 0 and 5"
#define SQL_BETWEEN_AND_FP "SELECT c0 FROM tmp WHERE c0 between 0.0 and 5.0"
#define SQL_BETWEEN_AND_DATE \
  "SELECT c0 FROM tmp WHERE c0 between date '1970-02-01' and date '1970-03-01'"

#define GEN_BETWEEN_AND_CLASS(S_TYPE, SQL_TYPE)                                       \
  class BetweenAnd##S_TYPE##Test : public CiderNextgenTestBase {                      \
   public:                                                                            \
    BetweenAnd##S_TYPE##Test() {                                                      \
      table_name_ = "tmp";                                                            \
      create_ddl_ = "CREATE TABLE tmp(c0 " #SQL_TYPE " NOT NULL);";                   \
      QueryArrowDataGenerator::generateBatchByTypes(                                  \
          input_schema_, input_array_, 100, {"c0"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}); \
    }                                                                                 \
  };

GEN_BETWEEN_AND_CLASS(I8, TINYINT)
GEN_BETWEEN_AND_CLASS(I16, SMALLINT)
GEN_BETWEEN_AND_CLASS(I32, INTEGER)
GEN_BETWEEN_AND_CLASS(I64, BIGINT)
GEN_BETWEEN_AND_CLASS(Fp32, FLOAT)
GEN_BETWEEN_AND_CLASS(Fp64, DOUBLE)
GEN_BETWEEN_AND_CLASS(Date, DATE)

#define GEN_BETWEEN_AND_NULL_CLASS(S_TYPE, SQL_TYPE)                                 \
  class BetweenAnd##S_TYPE##NullTest : public CiderNextgenTestBase {                 \
   public:                                                                           \
    BetweenAnd##S_TYPE##NullTest() {                                                 \
      table_name_ = "tmp";                                                           \
      create_ddl_ = "CREATE TABLE tmp(c0 " #SQL_TYPE ");";                           \
      QueryArrowDataGenerator::generateBatchByTypes(input_schema_,                   \
                                                    input_array_,                    \
                                                    100,                             \
                                                    {"c0"},                          \
                                                    {CREATE_SUBSTRAIT_TYPE(S_TYPE)}, \
                                                    {2});                            \
    }                                                                                \
  };

GEN_BETWEEN_AND_NULL_CLASS(I8, TINYINT)
GEN_BETWEEN_AND_NULL_CLASS(I16, SMALLINT)
GEN_BETWEEN_AND_NULL_CLASS(I32, INTEGER)
GEN_BETWEEN_AND_NULL_CLASS(I64, BIGINT)
GEN_BETWEEN_AND_NULL_CLASS(Fp32, FLOAT)
GEN_BETWEEN_AND_NULL_CLASS(Fp64, DOUBLE)
GEN_BETWEEN_AND_NULL_CLASS(Date, DATE)

TEST_F(BetweenAndI8Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i8_velox.json");
}

TEST_F(BetweenAndI8NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i8_velox.json");
}

TEST_F(BetweenAndI16Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i16_velox.json");
}

TEST_F(BetweenAndI16NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i16_velox.json");
}

TEST_F(BetweenAndI32Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i32_velox.json");
}

TEST_F(BetweenAndI32NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i32_velox.json");
}

TEST_F(BetweenAndI64Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i64_velox.json");
}

TEST_F(BetweenAndI64NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_INT, "between_and_i64_velox.json");
}

TEST_F(BetweenAndFp32Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_FP, "between_and_fp32_velox.json");
}

TEST_F(BetweenAndFp32NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_FP, "between_and_fp32_velox.json");
}

TEST_F(BetweenAndFp64Test, NotNullTest) {
  assertQuery(SQL_BETWEEN_AND_FP, "between_and_fp64_velox.json");
}

TEST_F(BetweenAndFp64NullTest, NullTest) {
  assertQuery(SQL_BETWEEN_AND_FP, "between_and_fp64_velox.json");
}

TEST_F(BetweenAndDateNullTest, DateNullTest) {
  assertQuery(SQL_BETWEEN_AND_DATE, "between_and_date_velox.json");
}

TEST_F(BetweenAndDateTest, DateNotNullTest) {
  assertQuery(SQL_BETWEEN_AND_DATE, "between_and_date_velox.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  generator::registerExtensionFunctions();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
