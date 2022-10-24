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
#include "CiderBatchChecker.h"
#include "QueryArrowDataGenerator.h"
#include "exec/plan/parser/TypeUtils.h"

#define VEC \
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }

#define TEST_GEN_SINGLE_COLUMN(C_TYPE, S_TYPE)                                      \
  {                                                                                 \
    ArrowArray* actual_array = nullptr;                                             \
    ArrowSchema* actual_schema = nullptr;                                           \
    QueryArrowDataGenerator::generateBatchByTypes(                                  \
        actual_schema, actual_array, 10, {"col"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}); \
    EXPECT_EQ(actual_array->length, 10);                                            \
    EXPECT_EQ(actual_array->n_children, 1);                                         \
    ArrowArray* expected_array = nullptr;                                           \
    ArrowSchema* expected_schema = nullptr;                                         \
    std::tie(expected_schema, expected_array) =                                     \
        ArrowArrayBuilder()                                                         \
            .addColumn<C_TYPE>("col_1", CREATE_SUBSTRAIT_TYPE(S_TYPE), VEC)         \
            .build();                                                               \
    EXPECT_EQ(expected_array->length, 10);                                          \
    EXPECT_EQ(expected_array->n_children, 1);                                       \
    /*TODO: EXPECT_TRUE(ArrowChecker::checkEq(expected_batch, actual_batch));*/     \
  }

#define TEST_GEN_SINGLE_COLUMN_RANDOM(S_TYPE)                                      \
  {                                                                                \
    ArrowArray* array = nullptr;                                                   \
    ArrowSchema* schema = nullptr;                                                 \
    QueryArrowDataGenerator::generateBatchByTypes(schema,                          \
                                                  array,                           \
                                                  10,                              \
                                                  {"col"},                         \
                                                  {CREATE_SUBSTRAIT_TYPE(S_TYPE)}, \
                                                  {},                              \
                                                  GeneratePattern::Random);        \
    EXPECT_EQ(array->length, 10);                                                  \
    EXPECT_EQ(array->n_children, 1);                                               \
  }

TEST(QueryArrowDataGeneratorTest, genSingleColumn) {
  TEST_GEN_SINGLE_COLUMN(int8_t, I8);
  TEST_GEN_SINGLE_COLUMN(int16_t, I16);
  TEST_GEN_SINGLE_COLUMN(int32_t, I32);
  TEST_GEN_SINGLE_COLUMN(int64_t, I64);
  TEST_GEN_SINGLE_COLUMN(float, Fp32);
  TEST_GEN_SINGLE_COLUMN(double, Fp64);
}

TEST(QueryArrowDataGeneratorTest, genSingleColumnRandom) {
  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;
  QueryArrowDataGenerator::generateBatchByTypes(schema,
                                                array,
                                                10,
                                                {"col"},
                                                {CREATE_SUBSTRAIT_TYPE(I32)},
                                                {},
                                                GeneratePattern::Random);

  EXPECT_EQ(array->length, 10);
  EXPECT_EQ(array->n_children, 1);
  EXPECT_NE(array->children[0], nullptr);
}

TEST(QueryArrowDataGeneratorTest, randomData) {
  TEST_GEN_SINGLE_COLUMN_RANDOM(I8);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I16);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I32);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I64);
  TEST_GEN_SINGLE_COLUMN_RANDOM(Fp32);
  TEST_GEN_SINGLE_COLUMN_RANDOM(Fp64);
}

TEST(QueryArrowDataGeneratorTest, genMultiColumns) {
  ArrowArray* actual_array = nullptr;
  ArrowSchema* actual_schema = nullptr;
  QueryArrowDataGenerator::generateBatchByTypes(
      actual_schema,
      actual_array,
      10,
      {"col_i8", "col_i16", "col_i32", "col_i64", "col_f32", "col_f64"},
      {CREATE_SUBSTRAIT_TYPE(I8),
       CREATE_SUBSTRAIT_TYPE(I16),
       CREATE_SUBSTRAIT_TYPE(I32),
       CREATE_SUBSTRAIT_TYPE(I64),
       CREATE_SUBSTRAIT_TYPE(Fp32),
       CREATE_SUBSTRAIT_TYPE(Fp64)});
  EXPECT_EQ(actual_array->length, 10);
  EXPECT_EQ(actual_array->n_children, 6);

  ArrowArray* expected_array = nullptr;
  ArrowSchema* expected_schema = nullptr;
  std::tie(expected_schema, expected_array) =
      ArrowArrayBuilder()
          .addColumn<int8_t>("", CREATE_SUBSTRAIT_TYPE(I8), VEC)
          .addColumn<int16_t>("", CREATE_SUBSTRAIT_TYPE(I16), VEC)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), VEC)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), VEC)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), VEC)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), VEC)
          .build();
  // TODO: EXPECT_TRUE(ArrowChecker::checkEq(expected_batch, actual_batch));
}

TEST(QueryArrowDataGeneratorTest, genNullColumnTest) {
  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;
  QueryArrowDataGenerator::generateBatchByTypes(schema,
                                                array,
                                                10,
                                                {"col"},
                                                {CREATE_SUBSTRAIT_TYPE(I32)},
                                                {0},
                                                GeneratePattern::Sequence);
  EXPECT_EQ(*(uint8_t*)(array->children[0]->buffers[0]), 0xFF);
  EXPECT_EQ(*(uint8_t*)(array->children[0]->buffers[0] + 1), 0xFF);
  QueryArrowDataGenerator::generateBatchByTypes(schema,
                                                array,
                                                10,
                                                {"col"},
                                                {CREATE_SUBSTRAIT_TYPE(I32)},
                                                {1},
                                                GeneratePattern::Sequence);
  EXPECT_EQ(*(uint8_t*)(array->children[0]->buffers[0]), 0x00);
  EXPECT_EQ(*(uint8_t*)(array->children[0]->buffers[0] + 1), 0xFC);
}

// TODO: add STRING DATA TIMESTAMP tests

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
