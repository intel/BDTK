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
#include "CiderBatchBuilder.h"
#include "CiderBatchChecker.h"
#include "Utils.h"
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "exec/plan/parser/TypeUtils.h"

TEST(CiderBatchCheckerArrowTest, colNumCheck) {
  auto data = std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto expected = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data)
          .build());
  auto actual_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data)
          .build());
  auto actual_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected, actual_1));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_1, expected));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(expected, actual_2));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(actual_2, expected));
}

TEST(CiderBatchCheckerArrowTest, rowNumCheck) {
  auto data_1 = std::vector<int>{0, 1, 2, 3, 4};
  auto data_2 = std::vector<int>{0, 1, 2, 3, 4, 5};

  auto expected = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data_1)
          .build());
  auto actual = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(6)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), data_2)
          .build());

  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(expected, actual));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(actual, expected));
}

#define TEST_SINGLE_COLUMN_ARROW(C_TYPE, SUBSTRAIT_TYPE)                               \
  {                                                                                    \
    auto data = std::vector<C_TYPE>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};                     \
    auto nulls = std::vector<bool>{                                                    \
        false, true, false, true, false, true, false, true, false, true};              \
                                                                                       \
    auto expected##C_TYPE = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(       \
        ArrowArrayBuilder()                                                            \
            .setRowNum(10)                                                             \
            .addColumn<C_TYPE>("", CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE), data, nulls) \
            .build());                                                                 \
    auto actual##C_TYPE = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(         \
        ArrowArrayBuilder()                                                            \
            .setRowNum(10)                                                             \
            .addColumn<C_TYPE>("", CREATE_SUBSTRAIT_TYPE(SUBSTRAIT_TYPE), data, nulls) \
            .build());                                                                 \
    EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected##C_TYPE, actual##C_TYPE));    \
    EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual##C_TYPE, expected##C_TYPE));    \
  }

TEST(CiderBatchCheckerArrowTest, singleColumn) {
  TEST_SINGLE_COLUMN_ARROW(int8_t, I8);
  TEST_SINGLE_COLUMN_ARROW(int16_t, I16);
  TEST_SINGLE_COLUMN_ARROW(int32_t, I32);
  TEST_SINGLE_COLUMN_ARROW(int64_t, I64);
  TEST_SINGLE_COLUMN_ARROW(float, Fp32);
  TEST_SINGLE_COLUMN_ARROW(double, Fp64);
}

TEST(CiderBatchCheckerArrowTest, booleanTest) {
  /// TODO: (YBRua) switch to ArrowArrayBuilder after relevent PR is merged
  auto batch_vec =
      std::vector<bool>{true, false, true, false, true, false, true, false, true, false};
  auto batch_null =
      std::vector<bool>{true, true, true, true, true, false, false, false, false, false};
  // ignore order
  auto batch_vec_2 =
      std::vector<bool>{true, true, true, true, true, false, false, false, false, false};
  auto batch_null_2 =
      std::vector<bool>{true, false, true, false, true, false, true, false, true, false};

  auto batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", batch_vec, batch_null)
          .addBoolColumn<bool>("", batch_vec)
          .build());
  auto eq_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", batch_vec, batch_null)
          .addBoolColumn<bool>("", batch_vec)
          .build());
  auto neq_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", std::vector<bool>(10, true), batch_null)
          .addBoolColumn<bool>("", std::vector<bool>(10, true))
          .build());

  auto ignore_order_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addBoolColumn<bool>("", batch_vec_2, batch_null_2)
          .addBoolColumn("", batch_vec_2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(batch, eq_batch));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(batch, neq_batch));

  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(batch, ignore_order_batch));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(batch, ignore_order_batch, true));
}

TEST(CiderBatchCheckerArrowTest, rowValue) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  auto expected_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  std::vector<int> vec2{0, 2, 3, 4, 5};
  auto actual_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec2)
          .build());

  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batch));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(actual_batch, expected_batch));
}

TEST(CiderBatchCheckerArrowTest, integerTypeCheck) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{1, 2, 3, 4, 5};

  auto expected_batch1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  auto actual_batch1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int64_t>("long", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch1, actual_batch1));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_batch1, expected_batch1));

  auto expected_batch2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  auto actual_batch2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("long", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch2, actual_batch2));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_batch2, expected_batch2));
}

TEST(CiderBatchCheckerArrowTest, floatTypeCheck) {
  std::vector<double> vec1{1, 2, 3, 4, 5};
  std::vector<float> vec2{1, 2, 3, 4, 5};

  auto expected_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<double>("double", CREATE_SUBSTRAIT_TYPE(Fp64), vec1)
          .build());

  auto actual_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .addColumn<float>("float", CREATE_SUBSTRAIT_TYPE(Fp32), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_batch, actual_batch));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_batch, expected_batch));
}

TEST(CiderBatchCheckerArrowTest, nullTest) {
  auto data_int32 = std::vector<int32_t>{0, 1, 2, 3, 4};
  auto data_fp32 = std::vector<float>{0.5, 1.6, 2.3, 3.6, 4.2};

  auto all_null = std::vector<bool>(5, true);
  auto no_null = std::vector<bool>(5, false);

  // no nulls
  auto expected_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), data_int32, no_null)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), data_fp32, no_null)
          .build());
  auto actual_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), data_int32, no_null)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), data_fp32, no_null)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_1, actual_1));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_1, expected_1));

  // all nulls
  auto expected_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), data_int32, all_null)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), data_fp32, all_null)
          .build());
  auto actual_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), data_int32, all_null)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), data_fp32, all_null)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_2, actual_2));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_2, expected_2));
}

TEST(CiderBatchCheckerArrowTest, ignoreOrder) {
  auto vec_1 = std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto vec_2 = std::vector<int>{0, 2, 4, 6, 8, 1, 3, 5, 7, 9};
  auto nulls_1 =
      std::vector<bool>{true, false, true, false, true, false, true, false, true, false};
  auto nulls_2 =
      std::vector<bool>{true, true, true, true, true, false, false, false, false, false};

  // different order, without nulls
  auto expected_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_1)
          .build());
  auto actual_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_2)
          .build());
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(expected_1, actual_1));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_1, actual_1, true));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(actual_1, expected_1));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_1, expected_1, true));

  // different order, with nulls
  auto expected_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_1, nulls_1)
          .build());
  auto actual_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_2, nulls_2)
          .build());
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(expected_2, actual_2));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(expected_2, actual_2, true));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(actual_2, expected_2));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(actual_2, expected_2, true));
}

TEST(CiderBatchCheckerArrowTest, multiBatches) {
  auto vec_1 = std::vector<int>{0, 1, 2, 3, 4};
  auto nulls_1 = std::vector<bool>{true, true, false, false, false};
  auto vec_2 = std::vector<int>{5, 6, 7, 8, 9};
  auto nulls_2 = std::vector<bool>{false, false, false, true, true};

  auto vec = std::vector<int>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto nulls =
      std::vector<bool>{true, true, false, false, false, false, false, false, true, true};

  auto one_batch = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec, nulls)
          .build());

  // same order
  std::vector<std::shared_ptr<CiderBatch>> many_batches_1;
  // different order
  std::vector<std::shared_ptr<CiderBatch>> many_batches_2;
  auto manys_1 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_1, nulls_1)
          .build());
  auto manys_2 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_2, nulls_2)
          .build());
  auto manys_3 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_1, nulls_1)
          .build());
  auto manys_4 = ArrowToCiderBatch::createCiderBatchFromArrowBuilder(
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("", CREATE_SUBSTRAIT_TYPE(I32), vec_2, nulls_2)
          .build());
  many_batches_1.emplace_back(manys_1);
  many_batches_1.emplace_back(manys_2);
  many_batches_2.emplace_back(manys_4);
  many_batches_2.emplace_back(manys_3);

  // one-to-many and many-to-one
  // same order
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(one_batch, many_batches_1, false));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(many_batches_1, one_batch, false));
  // different order
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(one_batch, many_batches_2, false));
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(many_batches_2, one_batch, false));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(one_batch, many_batches_2, true));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(many_batches_2, one_batch, true));
  // many-to-many
  // same order
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(many_batches_1, many_batches_1, false));
  // different order
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq(many_batches_1, many_batches_2, false));
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq(many_batches_1, many_batches_2, true));
}

/// TODO: (YBRua) tests to be added
/// 1. VarChar tests
/// 2. date / time tests

// Old CiderBatch tests below

#define TEST_SINGLE_COLUMN(C_TYPE, S_TYPE)                                               \
  {                                                                                      \
    std::vector<C_TYPE> vec##S_TYPE{1, 2, 3, 4, 5};                                      \
    auto expected_batch##S_TYPE = std::make_shared<CiderBatch>(                          \
        CiderBatchBuilder()                                                              \
            .addColumn<C_TYPE>("col", CREATE_SUBSTRAIT_TYPE(S_TYPE), vec##S_TYPE)        \
            .build());                                                                   \
    auto actual_batch##S_TYPE = std::make_shared<CiderBatch>(                            \
        CiderBatchBuilder()                                                              \
            .addColumn<C_TYPE>("col", CREATE_SUBSTRAIT_TYPE(S_TYPE), vec##S_TYPE)        \
            .build());                                                                   \
    EXPECT_TRUE(                                                                         \
        CiderBatchChecker::checkEq(expected_batch##S_TYPE, actual_batch##S_TYPE, true)); \
    EXPECT_TRUE(                                                                         \
        CiderBatchChecker::checkEq(actual_batch##S_TYPE, expected_batch##S_TYPE, true)); \
  }

TEST(CiderBatchCheckerTest, singleColumn) {
  TEST_SINGLE_COLUMN(int8_t, I8);
  TEST_SINGLE_COLUMN(int16_t, I16);
  TEST_SINGLE_COLUMN(int32_t, I32);
  TEST_SINGLE_COLUMN(int64_t, I64);
  TEST_SINGLE_COLUMN(float, Fp32);
  TEST_SINGLE_COLUMN(double, Fp64);
}

TEST(CiderBatchCheckerTest, stringBatchEq) {
  // one to one ordered
  std::vector<CiderByteArray> vec;
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec)
          .build());

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch));

  // one to one non-ordered
  std::vector<CiderByteArray> vec2;
  vec2.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));
  vec2.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec2.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));

  auto actual_batch_2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch_2, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch_2, expected_batch, true));

  // complex cases
  std::vector<std::shared_ptr<CiderBatch>> actual_vec_1;
  std::vector<CiderByteArray> vec3;
  std::vector<CiderByteArray> vec4;

  vec3.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));
  vec4.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec4.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  auto actual_batch_3 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec3)
          .build());
  auto actual_batch_4 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(2)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec4)
          .build());
  actual_vec_1.emplace_back(actual_batch_3);
  actual_vec_1.emplace_back(actual_batch_4);

  std::vector<std::shared_ptr<CiderBatch>> actual_vec_2;
  std::vector<CiderByteArray> vec5;
  std::vector<CiderByteArray> vec6;

  vec5.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));
  vec5.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec6.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  auto actual_batch_5 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(2)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec5)
          .build());
  auto actual_batch_6 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(1)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec6)
          .build());
  actual_vec_2.emplace_back(actual_batch_5);
  actual_vec_2.emplace_back(actual_batch_6);

  // one to many non-ordered
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_vec_1, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_vec_2, true));

  // many to one non-ordered
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_vec_1, expected_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_vec_2, expected_batch, true));

  // many to many non-ordered
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_vec_1, actual_vec_2, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_vec_2, actual_vec_1, true));
}

TEST(CiderBatchCheckerTest, varcharBatchEq) {
  std::vector<CiderByteArray> vec;
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(Varchar), vec)
          .build());

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(Varchar), vec)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, stringBatchNEq) {
  std::vector<CiderByteArray> vec1;
  vec1.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec1.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec1.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec1)
          .build());

  std::vector<CiderByteArray> vec2;
  vec2.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec2.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec2.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("bbbbbaaaaa")));

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(String), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkNotEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkNotEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, oneToMultipleColumn) {
  std::vector<int> vec1_1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2_1{6, 7, 8, 9, 10};
  std::vector<float> vec3_1{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<double> vec4_1{1.11, 2.22, 3.33, 4.44, 5.55};

  std::vector<int> vec1_2{100, 9, 8, 7, 6};
  std::vector<int64_t> vec2_2{98, 74, 66, 82, 0};
  std::vector<float> vec3_2{10.1, 2.3, 3.2, 0.5, 6.8};
  std::vector<double> vec4_2{1.11, 2.28, 1.67, 4.49, 0.88};

  std::vector<int> vec1_3{1, 100, 3, 9, 5, 7, 6, 8, 4, 2};
  std::vector<int64_t> vec2_3{6, 98, 8, 74, 10, 82, 0, 66, 9, 7};
  std::vector<float> vec3_3{1.1, 10.1, 3.3, 2.3, 5.5, 0.5, 6.8, 3.2, 4.4, 2.2};
  std::vector<double> vec4_3{1.11, 1.11, 3.33, 2.28, 5.55, 4.49, 0.88, 1.67, 4.44, 2.22};

  std::vector<std::shared_ptr<CiderBatch>> actual_batches;
  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1_1)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2_1)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3_1)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4_1)
          .build());
  actual_batches.emplace_back(actual_batch);
  auto actual_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1_2)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2_2)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3_2)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4_2)
          .build());
  actual_batches.emplace_back(actual_batch2);

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1_3)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2_3)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3_3)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4_3)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batches, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batches, expected_batch, true));

  EXPECT_FALSE(CiderBatchChecker::checkEq(expected_batch, actual_batches, false));
  EXPECT_FALSE(CiderBatchChecker::checkEq(actual_batches, expected_batch, false));
}

TEST(CiderBatchCheckerTest, multipleToMultipleColumn) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{6, 7, 8, 9, 10};
  std::vector<float> vec3{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<double> vec4{1.1, 2.2, 3.3, 4.4, 5.5};

  std::vector<std::shared_ptr<CiderBatch>> actual_batches;
  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
          .build());
  actual_batches.emplace_back(actual_batch);
  auto actual_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
          .build());
  actual_batches.emplace_back(actual_batch2);

  std::vector<std::shared_ptr<CiderBatch>> expected_batches;
  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
          .build());
  expected_batches.emplace_back(expected_batch);
  auto expected_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
          .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
          .build());
  expected_batches.emplace_back(expected_batch2);

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batches, actual_batches, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batches, expected_batches, true));
}

TEST(CiderBatchCheckerTest, rowNum) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int> vec2{1, 2, 3, 4, 5, 6};

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec2)
          .build());

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkNotEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkNotEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, colNum) {
  std::vector<int> vec1{1, 2, 3, 4, 5};

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkNotEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkNotEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, rowValue) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  std::vector<int> vec2{0, 2, 3, 4, 5};
  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkNotEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkNotEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, typeCheck) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{1, 2, 3, 4, 5};

  auto expected_batch1 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  auto actual_batch1 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int64_t>("long", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch1, actual_batch1, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch1, expected_batch1, true));

  auto expected_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());

  auto actual_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .addColumn<int64_t>("long", CREATE_SUBSTRAIT_TYPE(I64), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch2, actual_batch2, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch2, expected_batch2, true));
}

TEST(CiderBatchCheckerTest, valueCheck) {
  std::vector<double> vec1{1, 2, 3, 4, 5};
  std::vector<float> vec2{1, 2, 3, 4, 5};

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<double>("double", CREATE_SUBSTRAIT_TYPE(Fp64), vec1)
          .build());

  auto actual_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<float>("float", CREATE_SUBSTRAIT_TYPE(Fp32), vec2)
          .build());

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch, true));
}

TEST(CiderBatchCheckerTest, vectorEqual) {
  std::vector<int> vec0{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec0)
          .build());

  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int> vec2{6, 7, 8, 9, 10};

  std::vector<std::shared_ptr<CiderBatch>> actual_batches;
  auto actual_batch1 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
          .build());
  auto actual_batch2 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec2)
          .build());
  actual_batches.emplace_back(actual_batch1);
  actual_batches.emplace_back(actual_batch2);

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batches, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batches, expected_batch, true));

  auto actual_batch3 = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec0)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch3, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch3, expected_batch, true));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
