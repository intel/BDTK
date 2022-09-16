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
#include "CiderBatchBuilder.h"
#include "CiderBatchChecker.h"
#include "exec/plan/parser/TypeUtils.h"

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

  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch, true));
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch, true));

  // TODO: add multi batches to multi batches check
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
