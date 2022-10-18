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
#include "cider/batch/ScalarBatch.h"
#include "cider/batch/StructBatch.h"
#include "exec/plan/parser/TypeUtils.h"

template <typename T>
void fillSequenceChildScalarBatch(ScalarBatch<T>* child, int n_rows, int n_nulls) {
  CHECK_LE(n_nulls, n_rows);
  CHECK(child->resizeBatch(n_rows));
  auto data_buffer = child->getMutableRawData();
  auto null_buffer = child->getMutableNulls();

  int n_not_nulls = n_rows - n_nulls;
  for (int i = 0; i < n_rows; ++i) {
    bool is_valid = (i < n_not_nulls);
    T value = static_cast<T>(i);
    data_buffer[i] = is_valid ? i : 0;
    if (is_valid) {
      CiderBitUtils::setBitAt(null_buffer, i);
    } else {
      CiderBitUtils::clearBitAt(null_buffer, i);
    }
  }
}

std::shared_ptr<CiderBatch> generateSimpleArrowCiderBatch(
    int n_rows = 10,
    int n_nulls = 3,
    const std::vector<SQLTypeInfo>& c = {}) {
  std::vector<SQLTypeInfo> children_types;
  if (c.size()) {
    children_types = c;
  } else {
    children_types = {SQLTypeInfo(kINT, false), SQLTypeInfo(kFLOAT, false)};
  }
  auto types = SQLTypeInfo(kSTRUCT, false, children_types);

  auto schema = CiderBatchUtils::convertCiderTypeInfoToArrowSchema(types);
  auto batch = StructBatch::Create(schema, std::make_shared<CiderDefaultAllocator>());
  CHECK(batch->resizeBatch(n_rows));

  auto n_cols = batch->getChildrenNum();
  for (auto col_index = 0; col_index < n_cols; ++col_index) {
    auto child_type = children_types[col_index];
    auto child = batch->getChildAt(col_index);

    int child_n_nulls = child_type.get_notnull() ? 0 : n_nulls;
    switch (child_type.get_type()) {
      case kTINYINT:
        fillSequenceChildScalarBatch<int8_t>(
            child->asMutable<ScalarBatch<int8_t>>(), n_rows, child_n_nulls);
        break;
      case kSMALLINT:
        fillSequenceChildScalarBatch<int16_t>(
            child->asMutable<ScalarBatch<int16_t>>(), n_rows, child_n_nulls);
        break;
      case kINT:
        fillSequenceChildScalarBatch<int32_t>(
            child->asMutable<ScalarBatch<int32_t>>(), n_rows, child_n_nulls);
        break;
      case kBIGINT:
        fillSequenceChildScalarBatch<int64_t>(
            child->asMutable<ScalarBatch<int64_t>>(), n_rows, child_n_nulls);
        break;
      case kFLOAT:
        fillSequenceChildScalarBatch<float>(
            child->asMutable<ScalarBatch<float>>(), n_rows, child_n_nulls);
        break;
      case kDOUBLE:
        fillSequenceChildScalarBatch<double>(
            child->asMutable<ScalarBatch<double>>(), n_rows, child_n_nulls);
        break;
      default:
        CIDER_THROW(CiderCompileException, "Unsupported data type.");
    }
  }

  return std::make_shared<CiderBatch>(std::move(*batch));
}

TEST(CiderBatchCheckerArrowTest, ifItRuns) {
  auto expected = generateSimpleArrowCiderBatch();
  auto actual = generateSimpleArrowCiderBatch();
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq({expected}, {actual}));
}

TEST(CiderBatchCheckerArrowTest, colNumCheck) {
  auto expected_1 = generateSimpleArrowCiderBatch();
  auto actual_1 = generateSimpleArrowCiderBatch();
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq({expected_1}, {actual_1}));

  auto expected_2 = generateSimpleArrowCiderBatch();
  auto actual_2 = generateSimpleArrowCiderBatch(10, 3, {SQLTypeInfo(kINT, false)});
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq({expected_2}, {actual_2}));

  auto expected_3 = generateSimpleArrowCiderBatch(10, 3, {SQLTypeInfo(kINT, false)});
  auto actual_3 = generateSimpleArrowCiderBatch();
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq({expected_3}, {actual_3}));

  auto expected_4 = generateSimpleArrowCiderBatch(0, 0);
  auto actual_4 = generateSimpleArrowCiderBatch(0, 0, {SQLTypeInfo(kINT, false)});
  EXPECT_TRUE(CiderBatchChecker::checkArrowEq({expected_4}, {actual_4}));
}

TEST(CiderBatchCheckerArrowTest, rowNumCheck) {
  auto expected_1 = generateSimpleArrowCiderBatch();
  auto actual_1 = generateSimpleArrowCiderBatch(20, 6);
  EXPECT_FALSE(CiderBatchChecker::checkArrowEq({expected_1}, {actual_1}));
}

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
