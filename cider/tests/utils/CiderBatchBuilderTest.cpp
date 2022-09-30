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
#include "exec/plan/parser/TypeUtils.h"

TEST(CiderBatchBuilderTest, EmptyBatch) {
  // should not build an empty batch.
  EXPECT_THROW({ auto emptyBatch = CiderBatchBuilder().build(); }, CiderCompileException);

  auto batch =
      CiderBatchBuilder().addColumn<int>("col", CREATE_SUBSTRAIT_TYPE(I32), {}).build();
  EXPECT_EQ(0, batch.row_num());
  EXPECT_EQ(1, batch.column_num());
}

TEST(CiderBatchBuilderTest, RowNum) {
  // test add empty row num
  auto batch1 = CiderBatchBuilder()
                    .setRowNum(10)
                    .addColumn<int>("col", CREATE_SUBSTRAIT_TYPE(I32), {})
                    .build();
  EXPECT_EQ(10, batch1.row_num());

  auto batch2 = CiderBatchBuilder()
                    .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), {})
                    .addColumn<int>("col2", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
                    .build();
  EXPECT_EQ(5, batch2.row_num());

  // test set row num multiple times.
  EXPECT_THROW({ auto batch3 = CiderBatchBuilder().setRowNum(10).setRowNum(20).build(); },
               CiderCompileException);

  // test set row num after add Column
  EXPECT_THROW(
      {
        auto batch4 =
            CiderBatchBuilder()
                .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
                .setRowNum(20)
                .build();
      },
      CiderCompileException);

  // test row num not equal
  EXPECT_THROW(
      {
        auto batch5 =
            CiderBatchBuilder()
                .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
                .addColumn<int>("col2", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5, 6})
                .build();
      },
      CiderCompileException);

  EXPECT_THROW(
      {
        auto batch6 =
            CiderBatchBuilder()
                .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5})
                .addColumn<int>("col2", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4})
                .build();
      },
      CiderCompileException);
}

TEST(CiderBatchBuilderTest, OneColumnBatch) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  auto batch = CiderBatchBuilder()
                   .setRowNum(5)
                   .addColumn<int>("int", CREATE_SUBSTRAIT_TYPE(I32), vec1)
                   .build();
  EXPECT_EQ(5, batch.row_num());
  EXPECT_EQ(1, batch.column_num());
}

TEST(CiderBatchBuilderTest, CiderByteArrayBatch) {
  std::vector<CiderByteArray> vec;
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("aaaaa")));
  vec.push_back(CiderByteArray(5, reinterpret_cast<const uint8_t*>("bbbbb")));
  vec.push_back(CiderByteArray(10, reinterpret_cast<const uint8_t*>("aaaaabbbbb")));

  auto batch =
      CiderBatchBuilder()
          .setRowNum(3)
          .addColumn<CiderByteArray>("col_str", CREATE_SUBSTRAIT_TYPE(Varchar), vec)
          .build();
  EXPECT_EQ(3, batch.row_num());
  auto ptr = reinterpret_cast<const CiderByteArray*>(batch.column(0));
  CHECK_EQ(5, ptr[0].len);
  CHECK_EQ(5, ptr[1].len);
  CHECK_EQ(10, ptr[2].len);

  CHECK_EQ(0, std::memcmp("aaaaa", ptr[0].ptr, ptr[0].len));
  CHECK_EQ(0, std::memcmp("bbbbb", ptr[1].ptr, ptr[1].len));
  CHECK_EQ(0, std::memcmp("aaaaabbbbb", ptr[2].ptr, ptr[2].len));
  CHECK_NE(0, std::memcmp("bbbbbaaaaa", ptr[2].ptr, ptr[2].len));
}

TEST(CiderBatchBuilderTest, StringBatch) {
  auto batch1 = CiderBatchBuilder()
                    .setRowNum(5)
                    .addColumn<std::string>("str", CREATE_SUBSTRAIT_TYPE(String), {})
                    .build();
  EXPECT_EQ(5, batch1.row_num());
  EXPECT_EQ(1, batch1.column_num());

  auto batch2 = CiderBatchBuilder()
                    .setRowNum(5)
                    .addColumn<std::string>("VarChar", CREATE_SUBSTRAIT_TYPE(Varchar), {})
                    .build();
  EXPECT_EQ(5, batch2.row_num());
  EXPECT_EQ(1, batch2.column_num());

  // test even we pass some input data, the data buffer should be nullptr.
  auto batch3 =
      CiderBatchBuilder()
          .setRowNum(5)
          .addColumn<std::string>(
              "VarChar", CREATE_SUBSTRAIT_TYPE(Varchar), {"a", "b", "c", "d", "e"})
          .build();
  EXPECT_EQ(5, batch3.row_num());
  EXPECT_EQ(1, batch3.column_num());
}

TEST(CiderBatchBuilderTest, MultiColumnsBatch) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int> vec2{6, 7, 8, 9, 10};

  auto batch = CiderBatchBuilder()
                   .setRowNum(5)
                   .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
                   .addColumn<int>("col2", CREATE_SUBSTRAIT_TYPE(I32), vec2)
                   .build();
  EXPECT_EQ(5, batch.row_num());
  EXPECT_EQ(2, batch.column_num());
}

TEST(CiderBatchBuilderTest, MultiTypesBatch) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{6, 7, 8, 9, 10};
  std::vector<float> vec3{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<double> vec4{1.1, 2.2, 3.3, 4.4, 5.5};

  auto batch = CiderBatchBuilder()
                   .setRowNum(5)
                   .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
                   .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
                   .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
                   .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
                   .build();

  EXPECT_EQ(5, batch.row_num());
  EXPECT_EQ(4, batch.column_num());
}

TEST(CiderBatchBuilderTest, ToValueStringTest) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{1, 2, 3, 4, 5};
  std::vector<float> vec3{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<double> vec4{1.1, 2.2, 3.3, 4.4, 5.5};

  auto batch = CiderBatchBuilder()
                   .setRowNum(5)
                   .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1)
                   .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2)
                   .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3)
                   .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4)
                   .build();

  std::string res =
      "row num: 5, column num: 4.\n"
      "column type: int32_t 1\t2\t3\t4\t5\t\n"
      "column type: int64_t 1\t2\t3\t4\t5\t\n"
      "column type: float 1.1\t2.2\t3.3\t4.4\t5.5\t\n"
      "column type: double 1.1\t2.2\t3.3\t4.4\t5.5\t\n";
  EXPECT_EQ(res, batch.toValueString());
}

TEST(CiderBatchBuilderTest, DateTypebatch) {
  std::vector<CiderDateType> vec1;
  vec1.push_back(CiderDateType("1970-01-01"));
  vec1.push_back(CiderDateType("1970-01-02"));
  vec1.push_back(CiderDateType("2020-01-01"));
  vec1.push_back(CiderDateType("2030-01-01"));
  vec1.push_back(CiderDateType("1970-01-01"));

  auto batch1 =
      CiderBatchBuilder()
          .setRowNum(5)
          .addTimingColumn<CiderDateType>("col1", CREATE_SUBSTRAIT_TYPE(Date), vec1)
          .build();

  EXPECT_EQ(5, batch1.row_num());
  EXPECT_EQ(1, batch1.column_num());

  EXPECT_THROW(
      {
        std::vector<CiderDateType> vec2;
        vec2.push_back(CiderDateType("1970-13-01"));
        auto batch2 =
            CiderBatchBuilder()
                .addTimingColumn<CiderDateType>("col1", CREATE_SUBSTRAIT_TYPE(Date), vec2)
                .build();
      },
      CiderCompileException);
}

// CiderBatchBuilder will not use null_vector in addColumn context. You should generate
// null value(MIN_VALUE) for target type in value vector.
TEST(CiderBatchBuilderTest, nullTest) {
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<int64_t> vec2{1, 2, 3, 4, 5};
  std::vector<float> vec3{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<double> vec4{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<bool> vec_null{true, false, true, false, true};

  auto batch = CiderBatchBuilder()
                   .setRowNum(5)
                   .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1, vec_null)
                   .addColumn<int64_t>("col2", CREATE_SUBSTRAIT_TYPE(I64), vec2, vec_null)
                   .addColumn<float>("col3", CREATE_SUBSTRAIT_TYPE(Fp32), vec3, vec_null)
                   .addColumn<double>("col4", CREATE_SUBSTRAIT_TYPE(Fp64), vec4, vec_null)
                   .build();

  std::string res =
      "row num: 5, column num: 4.\n"
      "column type: int32_t 1\t2\t3\t4\t5\t\n"
      "column type: int64_t 1\t2\t3\t4\t5\t\n"
      "column type: float 1.1\t2.2\t3.3\t4.4\t5.5\t\n"
      "column type: double 1.1\t2.2\t3.3\t4.4\t5.5\t\n";
  EXPECT_EQ(res, batch.toValueString());
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
