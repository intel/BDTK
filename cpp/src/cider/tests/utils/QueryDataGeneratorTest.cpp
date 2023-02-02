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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "CiderBatchBuilder.h"
#include "CiderBatchChecker.h"
#include "QueryDataGenerator.h"
#include "exec/plan/parser/TypeUtils.h"

#define VEC \
  { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }

#define TEST_GEN_SINGLE_COLUMN(C_TYPE, S_TYPE)                                 \
  {                                                                            \
    auto actual_batch =                                                        \
        std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes( \
            10, {"col"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}));                    \
    EXPECT_EQ(actual_batch->row_num(), 10);                                    \
    EXPECT_EQ(actual_batch->column_num(), 1);                                  \
                                                                               \
    auto expected_batch = std::make_shared<CiderBatch>(                        \
        CiderBatchBuilder()                                                    \
            .addColumn<C_TYPE>("col_1", CREATE_SUBSTRAIT_TYPE(S_TYPE), VEC)    \
            .build());                                                         \
    EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));     \
  }

#define TEST_GEN_SINGLE_COLUMN_RANDOM(S_TYPE)                                       \
  {                                                                                 \
    auto actual_batch = QueryDataGenerator::generateBatchByTypes(                   \
        10, {"col"}, {CREATE_SUBSTRAIT_TYPE(S_TYPE)}, {}, GeneratePattern::Random); \
    EXPECT_EQ(actual_batch.row_num(), 10);                                          \
    EXPECT_EQ(actual_batch.column_num(), 1);                                        \
  }

TEST(QueryDataGeneratorTest, genSingleColumn) {
  TEST_GEN_SINGLE_COLUMN(int8_t, I8);
  TEST_GEN_SINGLE_COLUMN(int16_t, I16);
  TEST_GEN_SINGLE_COLUMN(int32_t, I32);
  TEST_GEN_SINGLE_COLUMN(int64_t, I64);
  TEST_GEN_SINGLE_COLUMN(float, Fp32);
  TEST_GEN_SINGLE_COLUMN(double, Fp64);
}

TEST(QueryDataGeneratorTest, genSingleColumnRandom) {
  auto actual_batch = QueryDataGenerator::generateBatchByTypes(
      10, {"col"}, {CREATE_SUBSTRAIT_TYPE(I32)}, {}, GeneratePattern::Random);

  EXPECT_EQ(actual_batch.row_num(), 10);
  EXPECT_EQ(actual_batch.column_num(), 1);
  EXPECT_NE(actual_batch.column(0), nullptr);

  if (false) {  // print out the data if necessary
    const int32_t* buf = (const int32_t*)actual_batch.column(0);
    for (int i = 0; i < 10; i++) {
      std::cout << buf[i] << "\t";
    }
    std::cout << std::endl;
  }
}

TEST(QueryDataGeneratorTest, genStringColumn) {
  auto actual_batch = QueryDataGenerator::generateBatchByTypes(
      62, {"col_string"}, {CREATE_SUBSTRAIT_TYPE(String)});

  EXPECT_EQ(actual_batch.row_num(), 62);
  EXPECT_EQ(actual_batch.column_num(), 1);

  CiderByteArray* array = (CiderByteArray*)actual_batch.column(0);
  EXPECT_EQ(array[0].len, 10);
  EXPECT_EQ(std::memcmp(array[0].ptr, "0000000000", array[0].len), 0);
  EXPECT_EQ(std::memcmp(array[10].ptr, "AAAAAAAAAA", array[10].len), 0);
  EXPECT_EQ(std::memcmp(array[36].ptr, "aaaaaaaaaa", array[36].len), 0);
  EXPECT_EQ(std::memcmp(array[61].ptr, "zzzzzzzzzz", array[36].len), 0);
  EXPECT_NE(std::memcmp(array[61].ptr, "aaaaaaaaaa", array[36].len), 0);
}

TEST(QueryDataGeneratorTest, randomData) {
  TEST_GEN_SINGLE_COLUMN_RANDOM(I8);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I16);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I32);
  TEST_GEN_SINGLE_COLUMN_RANDOM(I64);
  TEST_GEN_SINGLE_COLUMN_RANDOM(Fp32);
  TEST_GEN_SINGLE_COLUMN_RANDOM(Fp64);
}

TEST(TpcHQueryDataGeneratorTest, genTables) {
  auto batchLineitemSeq = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 0), GeneratePattern::Sequence);
  EXPECT_EQ(1024, batchLineitemSeq.row_num());
  EXPECT_EQ(16, batchLineitemSeq.column_num());

  auto batchLineitemRdm = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 0), GeneratePattern::Random);
  EXPECT_EQ(1024, batchLineitemRdm.row_num());
  EXPECT_EQ(16, batchLineitemRdm.column_num());
}

TEST(TpcHQueryDataGeneratorTest, genTablesWithNullValues) {
  auto batchLineitemAllNullSeq = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 1), GeneratePattern::Sequence);
  EXPECT_EQ(1024, batchLineitemAllNullSeq.row_num());
  EXPECT_EQ(16, batchLineitemAllNullSeq.column_num());

  auto batchLineitemAllNullRdm = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 1), GeneratePattern::Random);
  EXPECT_EQ(1024, batchLineitemAllNullRdm.row_num());
  EXPECT_EQ(16, batchLineitemAllNullRdm.column_num());

  auto batchLineitemNullSeq = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 5), GeneratePattern::Sequence);
  EXPECT_EQ(1024, batchLineitemNullSeq.row_num());
  EXPECT_EQ(16, batchLineitemNullSeq.column_num());

  auto batchLineitemNullRdm = TpcHDataGenerator::genLineitem(
      1024, std::vector<int32_t>(16, 5), GeneratePattern::Random);
  EXPECT_EQ(1024, batchLineitemNullRdm.row_num());
  EXPECT_EQ(16, batchLineitemNullRdm.column_num());
}

TEST(QueryDataGeneratorTest, genMultiColumns) {
  auto actual_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          10,
          {"col_i8", "col_i16", "col_i32", "col_i64", "col_f32", "col_f64"},
          {CREATE_SUBSTRAIT_TYPE(I8),
           CREATE_SUBSTRAIT_TYPE(I16),
           CREATE_SUBSTRAIT_TYPE(I32),
           CREATE_SUBSTRAIT_TYPE(I64),
           CREATE_SUBSTRAIT_TYPE(Fp32),
           CREATE_SUBSTRAIT_TYPE(Fp64)}));
  EXPECT_EQ(actual_batch->row_num(), 10);
  EXPECT_EQ(actual_batch->column_num(), 6);

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addColumn<int8_t>("", CREATE_SUBSTRAIT_TYPE(I8), VEC)
          .addColumn<int16_t>("", CREATE_SUBSTRAIT_TYPE(I16), VEC)
          .addColumn<int32_t>("", CREATE_SUBSTRAIT_TYPE(I32), VEC)
          .addColumn<int64_t>("", CREATE_SUBSTRAIT_TYPE(I64), VEC)
          .addColumn<float>("", CREATE_SUBSTRAIT_TYPE(Fp32), VEC)
          .addColumn<double>("", CREATE_SUBSTRAIT_TYPE(Fp64), VEC)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(expected_batch, actual_batch));
}

TEST(QueryDataGeneratorTest, genDateColumn) {
  auto actual_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          5, {"col_date"}, {CREATE_SUBSTRAIT_TYPE(Date)}));

  EXPECT_EQ(actual_batch->row_num(), 5);
  EXPECT_EQ(actual_batch->column_num(), 1);

  std::vector<CiderDateType> col;
  col.push_back(CiderDateType("1970-01-01"));
  col.push_back(CiderDateType("1970-01-02"));
  col.push_back(CiderDateType("1970-01-03"));
  col.push_back(CiderDateType("1970-01-04"));
  col.push_back(CiderDateType("1970-01-05"));

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addTimingColumn<CiderDateType>("col_date", CREATE_SUBSTRAIT_TYPE(Date), col)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch));
}

TEST(QueryDataGeneratorTest, genTimeColumn) {
  auto actual_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          5, {"col_time"}, {CREATE_SUBSTRAIT_TYPE(Time)}));

  EXPECT_EQ(actual_batch->row_num(), 5);
  EXPECT_EQ(actual_batch->column_num(), 1);

  std::vector<CiderTimeType> col;
  col.push_back(CiderTimeType("00:00:00.000000"));
  col.push_back(CiderTimeType("00:00:00.000001"));
  col.push_back(CiderTimeType("00:00:00.000002"));
  col.push_back(CiderTimeType("00:00:00.000003"));
  col.push_back(CiderTimeType("00:00:00.000004"));

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addTimingColumn<CiderTimeType>("col_time", CREATE_SUBSTRAIT_TYPE(Time), col)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch));
}

TEST(QueryDataGeneratorTest, genTimestampColumn) {
  auto actual_batch =
      std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
          5, {"col_timestamp"}, {CREATE_SUBSTRAIT_TYPE(Timestamp)}));

  EXPECT_EQ(actual_batch->row_num(), 5);
  EXPECT_EQ(actual_batch->column_num(), 1);

  std::vector<CiderTimestampType> col;
  col.push_back(CiderTimestampType("1970-01-01 00:00:00.000000"));
  col.push_back(CiderTimestampType("1970-01-01 00:00:00.000001"));
  col.push_back(CiderTimestampType("1970-01-01 00:00:00.000002"));
  col.push_back(CiderTimestampType("1970-01-01 00:00:00.000003"));
  col.push_back(CiderTimestampType("1970-01-01 00:00:00.000004"));

  auto expected_batch = std::make_shared<CiderBatch>(
      CiderBatchBuilder()
          .addTimingColumn<CiderTimestampType>(
              "col_timestamp", CREATE_SUBSTRAIT_TYPE(Timestamp), col)
          .build());
  EXPECT_TRUE(CiderBatchChecker::checkEq(actual_batch, expected_batch));
}

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
