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
#include "CiderNongroupbyAggTestHelper.h"
#include "TestHelpers.h"

using namespace TestHelpers;
static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();
class CiderNongroupbyAggArrowFormatTest : public ::testing::Test {
 protected:
  static MockTable* MockTableForTest;

  static void SetUpTestSuite() {
    if (nullptr == MockTableForTest) {
      size_t element_num = 5;

      std::vector<std::string> col_names = {"tinyint_notnull",
                                            "tinyint_null",
                                            "smallint_notnull",
                                            "smallint_null",
                                            "int_notnull",
                                            "int_null",
                                            "bigint_notnull",
                                            "bigint_null",
                                            "float_notnull",
                                            "float_null",
                                            "double_notnull",
                                            "double_null",
                                            "bool_true_notnull",
                                            "bool_true_null",
                                            "bool_false_notnull",
                                            "bool_false_null"};

      std::vector<SQLTypeInfo> col_types = {
          SQLTypeInfo(kTINYINT, true),
          SQLTypeInfo(kTINYINT, false),
          SQLTypeInfo(kSMALLINT, true),
          SQLTypeInfo(kSMALLINT, false),
          SQLTypeInfo(kINT, true),
          SQLTypeInfo(kINT, false),
          SQLTypeInfo(kBIGINT, true),
          SQLTypeInfo(kBIGINT, false),
          SQLTypeInfo(kFLOAT, true),
          SQLTypeInfo(kFLOAT, false),
          SQLTypeInfo(kDOUBLE, true),
          SQLTypeInfo(kDOUBLE, false),
          SQLTypeInfo(kBOOLEAN, true),
          SQLTypeInfo(kBOOLEAN, false),
          SQLTypeInfo(kBOOLEAN, true),
          SQLTypeInfo(kBOOLEAN, false),
      };

      int8_t* tinyint_ptr = new int8_t[element_num];
      int16_t* smallint_ptr = new int16_t[element_num];
      int32_t* int_ptr = new int32_t[element_num];
      int64_t* bigint_ptr = new int64_t[element_num];
      float* float_ptr = new float[element_num];
      double* double_ptr = new double[element_num];
      bool* bool_true_ptr = new bool[element_num];
      bool* bool_false_ptr = new bool[element_num];

      for (size_t i = 0; i < element_num; ++i) {
        tinyint_ptr[i] = -128 + i;
        smallint_ptr[i] = 32765 + i;
        int_ptr[i] = -1234567890 + i;
        bigint_ptr[i] = 9876543210 + i;
        float_ptr[i] = 123456.123456 + i;
        double_ptr[i] = 123456789.123456789 + i;
        bool_true_ptr[i] = true;
        bool_false_ptr[i] = false;
      }

      std::vector<int8_t*> col_datas = {reinterpret_cast<int8_t*>(tinyint_ptr),
                                        reinterpret_cast<int8_t*>(tinyint_ptr),
                                        reinterpret_cast<int8_t*>(smallint_ptr),
                                        reinterpret_cast<int8_t*>(smallint_ptr),
                                        reinterpret_cast<int8_t*>(int_ptr),
                                        reinterpret_cast<int8_t*>(int_ptr),
                                        reinterpret_cast<int8_t*>(bigint_ptr),
                                        reinterpret_cast<int8_t*>(bigint_ptr),
                                        reinterpret_cast<int8_t*>(float_ptr),
                                        reinterpret_cast<int8_t*>(float_ptr),
                                        reinterpret_cast<int8_t*>(double_ptr),
                                        reinterpret_cast<int8_t*>(double_ptr),
                                        reinterpret_cast<int8_t*>(bool_true_ptr),
                                        reinterpret_cast<int8_t*>(bool_true_ptr),
                                        reinterpret_cast<int8_t*>(bool_false_ptr),
                                        reinterpret_cast<int8_t*>(bool_false_ptr)};

      MockTableForTest = new MockTable(col_names, col_types, col_datas, element_num);
    }
  }

  static void TearDownTestSuite() {
    delete MockTableForTest;
    MockTableForTest = nullptr;
  }
};

MockTable* CiderNongroupbyAggArrowFormatTest::MockTableForTest = nullptr;

void testScalarTypeSingleKey(MockTable* table,
                             const std::string& col_name,
                             AggArrowResult expect_result,
                             bool all_null) {
  // select COUNT(), COUNT(col), SUM(col), MIN(col), MAX(col), AVG(col) from table
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);

  auto count_noparam_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kBIGINT, true), kCOUNT, nullptr, false, nullptr);
  auto count_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kBIGINT, true), kCOUNT, col_expr, false, nullptr);
  auto sum_expr = makeExpr<Analyzer::AggExpr>(
      col_expr->get_type_info(), kSUM, col_expr, false, nullptr);
  auto min_expr = makeExpr<Analyzer::AggExpr>(
      col_expr->get_type_info(), kMIN, col_expr, false, nullptr);
  auto max_expr = makeExpr<Analyzer::AggExpr>(
      col_expr->get_type_info(), kMAX, col_expr, false, nullptr);

  // Final avg for arrow format will lead to a column check fail, and this kind of query
  // scenario will not happen anymore, so we remove test for avg.

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {},
                              {count_noparam_expr.get(),
                               count_expr.get(),
                               sum_expr.get(),
                               max_expr.get(),
                               min_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<substrait::Type> column_types;
  std::vector<std::string> col_names;
  for (size_t i_target = 0; i_target < ra_exe_unit_ptr->target_exprs.size(); i_target++) {
    col_names.push_back(std::to_string(i_target));
    column_types.push_back(generator::getSubstraitType(
        ra_exe_unit_ptr->target_exprs[i_target]->get_type_info()));
  }
  std::vector<ColumnHint> col_hints = {Normal, Normal, Normal, Normal, Normal};
  auto schema =
      std::make_shared<CiderTableSchema>(col_names, column_types, "", col_hints);

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 4);

  runArrowTest(std::string("SingleKeyScalarTypeTest-" + col_name),
               table,
               ra_exe_unit_ptr,
               schema,
               {col_name},
               expect_result,
               all_null,
               false,
               16384,
               0,
               null_vectors);
}

TEST_F(CiderNongroupbyAggArrowFormatTest, SingleKeyScalarTypeTest) {
  AggArrowResult tinyint_agg_result_not_null = {.count_one = 5,
                                                .count_column = 5,
                                                .sum_int64 = -118,
                                                .max_int64 = -124,
                                                .min_int64 = -128,
                                                .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "tinyint_notnull", tinyint_agg_result_not_null, false);
  AggArrowResult tinyint_agg_result_with_null = {.count_one = 5,
                                                 .count_column = 3,
                                                 .sum_int64 = -123,
                                                 .max_int64 = -125,
                                                 .min_int64 = -128,
                                                 .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "tinyint_null", tinyint_agg_result_with_null, false);
  AggArrowResult tinyint_agg_result_all_null = {.count_one = 5,
                                                .count_column = 0,
                                                .sum_int64 = 0,
                                                .max_int64 = 0,
                                                .min_int64 = -1,
                                                .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "tinyint_null", tinyint_agg_result_all_null, true);

  AggArrowResult smallint_agg_result_not_null = {.count_one = 5,
                                                 .count_column = 5,
                                                 .sum_int64 = 32763,
                                                 .max_int64 = 32767,
                                                 .min_int64 = -32768,
                                                 .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "smallint_notnull", smallint_agg_result_not_null, false);
  AggArrowResult smallint_agg_result_with_null = {.count_one = 5,
                                                  .count_column = 3,
                                                  .sum_int64 = 32764,
                                                  .max_int64 = 32767,
                                                  .min_int64 = -32768,
                                                  .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "smallint_null", smallint_agg_result_with_null, false);
  AggArrowResult smallint_agg_result_all_null = {.count_one = 5,
                                                 .count_column = 0,
                                                 .sum_int64 = 0,
                                                 .max_int64 = 0,
                                                 .min_int64 = -1,
                                                 .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "smallint_null", smallint_agg_result_all_null, true);

  AggArrowResult int_agg_result_not_null = {.count_one = 5,
                                            .count_column = 5,
                                            .sum_int64 = -1877872144,
                                            .max_int64 = -1234567886,
                                            .min_int64 = -1234567890,
                                            .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "int_notnull", int_agg_result_not_null, false);
  AggArrowResult int_agg_result_with_null = {.count_one = 5,
                                             .count_column = 3,
                                             .sum_int64 = 591263631,
                                             .max_int64 = -1234567887,
                                             .min_int64 = -1234567890,
                                             .null = 0};
  testScalarTypeSingleKey(MockTableForTest, "int_null", int_agg_result_with_null, false);
  AggArrowResult int_agg_result_all_null = {.count_one = 5,
                                            .count_column = 0,
                                            .sum_int64 = 0,
                                            .max_int64 = 0,
                                            .min_int64 = -1,
                                            .null = 0};
  testScalarTypeSingleKey(MockTableForTest, "int_null", int_agg_result_all_null, true);

  AggArrowResult bigint_agg_result_not_null = {.count_one = 5,
                                               .count_column = 5,
                                               .sum_int64 = 49382716060,
                                               .max_int64 = 9876543214,
                                               .min_int64 = 9876543210,
                                               .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "bigint_notnull", bigint_agg_result_not_null, false);
  AggArrowResult bigint_agg_result_with_null = {.count_one = 5,
                                                .count_column = 3,
                                                .sum_int64 = 29629629635,
                                                .max_int64 = 9876543213,
                                                .min_int64 = 9876543210,
                                                .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "bigint_null", bigint_agg_result_with_null, false);
  AggArrowResult bigint_agg_result_all_null = {
      .count_one = 5,
      .count_column = 0,
      .sum_int64 = 0,
      .max_int64 = std::numeric_limits<int64_t>::min(),
      .min_int64 = std::numeric_limits<int64_t>::max(),
      .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "bigint_null", bigint_agg_result_all_null, true);

  AggArrowResult float_agg_result_not_null = {.count_one = 5,
                                              .count_column = 5,
                                              .sum_float = 617290.61728f,
                                              .max_float = 123460.123456f,
                                              .min_float = 123456.123456f,
                                              .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "float_notnull", float_agg_result_not_null, false);
  AggArrowResult float_agg_result_with_null = {.count_one = 5,
                                               .count_column = 3,
                                               .sum_float = 370373.370368f,
                                               .max_float = 123459.123456f,
                                               .min_float = 123456.123456f,
                                               .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "float_null", float_agg_result_with_null, false);
  AggArrowResult float_agg_result_all_null = {
      .count_one = 5,
      .count_column = 0,
      .sum_float = 0.0f,
      .max_float = -std::numeric_limits<float>::max(),
      .min_float = std::numeric_limits<float>::max(),
      .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "float_null", float_agg_result_all_null, true);

  AggArrowResult double_agg_result_not_null = {.count_one = 5,
                                               .count_column = 5,
                                               .sum_double = 617283955.61728394,
                                               .max_double = 123456793.123456789,
                                               .min_double = 123456789.123456789,
                                               .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "double_notnull", double_agg_result_not_null, false);
  AggArrowResult double_agg_result_with_null = {.count_one = 5,
                                                .count_column = 3,
                                                .sum_double = 370370372.370370367,
                                                .max_double = 123456792.123456789,
                                                .min_double = 123456789.123456789,
                                                .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "double_null", double_agg_result_with_null, false);
  AggArrowResult double_agg_result_all_null = {
      .count_one = 5,
      .count_column = 0,
      .sum_double = 0.0,
      .max_double = -std::numeric_limits<double>::max(),
      .min_double = std::numeric_limits<double>::max(),
      .null = 0};
  testScalarTypeSingleKey(
      MockTableForTest, "double_null", double_agg_result_all_null, true);
}

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
