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
#define CIDERBATCH_WITH_ARROW

#include <gtest/gtest.h>
#include "CiderAggTestHelper.h"
#include "TestHelpers.h"
#include "cider/CiderTypes.h"

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

extern bool g_is_test_env;
extern bool g_enable_watchdog;
extern size_t g_big_group_threshold;
extern size_t g_watchdog_baseline_max_groups;

using namespace TestHelpers;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderNewDataFormatTest : public ::testing::Test {
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
        tinyint_ptr[i] = i;
        smallint_ptr[i] = i;
        int_ptr[i] = i;
        bigint_ptr[i] = i;
        float_ptr[i] = i;
        double_ptr[i] = i;
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

MockTable* CiderNewDataFormatTest::MockTableForTest = nullptr;

void testScalarTypeSingleKey(MockTable* table,
                             const std::string& col_name,
                             size_t expect_ans_row_num) {
  // select col, CAST(col), -col, MAX(IS NULL col), COUNT(), COUNT(col), SUM(col),
  // MIN(col), MAX(col), AVG(col) from table group by col;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);

  auto col_var_expr = makeExpr<Analyzer::Var>(
      col->type, td->table_id, col->column_id, 0, false, Analyzer::Var::kGROUPBY, 1);
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
  auto avg_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kDOUBLE, col_expr->get_type_info().get_notnull()),
      kAVG,
      col_expr,
      false,
      nullptr);

  auto cast_expr = makeExpr<Analyzer::UOper>(kDOUBLE, kCAST, col_expr);
  auto minus_expr =
      makeExpr<Analyzer::UOper>(col_expr->get_type_info().get_type(), kUMINUS, col_expr);
  auto isnull_expr = makeExpr<Analyzer::UOper>(kBOOLEAN, kISNULL, col_expr);
  auto max_isnull_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kTINYINT, true), kMAX, isnull_expr, false, nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {col_expr},
                              {col_var_expr.get(),
                               cast_expr.get(),
                               minus_expr.get(),
                               max_isnull_expr.get(),
                               count_noparam_expr.get(),
                               count_expr.get(),
                               sum_expr.get(),
                               min_expr.get(),
                               max_expr.get(),
                               avg_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);

  runTest(std::string("SingleKeyScalarTypeTest-" + col_name),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, SingleKeyScalarTypeTest) {
  testScalarTypeSingleKey(MockTableForTest, "tinyint_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "tinyint_null", 4);

  testScalarTypeSingleKey(MockTableForTest, "smallint_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "smallint_null", 4);

  testScalarTypeSingleKey(MockTableForTest, "int_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "int_null", 4);

  testScalarTypeSingleKey(MockTableForTest, "bigint_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "bigint_null", 4);

  testScalarTypeSingleKey(MockTableForTest, "float_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "float_null", 4);

  testScalarTypeSingleKey(MockTableForTest, "double_notnull", 5);
  testScalarTypeSingleKey(MockTableForTest, "double_null", 4);
}

void testScalarTypeMultipleKey(MockTable* table,
                               const std::string& col1_name,
                               const std::string& col2_name,
                               size_t expect_ans_row_num) {
  // select col1, col2, COUNT(), COUNT(col1), SUM(col2), MIN(col1), MAX(col2), AVG(col1)
  // from table group by col1, col2;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col1 = table->getMetadataForColumn(col1_name);
  auto col2 = table->getMetadataForColumn(col2_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col1, col2});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col1_expr =
      makeExpr<Analyzer::ColumnVar>(col1->type, td->table_id, col1->column_id, 0);
  auto col2_expr =
      makeExpr<Analyzer::ColumnVar>(col2->type, td->table_id, col2->column_id, 0);

  auto col1_var_expr = makeExpr<Analyzer::Var>(
      col1->type, td->table_id, col1->column_id, 0, false, Analyzer::Var::kGROUPBY, 1);
  auto col2_var_expr = makeExpr<Analyzer::Var>(
      col2->type, td->table_id, col2->column_id, 0, false, Analyzer::Var::kGROUPBY, 2);

  auto count_noparam_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kBIGINT, true), kCOUNT, nullptr, false, nullptr);
  auto count_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kBIGINT, true), kCOUNT, col1_expr, false, nullptr);
  auto sum_expr = makeExpr<Analyzer::AggExpr>(
      col2_expr->get_type_info(), kSUM, col2_expr, false, nullptr);
  auto min_expr = makeExpr<Analyzer::AggExpr>(
      col1_expr->get_type_info(), kMIN, col1_expr, false, nullptr);
  auto max_expr = makeExpr<Analyzer::AggExpr>(
      col2_expr->get_type_info(), kMAX, col2_expr, false, nullptr);
  auto avg_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kDOUBLE, col1_expr->get_type_info().get_notnull()),
      kAVG,
      col1_expr,
      false,
      nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {col1_expr, col2_expr},
                              {col1_var_expr.get(),
                               col2_var_expr.get(),
                               count_noparam_expr.get(),
                               count_expr.get(),
                               sum_expr.get(),
                               min_expr.get(),
                               max_expr.get(),
                               avg_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 1);

  runTest(std::string("MultipleKeyScalarTypeTest-" + col1_name + "-" + col2_name),
          table,
          ra_exe_unit_ptr,
          {col1_name, col2_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, MultipleKeyScalarTypeTest) {
  testScalarTypeMultipleKey(MockTableForTest, "tinyint_notnull", "tinyint_null", 5);
  testScalarTypeMultipleKey(MockTableForTest, "smallint_notnull", "smallint_null", 5);
  testScalarTypeMultipleKey(MockTableForTest, "int_notnull", "int_null", 5);
  testScalarTypeMultipleKey(MockTableForTest, "bigint_notnull", "bigint_null", 5);
  testScalarTypeMultipleKey(MockTableForTest, "float_notnull", "float_null", 5);
  testScalarTypeMultipleKey(MockTableForTest, "double_notnull", "double_null", 5);
}

void testScalarTypeArithemicExpr(MockTable* table,
                                 const std::string& col1_name,
                                 const std::string& col2_name,
                                 const char* op,
                                 size_t expect_ans_row_num) {
  // select SUM(col1 + col2), SUM(col1 + col1), SUM(col2 + col2) from table
  // group by col1;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col1 = table->getMetadataForColumn(col1_name);
  auto col2 = table->getMetadataForColumn(col2_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col1, col2});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col1_expr =
      makeExpr<Analyzer::ColumnVar>(col1->type, td->table_id, col1->column_id, 0);
  auto col2_expr =
      makeExpr<Analyzer::ColumnVar>(col2->type, td->table_id, col2->column_id, 0);

  SQLOps op_type;
  switch (op[0]) {
    case 's':
      op_type = SQLOps::kMINUS;
      break;
    case 'a':
      op_type = SQLOps::kPLUS;
      break;
    case 'd':
      op_type = SQLOps::kDIVIDE;
      break;
    case 'm':
      switch (op[1]) {
        case 'u':
          op_type = SQLOps::kMULTIPLY;
          break;
        case 'o':
          op_type = SQLOps::kMODULO;
          break;
        default:
          break;
      }
    default:
      break;
  }

  auto arthi_1_2 = makeExpr<Analyzer::BinOper>(
      col1->type.get_type(), op_type, SQLQualifier::kONE, col1_expr, col2_expr);
  auto arthi_1_1 = makeExpr<Analyzer::BinOper>(
      col1->type.get_type(), op_type, SQLQualifier::kONE, col1_expr, col1_expr);
  auto arthi_2_2 = makeExpr<Analyzer::BinOper>(
      col2->type.get_type(), op_type, SQLQualifier::kONE, col2_expr, col2_expr);

  auto sum12_expr = makeExpr<Analyzer::AggExpr>(
      arthi_1_2->get_type_info(), kSUM, arthi_1_2, false, nullptr);
  auto sum11_expr = makeExpr<Analyzer::AggExpr>(
      arthi_1_1->get_type_info(), kSUM, arthi_1_1, false, nullptr);
  auto sum22_expr = makeExpr<Analyzer::AggExpr>(
      arthi_2_2->get_type_info(), kSUM, arthi_2_2, false, nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {col1_expr},
                              {sum12_expr.get(), sum11_expr.get(), sum22_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 1);

  runTest(
      std::string("ScalarTypeArithemicExpr-") + op + "-" + col1_name + "-" + col2_name,
      table,
      ra_exe_unit_ptr,
      {col1_name, col2_name},
      expect_ans_row_num,
      false,
      16384,
      0,
      null_vectors);
}

TEST_F(CiderNewDataFormatTest, ScalarTypeArithemicExpr) {
  const char* op[] = {"add", "sub", "mul", "div", "mod"};
  for (size_t i = 0; i < 5; ++i) {
    testScalarTypeArithemicExpr(
        MockTableForTest, "tinyint_notnull", "tinyint_null", op[i], 5);
    testScalarTypeArithemicExpr(
        MockTableForTest, "smallint_notnull", "smallint_null", op[i], 5);
    testScalarTypeArithemicExpr(MockTableForTest, "int_notnull", "int_null", op[i], 5);
    testScalarTypeArithemicExpr(
        MockTableForTest, "bigint_notnull", "bigint_null", op[i], 5);
    testScalarTypeArithemicExpr(
        MockTableForTest, "float_notnull", "float_null", op[i], 5);
    testScalarTypeArithemicExpr(
        MockTableForTest, "double_notnull", "double_null", op[i], 5);
  }
}

void testScalarTypeCmpExpr(MockTable* table,
                           const std::string& col1_name,
                           const std::string& col2_name,
                           const char* op,
                           size_t expect_ans_row_num) {
  // select MAX(col1 = col2), MAX(col1 = 0) from table group by col1;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col1 = table->getMetadataForColumn(col1_name);
  auto col2 = table->getMetadataForColumn(col2_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col1, col2});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col1_expr =
      makeExpr<Analyzer::ColumnVar>(col1->type, td->table_id, col1->column_id, 0);
  auto col2_expr =
      makeExpr<Analyzer::ColumnVar>(col2->type, td->table_id, col2->column_id, 0);

  SQLOps op_type;
  switch (op[0]) {
    case 'e':
      op_type = SQLOps::kEQ;
      break;
    case 'n':
      op_type = SQLOps::kNE;
      break;
    case 'l':
      switch (op[1]) {
        case 't':
          op_type = SQLOps::kLT;
          break;
        case 'e':
          op_type = SQLOps::kLE;
          break;
      }
    case 'g':
      switch (op[1]) {
        case 't':
          op_type = SQLOps::kGT;
          break;
        case 'e':
          op_type = SQLOps::kGE;
          break;
        default:
          break;
      }
    default:
      break;
  }

  auto cmp_1_2 = makeExpr<Analyzer::BinOper>(
      kBOOLEAN, op_type, SQLQualifier::kONE, col1_expr, col2_expr);
  auto sum12_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kTINYINT, false), kMAX, cmp_1_2, false, nullptr);
  Datum constant{0};
  auto constant_expr =
      makeExpr<Analyzer::Constant>(col1_expr->get_type_info(), false, constant);
  auto constant_cmp_expr = makeExpr<Analyzer::BinOper>(
      kBOOLEAN, op_type, SQLQualifier::kONE, col1_expr, constant_expr);
  auto constant_max_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kTINYINT, col1_expr->get_type_info().get_notnull()),
      kMAX,
      constant_cmp_expr,
      false,
      nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {col1_expr},
                              {sum12_expr.get(), constant_max_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 1);

  runTest(std::string("ScalarTypeCmpExpr-") + op + "-" + col1_name + "-" + col2_name,
          table,
          ra_exe_unit_ptr,
          {col1_name, col2_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, ScalarTypeCmpExpr) {
  const char* op[] = {"eq", "ne", "lt", "gt", "le", "ge"};
  for (size_t i = 0; i < 6; ++i) {
    testScalarTypeCmpExpr(MockTableForTest, "tinyint_notnull", "tinyint_null", op[i], 5);
    testScalarTypeCmpExpr(
        MockTableForTest, "smallint_notnull", "smallint_null", op[i], 5);
    testScalarTypeCmpExpr(MockTableForTest, "int_notnull", "int_null", op[i], 5);
    testScalarTypeCmpExpr(MockTableForTest, "bigint_notnull", "bigint_null", op[i], 5);
    testScalarTypeCmpExpr(MockTableForTest, "float_notnull", "float_null", op[i], 5);
    testScalarTypeCmpExpr(MockTableForTest, "double_notnull", "double_null", op[i], 5);
  }
}

void testScalarTypeLogicalExpr(MockTable* table,
                               const char* op,
                               const std::string& key_name,
                               const std::string& col1_name,
                               const std::string& col2_name,
                               size_t expect_ans_row_num) {
  // select MAX(col1 AND col2), MAX(NOT col1) from table group by key;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto key = table->getMetadataForColumn(key_name);
  auto col1 = table->getMetadataForColumn(col1_name);
  auto col2 = table->getMetadataForColumn(col2_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs(
      col1_name == col2_name ? std::vector<ColumnInfoPtr>({key, col1})
                             : std::vector<ColumnInfoPtr>({key, col1, col2}));
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto key_expr =
      makeExpr<Analyzer::ColumnVar>(key->type, td->table_id, key->column_id, 0);
  auto col1_expr =
      makeExpr<Analyzer::ColumnVar>(col1->type, td->table_id, col1->column_id, 0);
  auto col2_expr =
      makeExpr<Analyzer::ColumnVar>(col2->type, td->table_id, col2->column_id, 0);

  SQLOps op_type;
  switch (op[0]) {
    case 'a':
      op_type = SQLOps::kAND;
      break;
    case 'o':
      op_type = SQLOps::kOR;
      break;
  }

  auto logical_1_2 = makeExpr<Analyzer::BinOper>(
      kBOOLEAN, op_type, SQLQualifier::kONE, col1_expr, col2_expr);
  auto sum12_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kTINYINT, logical_1_2->get_type_info().get_notnull()),
      kMAX,
      logical_1_2,
      false,
      nullptr);
  auto logical_1 = makeExpr<Analyzer::UOper>(kBOOLEAN, SQLOps::kNOT, col1_expr);
  auto sum1_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kTINYINT, logical_1->get_type_info().get_notnull()),
      kMAX,
      logical_1,
      false,
      nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {key_expr},
                              {sum12_expr.get(), sum1_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      3, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));

  if (!col1->type.get_notnull()) {
    CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 0);
  }
  if (!col2->type.get_notnull()) {
    CiderBitUtils::clearBitAt(null_vectors[2].as<uint8_t>(), 0);
  }

  runTest(std::string("ScalarTypeLogicalExpr-") + op + "-" + col1_name + "-" + col2_name,
          table,
          ra_exe_unit_ptr,
          col1_name == col2_name
              ? std::vector<std::string>({key_name, col1_name})
              : std::vector<std::string>({key_name, col1_name, col2_name}),
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, ScalarTypeLogicalExpr) {
  const char* op[] = {"and", "or"};
  for (size_t i = 0; i < 2; ++i) {
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_notnull",
                              "bool_true_notnull",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_notnull",
                              "bool_false_notnull",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_notnull",
                              "bool_true_null",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_notnull",
                              "bool_false_null",
                              5);
    testScalarTypeLogicalExpr(
        MockTableForTest, op[i], "bigint_notnull", "bool_true_null", "bool_true_null", 5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_null",
                              "bool_false_notnull",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_true_null",
                              "bool_false_null",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_false_notnull",
                              "bool_false_notnull",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_false_notnull",
                              "bool_false_null",
                              5);
    testScalarTypeLogicalExpr(MockTableForTest,
                              op[i],
                              "bigint_notnull",
                              "bool_false_null",
                              "bool_false_null",
                              5);
  }
}

void testScalarTypeFilter(MockTable* table,
                          const std::string& col_name,
                          size_t expect_ans_row_num) {
  // select col1, COUNT() from table where not col1 is null and col1 > 0 group by col1;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col1 = table->getMetadataForColumn(col_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col1});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col1_expr =
      makeExpr<Analyzer::ColumnVar>(col1->type, td->table_id, col1->column_id, 0);

  auto col1_var_expr = makeExpr<Analyzer::Var>(
      col1->type, td->table_id, col1->column_id, 0, false, Analyzer::Var::kGROUPBY, 1);

  auto isnull_expr = makeExpr<Analyzer::UOper>(kBOOLEAN, kISNULL, col1_expr);
  auto not_isnull_expr = makeExpr<Analyzer::UOper>(kBOOLEAN, kNOT, isnull_expr);
  Datum constant{0};
  auto constant_expr =
      makeExpr<Analyzer::Constant>(col1_expr->get_type_info(), false, constant);
  auto constant_cmp_expr = makeExpr<Analyzer::BinOper>(
      kBOOLEAN, kGT, SQLQualifier::kONE, col1_expr, constant_expr);

  auto count_noparam_expr = makeExpr<Analyzer::AggExpr>(
      SQLTypeInfo(kBIGINT, true), kCOUNT, nullptr, false, nullptr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {not_isnull_expr, constant_cmp_expr},
                              {},
                              {},
                              {col1_expr},
                              {
                                  col1_var_expr.get(),
                                  count_noparam_expr.get(),
                              },
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[1].as<uint8_t>(), 1);

  runTest(std::string("FilterTest-" + col_name),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, ScalarTypeFilterTest) {
  testScalarTypeFilter(MockTableForTest, "tinyint_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "tinyint_null", 3);

  testScalarTypeFilter(MockTableForTest, "smallint_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "smallint_null", 3);

  testScalarTypeFilter(MockTableForTest, "int_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "int_null", 3);

  testScalarTypeFilter(MockTableForTest, "bigint_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "bigint_null", 3);

  testScalarTypeFilter(MockTableForTest, "float_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "float_null", 3);

  testScalarTypeFilter(MockTableForTest, "double_notnull", 4);
  testScalarTypeFilter(MockTableForTest, "double_null", 3);
}

void testScalarProject(MockTable* table,
                       const std::string& col_name,
                       size_t expect_ans_row_num) {
  // select col, -col from table;
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);

  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});
  std::vector<InputTableInfo> table_infos = {table->getInputTableInfo()};

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);

  // auto col_var_expr = makeExpr<Analyzer::Var>(
  //     col->type, td->table_id, col->column_id, 0, false, Analyzer::Var::kOUTPUT, 1);
  auto minus_expr =
      makeExpr<Analyzer::UOper>(col_expr->get_type_info().get_type(), kUMINUS, col_expr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {nullptr},
                              {col_expr.get(), minus_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);

  runTest(std::string("ScalarTypeProjectTest-" + col_name),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderNewDataFormatTest, ScalarTypeProjectTest) {
  testScalarProject(MockTableForTest, "tinyint_notnull", 5);
  testScalarProject(MockTableForTest, "tinyint_null", 5);

  testScalarProject(MockTableForTest, "smallint_notnull", 5);
  testScalarProject(MockTableForTest, "smallint_null", 5);

  testScalarProject(MockTableForTest, "int_notnull", 5);
  testScalarProject(MockTableForTest, "int_null", 5);

  testScalarProject(MockTableForTest, "bigint_notnull", 5);
  testScalarProject(MockTableForTest, "bigint_null", 5);

  testScalarProject(MockTableForTest, "float_notnull", 5);
  testScalarProject(MockTableForTest, "float_null", 5);

  testScalarProject(MockTableForTest, "double_notnull", 5);
  testScalarProject(MockTableForTest, "double_null", 5);
}

class CiderHasherTest : public ::testing::Test {};

TEST_F(CiderHasherTest, TypeTest) {
  CiderHasher range_able_hasher({SQLTypeInfo(kBOOLEAN),
                                 SQLTypeInfo(kTEXT),
                                 SQLTypeInfo(kDECIMAL),
                                 SQLTypeInfo(kSMALLINT),
                                 SQLTypeInfo(kINT),
                                 SQLTypeInfo(kBIGINT),
                                 SQLTypeInfo(kVARCHAR)},
                                8,
                                false);

  CiderHasher not_range_able_hasher({SQLTypeInfo(kBOOLEAN),
                                     SQLTypeInfo(kTEXT),
                                     SQLTypeInfo(kDECIMAL),
                                     SQLTypeInfo(kSMALLINT),
                                     SQLTypeInfo(kDOUBLE),
                                     SQLTypeInfo(kINT),
                                     SQLTypeInfo(kBIGINT),
                                     SQLTypeInfo(kVARCHAR)},
                                    8,
                                    false);

  EXPECT_EQ(range_able_hasher.getHashMode(), CiderHasher::kRangeHash);
  EXPECT_EQ(not_range_able_hasher.getHashMode(), CiderHasher::kDirectHash);
}

TEST_F(CiderHasherTest, RangeHash2RangeHashTest) {
  CiderHasher hasher(
      {SQLTypeInfo(kBOOLEAN), SQLTypeInfo(kINT), SQLTypeInfo(kBIGINT)}, 8, false);
  int64_t keys[3]{0};

  keys[0] = true;
  keys[1] = 10;
  keys[2] = 100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = inline_int_null_value<int8_t>();
  keys[1] = -10;
  keys[2] = inline_int_null_value<int64_t>();

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = false;
  keys[1] = inline_int_null_value<int32_t>();
  keys[2] = -100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  auto& key_range_info = hasher.getKeyColumnInfo();

  EXPECT_TRUE(key_range_info[0].need_rehash);
  EXPECT_TRUE(key_range_info[1].need_rehash);
  EXPECT_TRUE(key_range_info[2].need_rehash);

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -10);
  EXPECT_EQ(key_range_info[1].max, 10);

  EXPECT_EQ(key_range_info[2].min, -100);
  EXPECT_EQ(key_range_info[2].max, 100);

  auto entry_num = hasher.updateHashMode(0, 10000000000);
  EXPECT_EQ(entry_num, 28992);
  EXPECT_FALSE(hasher.needRehash());

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -15);
  EXPECT_EQ(key_range_info[1].max, 15);

  EXPECT_EQ(key_range_info[2].min, -150);
  EXPECT_EQ(key_range_info[2].max, 150);

  std::vector<int64_t> v0{inline_int_null_value<int8_t>(), 0, 1},
      v1{inline_int_null_value<int32_t>()}, v2{inline_int_null_value<int64_t>()};

  for (int64_t i = -15; i <= 15; ++i) {
    v1.push_back(i);
  }
  for (int64_t i = -150; i <= 150; ++i) {
    v2.push_back(i);
  }

  int64_t expected_hash = 0;
  for (auto i2 : v2) {
    for (auto i1 : v1) {
      for (auto i0 : v0) {
        keys[0] = i0;
        keys[1] = i1;
        keys[2] = i2;
        EXPECT_EQ(hasher.hash<int64_t>(keys), expected_hash);
        EXPECT_FALSE(hasher.needRehash());
        ++expected_hash;
      }
    }
  }
}

TEST_F(CiderHasherTest, RangeHash2RangeHashCiderFormatTest) {
  CiderHasher hasher(
      {SQLTypeInfo(kBOOLEAN), SQLTypeInfo(kINT), SQLTypeInfo(kBIGINT)}, 8, true);
  int64_t keys[4]{0};

  keys[0] = true;
  keys[1] = 10;
  keys[2] = 100;
  keys[3] = 0x7FFFFFFFFFFFFFFF;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 0);
  keys[1] = -10;
  keys[2] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 2);

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());
  keys[3] = 0x7FFFFFFFFFFFFFFF;

  keys[0] = false;
  keys[1] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 1);
  keys[2] = -100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  auto& key_range_info = hasher.getKeyColumnInfo();

  EXPECT_TRUE(key_range_info[0].need_rehash);
  EXPECT_TRUE(key_range_info[1].need_rehash);
  EXPECT_TRUE(key_range_info[2].need_rehash);

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -10);
  EXPECT_EQ(key_range_info[1].max, 10);

  EXPECT_EQ(key_range_info[2].min, -100);
  EXPECT_EQ(key_range_info[2].max, 100);

  auto entry_num = hasher.updateHashMode(0, 10000000000);
  EXPECT_EQ(entry_num, 28992);
  EXPECT_FALSE(hasher.needRehash());

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -15);
  EXPECT_EQ(key_range_info[1].max, 15);

  EXPECT_EQ(key_range_info[2].min, -150);
  EXPECT_EQ(key_range_info[2].max, 150);

  std::vector<int64_t> v0{inline_int_null_value<int8_t>(), 0, 1},
      v1{inline_int_null_value<int32_t>()}, v2{inline_int_null_value<int64_t>()};

  for (int64_t i = -15; i <= 15; ++i) {
    v1.push_back(i);
  }
  for (int64_t i = -150; i <= 150; ++i) {
    v2.push_back(i);
  }

  int64_t expected_hash = 0;
  for (auto i2 : v2) {
    for (auto i1 : v1) {
      for (auto i0 : v0) {
        keys[0] = i0;
        keys[1] = i1;
        keys[2] = i2;
        keys[3] = 0x7FFFFFFFFFFFFFFF;

        if (inline_int_null_value<int8_t>() == i0) {
          keys[0] = 0;
          CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 0);
        }

        if (inline_int_null_value<int32_t>() == i1) {
          keys[1] = 0;
          CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 1);
        }

        if (inline_int_null_value<int64_t>() == i2) {
          keys[2] = 0;
          CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 2);
        }

        EXPECT_EQ(hasher.hash<int64_t>(keys), expected_hash);
        EXPECT_FALSE(hasher.needRehash());
        ++expected_hash;
      }
    }
  }
}

void runRangeHash2DirectHashCiderFormatTest(const uint64_t entry_limit) {
  CiderHasher hasher(
      {SQLTypeInfo(kBOOLEAN), SQLTypeInfo(kINT), SQLTypeInfo(kBIGINT)}, 8, true);
  int64_t keys[4]{0};

  keys[0] = true;
  keys[1] = 10;
  keys[2] = 100;
  keys[3] = 0x7FFFFFFFFFFFFFFF;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 0);
  keys[1] = -10;
  keys[2] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 2);

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());
  keys[3] = 0x7FFFFFFFFFFFFFFF;

  keys[0] = false;
  keys[1] = 0;
  CiderBitUtils::clearBitAt(reinterpret_cast<uint8_t*>(&keys[3]), 1);
  keys[2] = -100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());
  keys[3] = 0x7FFFFFFFFFFFFFFF;

  auto& key_range_info = hasher.getKeyColumnInfo();

  EXPECT_TRUE(key_range_info[0].need_rehash);
  EXPECT_TRUE(key_range_info[1].need_rehash);
  EXPECT_TRUE(key_range_info[2].need_rehash);

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -10);
  EXPECT_EQ(key_range_info[1].max, 10);

  EXPECT_EQ(key_range_info[2].min, -100);
  EXPECT_EQ(key_range_info[2].max, 100);

  auto entry_num = hasher.updateHashMode(0, entry_limit);
  EXPECT_EQ(entry_num, entry_limit);
  EXPECT_FALSE(hasher.needRehash());
  EXPECT_EQ(hasher.getHashMode(), CiderHasher::kDirectHash);
}

TEST_F(CiderHasherTest, RangeHash2DirectHashCiderFormatTest) {
  runRangeHash2DirectHashCiderFormatTest(2);
  runRangeHash2DirectHashCiderFormatTest(20);
  runRangeHash2DirectHashCiderFormatTest(200);
  runRangeHash2DirectHashCiderFormatTest(28000);
}

void runRangeHash2DirectHashTest(const uint64_t entry_limit) {
  CiderHasher hasher(
      {SQLTypeInfo(kBOOLEAN), SQLTypeInfo(kINT), SQLTypeInfo(kBIGINT)}, 8, false);
  int64_t keys[3]{0};

  keys[0] = true;
  keys[1] = 10;
  keys[2] = 100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = inline_int_null_value<int8_t>();
  keys[1] = -10;
  keys[2] = inline_int_null_value<int64_t>();

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  keys[0] = false;
  keys[1] = inline_int_null_value<int32_t>();
  keys[2] = -100;

  hasher.hash<int64_t>(keys);
  EXPECT_TRUE(hasher.needRehash());

  auto& key_range_info = hasher.getKeyColumnInfo();

  EXPECT_TRUE(key_range_info[0].need_rehash);
  EXPECT_TRUE(key_range_info[1].need_rehash);
  EXPECT_TRUE(key_range_info[2].need_rehash);

  EXPECT_EQ(key_range_info[0].min, 0);
  EXPECT_EQ(key_range_info[0].max, 1);

  EXPECT_EQ(key_range_info[1].min, -10);
  EXPECT_EQ(key_range_info[1].max, 10);

  EXPECT_EQ(key_range_info[2].min, -100);
  EXPECT_EQ(key_range_info[2].max, 100);

  auto entry_num = hasher.updateHashMode(0, entry_limit);
  EXPECT_EQ(entry_num, entry_limit);
  EXPECT_FALSE(hasher.needRehash());
  EXPECT_EQ(hasher.getHashMode(), CiderHasher::kDirectHash);
}

TEST_F(CiderHasherTest, RangeHash2DirectHashTest) {
  runRangeHash2DirectHashTest(2);
  runRangeHash2DirectHashTest(20);
  runRangeHash2DirectHashTest(200);
  runRangeHash2DirectHashTest(28000);
}

bool checkByteArrayEq(CiderByteArray cba1, CiderByteArray cba2) {
  return cba1.len == cba2.len && !std::memcmp(cba1.ptr, cba2.ptr, cba1.len);
}

TEST_F(CiderHasherTest, StringUidConvertTest) {
  CiderHasher hasher({SQLTypeInfo(kVARCHAR)}, 8, false);

  CiderByteArray str1(1, reinterpret_cast<const uint8_t*>("1"));
  int64_t id1 = hasher.lookupIdByValue(str1);
  CiderByteArray str1_res = hasher.lookupValueById(id1);
  EXPECT_TRUE(checkByteArrayEq(str1, str1_res));

  CiderByteArray str2(2, reinterpret_cast<const uint8_t*>("22"));
  int64_t id2 = hasher.lookupIdByValue(str2);
  CiderByteArray str2_res = hasher.lookupValueById(id2);
  EXPECT_TRUE(checkByteArrayEq(str2, str2_res));

  CiderByteArray str3(3, reinterpret_cast<const uint8_t*>("333"));
  int64_t id3 = hasher.lookupIdByValue(str3);
  CiderByteArray str3_res = hasher.lookupValueById(id3);
  EXPECT_TRUE(checkByteArrayEq(str3, str3_res));

  CiderByteArray str20(20, reinterpret_cast<const uint8_t*>("12abc_aaaa_bbbb_cccc"));
  int64_t id20 = hasher.lookupIdByValue(str20);
  CiderByteArray str20_res = hasher.lookupValueById(id20);
  EXPECT_TRUE(checkByteArrayEq(str20, str20_res));

  CiderByteArray str27(27,
                       reinterpret_cast<const uint8_t*>("15abcde_aaa_bbb_ccc_ddd_eee"));
  int64_t id27 = hasher.lookupIdByValue(str27);
  CiderByteArray str27_res = hasher.lookupValueById(id27);
  EXPECT_TRUE(checkByteArrayEq(str27, str27_res));

  CiderByteArray str1_2(1, reinterpret_cast<const uint8_t*>("1"));
  int64_t id1_2 = hasher.lookupIdByValue(str1_2);
  CiderByteArray str1_2_res = hasher.lookupValueById(id1_2);
  EXPECT_TRUE(id1 == id1_2);
  EXPECT_TRUE(checkByteArrayEq(str1, str1_2_res));
  EXPECT_TRUE(checkByteArrayEq(str1_2, str1_2_res));

  CiderByteArray str20_2(20, reinterpret_cast<const uint8_t*>("12abc_aaaa_bbbb_cccc"));
  int64_t id20_2 = hasher.lookupIdByValue(str20_2);
  CiderByteArray str20_2_res = hasher.lookupValueById(id20_2);
  EXPECT_TRUE(id20 == id20_2);
  EXPECT_TRUE(checkByteArrayEq(str20, str20_2_res));
  EXPECT_TRUE(checkByteArrayEq(str20_2, str20_2_res));

  EXPECT_TRUE(checkByteArrayEq(str1, str1_res));
  EXPECT_TRUE(checkByteArrayEq(str2, str2_res));
  EXPECT_TRUE(checkByteArrayEq(str3, str3_res));
  EXPECT_TRUE(checkByteArrayEq(str20, str20_res));
  EXPECT_TRUE(checkByteArrayEq(str27, str27_res));
}

int main(int argc, char** argv) {
  g_is_test_env = true;

  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  namespace po = boost::program_options;

  po::options_description desc("Options");

  logger::LogOptions log_options(argv[0]);
  log_options.max_files_ = 0;  // stderr only by default
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
