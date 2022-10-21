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
#include <cstddef>
#include <string>
#include "function/datetime/DateAdd.h"
#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"
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

size_t ELEMENT_NUM = 5;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderDateFunctionTest : public ::testing::Test {
 protected:
  static MockTable* MockTableForTest;

  static void SetUpTestSuite() {
    if (nullptr == MockTableForTest) {
      std::vector<std::string> col_names = {"date_notnull", "date_null"};

      std::vector<SQLTypeInfo> col_types = {
          SQLTypeInfo(kBIGINT, true),
          SQLTypeInfo(kBIGINT, false),
      };

      int64_t* bigint_ptr = new int64_t[ELEMENT_NUM];

      for (size_t i = 0; i < ELEMENT_NUM; ++i) {
        bigint_ptr[i] = i * 86400 + i * 3600 + i * 60 + i;
      }

      std::vector<int8_t*> col_datas = {reinterpret_cast<int8_t*>(bigint_ptr),
                                        reinterpret_cast<int8_t*>(bigint_ptr)};

      MockTableForTest = new MockTable(col_names, col_types, col_datas, ELEMENT_NUM);
    }
  }

  static void TearDownTestSuite() {
    delete MockTableForTest;
    MockTableForTest = nullptr;
  }
};

MockTable* CiderDateFunctionTest::MockTableForTest = nullptr;

void testDateAdd(MockTable* table,
                 const std::string& col_name,
                 DateaddField field,
                 size_t expect_ans_row_num = ELEMENT_NUM) {
  // SELECT col_b + interval '1' year/month/day/hour/minute/second FROM table
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);
  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);
  SQLTypes types = kINTERVAL_DAY_TIME;
  if (field == DateaddField::daYEAR || field == DateaddField::daMONTH) {
    types = kINTERVAL_YEAR_MONTH;
  }
  auto constant_expr = makeExpr<Analyzer::Constant>(
      SQLTypeInfo(types, true), false, Datum{.bigintval = 1});
  auto data_add_expr = makeExpr<Analyzer::DateaddExpr>(
      SQLTypeInfo(kDATE, col_expr->get_type_info().get_notnull()),
      field,
      constant_expr,
      col_expr);

  auto ra_exe_unit_ptr =
      std::shared_ptr<RelAlgExecutionUnit>(new RelAlgExecutionUnit{input_descs,
                                                                   input_col_descs,
                                                                   {},
                                                                   {},
                                                                   {},
                                                                   {nullptr},
                                                                   {data_add_expr.get()},
                                                                   nullptr,
                                                                   SortInfo{},
                                                                   0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);

  runTest(std::string("DateAddTest-" + col_name + "-" + std::to_string(field)),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderDateFunctionTest, DateAddTest) {
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daYEAR);
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daMONTH);
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daDAY);
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daHOUR);
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daMINUTE);
  testDateAdd(MockTableForTest, "date_notnull", DateaddField::daSECOND);

  testDateAdd(MockTableForTest, "date_null", DateaddField::daSECOND);
}

void testDateDiff(MockTable* table,
                  const std::string& col_name,
                  DatetruncField field,
                  size_t expect_ans_row_num = ELEMENT_NUM) {
  // SELECT date_diff('year/month/day/hour/minute/second',col_a,'1970-01-03') from test
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);
  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);
  auto constant_expr = makeExpr<Analyzer::Constant>(
      SQLTypeInfo(kINTERVAL_DAY_TIME, true), false, Datum{.bigintval = 86400 * 3});
  auto data_diff_expr = makeExpr<Analyzer::DatediffExpr>(
      SQLTypeInfo(kDATE, col_expr->get_type_info().get_notnull()),
      field,
      col_expr,
      constant_expr);

  auto ra_exe_unit_ptr =
      std::shared_ptr<RelAlgExecutionUnit>(new RelAlgExecutionUnit{input_descs,
                                                                   input_col_descs,
                                                                   {},
                                                                   {},
                                                                   {},
                                                                   {nullptr},
                                                                   {data_diff_expr.get()},
                                                                   nullptr,
                                                                   SortInfo{},
                                                                   0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);

  runTest(std::string("DateDiffTest-" + col_name),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderDateFunctionTest, DateDiffTest) {
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtYEAR);
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtMONTH);
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtDAY);
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtHOUR);
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtMINUTE);
  testDateDiff(MockTableForTest, "date_notnull", DatetruncField::dtSECOND);

  testDateDiff(MockTableForTest, "date_null", DatetruncField::dtDAY);
}

void testDateTrunc(MockTable* table,
                   const std::string& col_name,
                   DatetruncField field,
                   size_t expect_ans_row_num = ELEMENT_NUM) {
  // SELECT date_trunc('year/month/day/hour/minute/second', col_a) from test
  CHECK(table);

  auto td = table->getMetadataForTable();
  auto col = table->getMetadataForColumn(col_name);
  auto input_descs = std::vector<InputDescriptor>{InputDescriptor(td->table_id, 0)};
  auto input_col_descs = table->getInputColDescs({col});

  auto col_expr =
      makeExpr<Analyzer::ColumnVar>(col->type, td->table_id, col->column_id, 0);

  auto data_trunc_expr = makeExpr<Analyzer::DatetruncExpr>(
      SQLTypeInfo(kDATE, col_expr->get_type_info().get_notnull()),
      false,
      field,
      col_expr);

  auto ra_exe_unit_ptr = std::shared_ptr<RelAlgExecutionUnit>(
      new RelAlgExecutionUnit{input_descs,
                              input_col_descs,
                              {},
                              {},
                              {},
                              {nullptr},
                              {data_trunc_expr.get()},
                              nullptr,
                              SortInfo{},
                              0});

  std::vector<CiderBitUtils::CiderBitVector<>> null_vectors(
      2, CiderBitUtils::CiderBitVector<>(allocator, 5, 0xFF));
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 0);
  CiderBitUtils::clearBitAt(null_vectors[0].as<uint8_t>(), 1);

  runTest(std::string("DateTruncTest-" + col_name),
          table,
          ra_exe_unit_ptr,
          {col_name},
          expect_ans_row_num,
          false,
          16384,
          0,
          null_vectors);
}

TEST_F(CiderDateFunctionTest, DateTruncTest) {
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtYEAR);
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtMONTH);
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtDAY);
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtHOUR);
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtMINUTE);
  testDateTrunc(MockTableForTest, "date_notnull", DatetruncField::dtSECOND);

  testDateTrunc(MockTableForTest, "date_null", DatetruncField::dtDAY);
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
