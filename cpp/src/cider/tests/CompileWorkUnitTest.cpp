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

#include "TestHelpers.h"

#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"
#include "exec/template/CgenState.h"
#include "exec/template/Execute.h"
#include "exec/template/ExpressionRange.h"
#include "exec/template/RelAlgExecutionUnit.h"

#include <gtest/gtest.h>

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

RelAlgExecutionUnit buildFakeAggRelAlgEU() {
  int db_id = 100;
  int table_id = 100;
  int column_id_0 = 1;
  int column_id_1 = 2;

  // input_descs
  std::vector<InputDescriptor> input_descs;
  InputDescriptor input_desc_0(table_id, 0);
  input_descs.push_back(input_desc_0);

  SQLTypes sqlTypes{SQLTypes::kBOOLEAN};
  SQLTypes subtypes{SQLTypes::kNULLT};
  SQLTypes dateSqlType{SQLTypes::kDATE};
  SQLTypes longTypes{SQLTypes::kBIGINT};

  SQLTypeInfo date_col_info(
      dateSqlType, 0, 0, false, EncodingType::kENCODING_DATE_IN_DAYS, 0, subtypes);
  std::shared_ptr<Analyzer::ColumnVar> col1 =
      std::make_shared<Analyzer::ColumnVar>(date_col_info, table_id, column_id_0, 0);
  SQLTypeInfo ti_boolean(
      sqlTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);
  SQLTypeInfo ti_long(longTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

  // input_col_descs
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  std::shared_ptr<const InputColDescriptor> input_col_desc_0 =
      std::make_shared<const InputColDescriptor>(
          std::make_shared<ColumnInfo>(
              db_id, table_id, column_id_0, "col_0", date_col_info, false),
          0);
  std::shared_ptr<const InputColDescriptor> input_col_desc_1 =
      std::make_shared<const InputColDescriptor>(
          std::make_shared<ColumnInfo>(
              db_id, table_id, column_id_1, "col_1", ti_long, false),
          0);
  input_col_descs.push_back(input_col_desc_0);
  input_col_descs.push_back(input_col_desc_1);

  // simple_quals

  std::shared_ptr<Analyzer::Expr> leftExpr =
      std::make_shared<Analyzer::UOper>(date_col_info, false, SQLOps::kCAST, col1);
  Datum d;
  d.intval = 8766;
  std::shared_ptr<Analyzer::Expr> rightExpr =
      std::make_shared<Analyzer::Constant>(dateSqlType, false, d);
  std::shared_ptr<Analyzer::Expr> simple_qual_0 = std::make_shared<Analyzer::BinOper>(
      ti_boolean, false, SQLOps::kGE, SQLQualifier::kONE, leftExpr, rightExpr);
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  simple_quals.push_back(simple_qual_0);

  std::vector<Analyzer::Expr*> target_exprs;
  std::shared_ptr<Analyzer::ColumnVar> col_expr_0 =
      std::make_shared<Analyzer::ColumnVar>(ti_long, table_id, column_id_1, 0);
  Analyzer::AggExpr* target_expr_0 =
      new Analyzer::AggExpr(ti_long, kSUM, col_expr_0, false, nullptr);

  target_exprs.push_back(target_expr_0);

  // notice! size should be 0
  std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  //  groupby_exprs.push_back(nullptr);

  //   ra_exe_unit.input_descs = input_descs;
  //   ra_exe_unit.input_col_descs = input_col_descs;
  //   ra_exe_unit.simple_quals = simple_quals;
  //   ra_exe_unit.groupby_exprs = groupby_exprs;
  //   ra_exe_unit.target_exprs = target_exprs;

  RelAlgExecutionUnit ra_exe_unit{input_descs,
                                  input_col_descs,
                                  simple_quals,
                                  std::list<std::shared_ptr<Analyzer::Expr>>(),
                                  JoinQualsPerNestingLevel(),
                                  groupby_exprs,
                                  target_exprs};

  return ra_exe_unit;
}

// build parameters, for test only.
std::vector<InputTableInfo> buildQueryInfo() {
  std::vector<InputTableInfo> query_infos;
  Fragmenter_Namespace::FragmentInfo fi_0;
  fi_0.fragmentId = 0;
  fi_0.shadowNumTuples = 20;
  fi_0.physicalTableId = 100;
  fi_0.setPhysicalNumTuples(20);

  Fragmenter_Namespace::TableInfo ti_0;
  ti_0.fragments = {fi_0};
  ti_0.setPhysicalNumTuples(20);

  InputTableInfo iti_0{1, 100, ti_0};
  query_infos.push_back(iti_0);

  return query_infos;
}

TEST(APITest, case1) {
  auto cider_compile_module =
      CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());

  // build compile input parameters
  RelAlgExecutionUnit ra_exe_unit = buildFakeAggRelAlgEU();
  std::vector<InputTableInfo> query_infos = buildQueryInfo();

  auto ceo = CiderExecutionOption::defaults();
  auto cco = CiderCompilationOption::defaults();
  auto compile_result = cider_compile_module->compile(
      &ra_exe_unit, &query_infos, std::make_shared<CiderTableSchema>(), cco, ceo);

  // build data input parameters
  std::vector<const int8_t*> input_buf;
  int64_t num_rows = 10;
  std::vector<int32_t> date_col_id_0(num_rows);
  std::vector<int64_t> long_col_id_1(num_rows);
  for (int i = 0; i < 5; i++) {
    date_col_id_0[i] = 8777;
    long_col_id_1[i] = i;
  }

  for (int i = 5; i < 10; i++) {
    date_col_id_0[i] = 8700;
    long_col_id_1[i] = i;
  }

  input_buf.push_back(reinterpret_cast<int8_t*>(date_col_id_0.data()));
  input_buf.push_back(reinterpret_cast<int8_t*>(long_col_id_1.data()));

  CiderBatch input_batch(num_rows, input_buf);
  CiderRuntimeModule cider_runtime_module(compile_result);
  cider_runtime_module.processNextBatch(input_batch);
  auto [_, output_batch] = cider_runtime_module.fetchResults();

  // should be 10(0 + 1 + 2 + 3 + 4)
  auto res_ptr = output_batch->column(0);
  int64_t res = reinterpret_cast<const int64_t*>(res_ptr)[0];
  std::cout << "agg result: " << res << std::endl;
  EXPECT_EQ(res, 10);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
