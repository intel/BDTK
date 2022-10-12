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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

RelAlgExecutionUnit buildFakeRelAlgEU(SQLTypes coltype, Datum d) {
  int db_id = 100;
  int table_id = 100;
  int column_id_0 = 1;
  int column_id_1 = 2;
  int column_id_2 = 3;
  int column_id_3 = 4;

  // input_descs
  std::vector<InputDescriptor> input_descs;
  InputDescriptor input_desc_0(table_id, 0);
  input_descs.push_back(input_desc_0);

  // simple_quals
  SQLTypes sqlTypes{SQLTypes::kBOOLEAN};
  SQLTypes subtypes{SQLTypes::kNULLT};

  SQLTypeInfo col_info(coltype, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);
  std::shared_ptr<Analyzer::ColumnVar> col1 =
      std::make_shared<Analyzer::ColumnVar>(col_info, table_id, column_id_0, 0);
  SQLTypeInfo ti_boolean(
      sqlTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

  // input_col_descs
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  input_col_descs.push_back(std::make_shared<const InputColDescriptor>(
      std::make_shared<ColumnInfo>(
          db_id, table_id, column_id_0, "col_0", col_info, false),
      0));
  input_col_descs.push_back(std::make_shared<const InputColDescriptor>(
      std::make_shared<ColumnInfo>(
          db_id, table_id, column_id_1, "col_1", col_info, false),
      0));
  input_col_descs.push_back(std::make_shared<const InputColDescriptor>(
      std::make_shared<ColumnInfo>(
          db_id, table_id, column_id_2, "col_2", col_info, false),
      0));
  input_col_descs.push_back(std::make_shared<const InputColDescriptor>(
      std::make_shared<ColumnInfo>(
          db_id, table_id, column_id_3, "col_3", col_info, false),
      0));

  std::shared_ptr<Analyzer::Expr> colExpr =
      std::make_shared<Analyzer::ColumnVar>(col_info, 100, 1, 0);

  std::shared_ptr<Analyzer::Expr> filterExpr =
      std::make_shared<Analyzer::Constant>(col_info, false, d);

  std::shared_ptr<Analyzer::Expr> simple_qual_0 = std::make_shared<Analyzer::BinOper>(
      ti_boolean, false, SQLOps::kLT, SQLQualifier::kONE, colExpr, filterExpr);
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  simple_quals.push_back(simple_qual_0);

  std::vector<Analyzer::Expr*> target_exprs;
  target_exprs.push_back(new Analyzer::ColumnVar(col_info, table_id, column_id_0, 0));
  target_exprs.push_back(new Analyzer::ColumnVar(col_info, table_id, column_id_1, 0));
  target_exprs.push_back(new Analyzer::ColumnVar(col_info, table_id, column_id_2, 0));
  target_exprs.push_back(new Analyzer::ColumnVar(col_info, table_id, column_id_3, 0));

  std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  groupby_exprs.push_back(nullptr);

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
  const int numTuples = 40;

  fi_0.fragmentId = 0;
  fi_0.shadowNumTuples = numTuples;
  fi_0.physicalTableId = 100;
  fi_0.setPhysicalNumTuples(numTuples);

  Fragmenter_Namespace::TableInfo ti_0;
  ti_0.fragments = {fi_0};
  ti_0.setPhysicalNumTuples(numTuples);

  InputTableInfo iti_0{100, 100, Fragmenter_Namespace::TableInfo()};
  query_infos.push_back(iti_0);

  return query_infos;
}

std::shared_ptr<CiderTableSchema> buildTableSchema(std::string type_json) {
  std::vector<std::string> col_names = {"col_0", "col_1", "col_2", "col_3"};
  ::substrait::Type col_type;
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  std::vector<::substrait::Type> col_types = {col_type, col_type, col_type, col_type};
  return std::make_shared<CiderTableSchema>(col_names, col_types);
}

TEST(CiderRuntimeModuleTest, processInt) {
  auto ciderCompileModule = CiderCompileModule::Make(allocator);

  // build compile input parameters
  SQLTypes intType{SQLTypes::kINT};
  Datum d;
  d.intval = 5;
  RelAlgExecutionUnit ra_exe_unit = buildFakeRelAlgEU(intType, d);
  std::vector<InputTableInfo> query_infos = buildQueryInfo();
  // prepare table schema
  std::string type_json = R"(
    {
      "i32": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  auto schema = buildTableSchema(type_json);

  auto cco = CiderCompilationOption::defaults();
  cco.use_default_col_range = false;
  auto result =
      ciderCompileModule->compile((void*)&ra_exe_unit, (void*)&query_infos, schema, cco);

  auto ciderRuntimeModule = std::make_shared<CiderRuntimeModule>(result);

  int rows = 10;

  std::vector<const int8_t*> input_buffers;
  std::vector<int32_t> col_id_0(rows);
  std::vector<int32_t> col_id_1(rows);
  std::vector<int32_t> col_id_2(rows);
  std::vector<int32_t> col_id_3(rows);
  for (int i = 0; i < 10; i++) {
    col_id_0[i] = i;
    col_id_1[i] = i * 10;
    col_id_2[i] = i - 1;
    col_id_3[i] = i * i;
  }
  input_buffers.push_back(reinterpret_cast<int8_t*>(col_id_0.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(col_id_1.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(col_id_2.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(col_id_3.data()));

  CiderBatch inBatch(rows, input_buffers);
  ciderRuntimeModule->processNextBatch(inBatch);
  auto [_, outBatch] = ciderRuntimeModule->fetchResults();

  std::vector<int32_t*> output_targets;
  std::vector<int32_t> out_col0{0, 1, 2, 3, 4};
  std::vector<int32_t> out_col1{0, 10, 20, 30, 40};
  std::vector<int32_t> out_col2{-1, 0, 1, 2, 3};
  std::vector<int32_t> out_col3{0, 1, 4, 9, 16};
  output_targets.push_back(out_col0.data());
  output_targets.push_back(out_col1.data());
  output_targets.push_back(out_col2.data());
  output_targets.push_back(out_col3.data());

  for (int i = 0; i < outBatch->column_num(); i++) {
    const int8_t* col = outBatch->column(i);
    int32_t* out_target = output_targets[i];
    const int32_t* out = reinterpret_cast<const int32_t*>(col);
    for (int j = 0; j < outBatch->row_num(); j++) {
      EXPECT_EQ(out_target[j], out[j]);
    }
  }
}

TEST(CiderRuntimeModuleTest, processDouble) {
  auto ciderCompileModule = CiderCompileModule::Make(allocator);

  // build compile input parameters
  SQLTypes doubleType{SQLTypes::kDOUBLE};
  Datum d;
  d.doubleval = 8765.4;
  RelAlgExecutionUnit ra_exe_unit = buildFakeRelAlgEU(doubleType, d);
  std::vector<InputTableInfo> query_infos = buildQueryInfo();
  // prepare table schema
  std::string type_json = R"(
    {
      "fp64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  auto schema = buildTableSchema(type_json);
  auto cco = CiderCompilationOption::defaults();
  cco.use_default_col_range = false;
  auto result =
      ciderCompileModule->compile((void*)&ra_exe_unit, (void*)&query_infos, schema, cco);

  auto ciderRuntimeModule = std::make_shared<CiderRuntimeModule>(result);

  int rows = 10;
  std::vector<const int8_t*> input_buffers;
  std::vector<double> date_col_id_0(rows);
  std::vector<double> long_col_id_1(rows);
  std::vector<double> date_col_id_2(rows);
  std::vector<double> long_col_id_3(rows);
  for (int i = 0; i < 5; i++) {
    date_col_id_0[i] = 8777.1234;
    long_col_id_1[i] = i;
    date_col_id_2[i] = 8777.343;
    long_col_id_3[i] = i;
  }
  for (int i = 5; i < 10; i++) {
    date_col_id_0[i] = 8700.1234;
    long_col_id_1[i] = i;
    date_col_id_2[i] = 8700.343;
    long_col_id_3[i] = i;
  }
  input_buffers.push_back(reinterpret_cast<int8_t*>(date_col_id_0.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(long_col_id_1.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(date_col_id_2.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(long_col_id_3.data()));

  CiderBatch inBatch(rows, input_buffers);
  ciderRuntimeModule->processNextBatch(inBatch);
  auto [_, outBatch] = ciderRuntimeModule->fetchResults();

  std::vector<double*> output_targets;
  std::vector<double> out_col0{8700.1234, 8700.1234, 8700.1234, 8700.1234, 8700.1234};
  std::vector<double> out_col1{5, 6, 7, 8, 9};
  std::vector<double> out_col2{8700.343, 8700.343, 8700.343, 8700.343, 8700.343};
  std::vector<double> out_col3{5, 6, 7, 8, 9};
  output_targets.push_back(out_col0.data());
  output_targets.push_back(out_col1.data());
  output_targets.push_back(out_col2.data());
  output_targets.push_back(out_col3.data());

  for (int i = 0; i < outBatch->column_num(); i++) {
    const int8_t* col = outBatch->column(i);
    double* out_target = output_targets[i];
    const double* out = reinterpret_cast<const double*>(col);
    for (int j = 0; j < outBatch->row_num(); j++) {
      EXPECT_EQ(out_target[j], out[j]);
    }
  }
}

TEST(CiderRuntimeModuleTest, processMultiple) {
  auto ciderCompileModule = CiderCompileModule::Make(allocator);

  // build compile input parameters
  SQLTypes doubleType{SQLTypes::kDOUBLE};
  Datum d;
  d.doubleval = 8765.4;
  RelAlgExecutionUnit ra_exe_unit = buildFakeRelAlgEU(doubleType, d);
  std::vector<InputTableInfo> query_infos = buildQueryInfo();
  // prepare table schema
  std::string type_json = R"(
    {
      "fp64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  auto schema = buildTableSchema(type_json);
  auto cco = CiderCompilationOption::defaults();
  cco.use_default_col_range = false;
  auto result =
      ciderCompileModule->compile((void*)&ra_exe_unit, (void*)&query_infos, schema, cco);

  auto ciderRuntimeModule = std::make_shared<CiderRuntimeModule>(result);

  int rows = 10;

  std::vector<const int8_t*> input_buffers;
  std::vector<double> date_col_id_0(rows);
  std::vector<double> long_col_id_1(rows);
  std::vector<double> date_col_id_2(rows);
  std::vector<double> long_col_id_3(rows);
  for (int i = 0; i < 5; i++) {
    date_col_id_0[i] = 8777.1234;
    long_col_id_1[i] = i;
    date_col_id_2[i] = 8777.343;
    long_col_id_3[i] = i;
  }
  for (int i = 5; i < 10; i++) {
    date_col_id_0[i] = 8700.1234;
    long_col_id_1[i] = i;
    date_col_id_2[i] = 8700.343;
    long_col_id_3[i] = i;
  }
  input_buffers.push_back(reinterpret_cast<int8_t*>(date_col_id_0.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(long_col_id_1.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(date_col_id_2.data()));
  input_buffers.push_back(reinterpret_cast<int8_t*>(long_col_id_3.data()));

  CiderBatch inBatch(rows, input_buffers);

  std::vector<double*> output_targets;
  std::vector<double> out_col0{8700.1234, 8700.1234, 8700.1234, 8700.1234, 8700.1234};
  std::vector<double> out_col1{5, 6, 7, 8, 9};
  std::vector<double> out_col2{8700.343, 8700.343, 8700.343, 8700.343, 8700.343};
  std::vector<double> out_col3{5, 6, 7, 8, 9};
  output_targets.push_back(out_col0.data());
  output_targets.push_back(out_col1.data());
  output_targets.push_back(out_col2.data());
  output_targets.push_back(out_col3.data());

  int batch_nums = 5;
  for (int i = 0; i < batch_nums; i++) {
    ciderRuntimeModule->processNextBatch(inBatch);
    auto [_, outBatch] = ciderRuntimeModule->fetchResults();
    EXPECT_EQ(outBatch->row_num(), 5);

    for (int i = 0; i < outBatch->column_num(); i++) {
      const int8_t* col = outBatch->column(i);
      double* out_target = output_targets[i];
      const double* out = reinterpret_cast<const double*>(col);
      for (int j = 0; j < outBatch->row_num(); j++) {
        EXPECT_EQ(out_target[j], out[j]);
      }
    }
  }
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
