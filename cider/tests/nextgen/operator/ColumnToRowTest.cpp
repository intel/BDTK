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

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/operators/ColumnToRowNode.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/SourceNode.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/TestHelpers.h"
#include "tests/nextgen/translator/MockRowToColumn.h"
#include "tests/utils/ArrowArrayBuilder.h"

using namespace cider::jitlib;
using namespace cider::exec::nextgen;
using namespace cider::exec::nextgen::operators;

using ExprPtr = std::shared_ptr<Analyzer::Expr>;

class ColumnToRowTest : public ::testing::Test {};

ExprPtr makeConstant(int32_t val) {
  Datum d;
  d.intval = val;
  return std::make_shared<Analyzer::Constant>(kINT, false, d);
}

TEST_F(ColumnToRowTest, FilterTest) {
  LLVMJITModule module("TestC2R", true);

  auto builder = [](JITFunction* function) {
    //   if (col_var1 <= 5) {
    //     res = col_var1 + 5;
    //   }else{
    //     return col_var1;

    auto col_var1 =
        std::make_shared<Analyzer::ColumnVar>(SQLTypeInfo(SQLTypes::kINT), 100, 1, 0);
    auto col_var2 =
        std::make_shared<Analyzer::ColumnVar>(SQLTypeInfo(SQLTypes::kDOUBLE), 100, 2, 0);

    // constant
    auto const_var = makeConstant(5);

    // var <= 5
    auto cmp_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kBOOLEAN, SQLOps::kLE, SQLQualifier::kONE, col_var1, const_var);

    // var + 5
    auto add_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kINT, SQLOps::kPLUS, SQLQualifier::kONE, col_var1, const_var);

    // source -> column2row -> filter -> project -> sinkR2C
    auto source = SourceTranslator(
        std::vector<ExprPtr>{col_var1, col_var2},
        std::make_unique<ColumnToRowTranslator>(
            std::vector<ExprPtr>{col_var1, col_var2},
            std::make_unique<FilterTranslator>(
                cmp_expr,
                std::make_unique<ProjectTranslator>(
                    add_expr, std::make_unique<MockRowToColumnTranslator>(1)))));

    Context context(function);
    source.consume(context);

    function->createReturn();
  };

  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_C2R_translator")
          .registerModule(module)
          .addReturn(JITTypeTag::VOID)
          .addParameter(JITTypeTag::POINTER, "in", JITTypeTag::INT8)
          .addParameter(JITTypeTag::POINTER, "out", JITTypeTag::INT32)
          .addProcedureBuilder(builder)
          .build();

  module.finish();

  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<double> vec2{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<bool> vec_null{false, false, true, false, true};

  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;
  std::tie(schema, array) =
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col1", CREATE_SUBSTRAIT_TYPE(I32), vec1, vec_null)
          .addColumn<double>("col2", CREATE_SUBSTRAIT_TYPE(Fp64), vec2, vec_null)
          .build();

  auto func_ptr = func->getFunctionPointer<void, int8_t*, int32_t*>();
  int32_t* output = new int32_t[5];
  func_ptr(reinterpret_cast<int8_t*>(array), output);
  EXPECT_EQ(output[0], 6);
  EXPECT_EQ(output[1], 7);
  EXPECT_EQ(output[2], 8);
  EXPECT_EQ(output[3], 9);
  EXPECT_EQ(output[4], 10);
  delete[] output;
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
