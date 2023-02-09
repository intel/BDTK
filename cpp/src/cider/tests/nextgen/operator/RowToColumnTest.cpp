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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/operators/ArrowSourceNode.h"
#include "exec/nextgen/operators/ColumnToRowNode.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/RowToColumnNode.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"

using namespace cider::jitlib;
using namespace cider::exec::nextgen;
using namespace cider::exec::nextgen::operators;

using ExprPtr = std::shared_ptr<Analyzer::Expr>;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class RowToColumnTest : public ::testing::Test {};

ExprPtr makeConstant(int32_t val) {
  Datum d;
  d.intval = val;
  return std::make_shared<Analyzer::Constant>(kINT, false, d);
}

TEST_F(RowToColumnTest, basicTest) {
  LLVMJITModule module("TestR2C", true);
  Context context;
  auto builder = [&context](JITFunction* function) {
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
    auto source = ArrowSourceTranslator(
        std::vector<ExprPtr>{col_var1, col_var2},
        std::make_unique<ColumnToRowTranslator>(
            std::vector<ExprPtr>{col_var1, col_var2},
            std::make_unique<FilterTranslator>(
                cmp_expr,
                std::make_unique<ProjectTranslator>(
                    add_expr,
                    std::make_unique<RowToColumnTranslator>(col_var1, nullptr)))));
    context.set_function(function);
    context.codegen_context_.setJITFunction(function);
    source.consume(context);

    function->createReturn();
  };

  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_R2C_translator")
          .registerModule(module)
          .addReturn(JITTypeTag::VOID)
          .addParameter(JITTypeTag::POINTER, "context", JITTypeTag::INT8)
          .addParameter(JITTypeTag::POINTER, "out", JITTypeTag::INT8)
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

  auto runtime_ctx = context.codegen_context_.generateRuntimeCTX(allocator);

  auto func_ptr = func->getFunctionPointer<void, int8_t*, int8_t*>();
  func_ptr(reinterpret_cast<int8_t*>(runtime_ctx.get()),
           reinterpret_cast<int8_t*>(array));

  auto output =
      reinterpret_cast<context::Batch*>(runtime_ctx->getContextItem(0))->getArray();
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);
  // TODO : null vector check
  EXPECT_EQ(*((int32_t*)(output->children[0]->buffers[1])), 6);
  EXPECT_EQ(*((int32_t*)(output->children[0]->buffers[1]) + 1), 7);
  EXPECT_EQ(*((int32_t*)(output->children[0]->buffers[1]) + 2), 8);
  EXPECT_EQ(*((int32_t*)(output->children[0]->buffers[1]) + 3), 9);
  EXPECT_EQ(*((int32_t*)(output->children[0]->buffers[1]) + 4), 10);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
