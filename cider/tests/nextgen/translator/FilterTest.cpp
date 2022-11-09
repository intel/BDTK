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
#include <memory>
#include <vector>

#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/translator/filter.h"
#include "exec/nextgen/translator/sink.h"
#include "tests/TestHelpers.h"
#include "type/plan/Analyzer.h"
#include "util/sqldefs.h"

using namespace cider::jitlib;
using namespace cider::exec::nextgen::translator;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;

class FilterTests : public ::testing::Test {};

ExprPtr makeConstant(int32_t val) {
  Datum d;
  d.intval = val;
  return std::make_shared<Analyzer::Constant>(kINT, true, d);
}

template <JITTypeTag Type, typename NativeType, typename Builder>
void executeFilterTest(NativeType input, bool output, Builder builder) {
  LLVMJITModule module("Test");
  JITFunctionPointer func = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_filter_op",
      .ret_type = JITFunctionParam{.type = Type},
      .params_type = {JITFunctionParam{.name = "x", .type = Type}},
  });

  builder(func.get());

  func->finish();
  module.finish();

  auto func_ptr = func->getFunctionPointer<bool, NativeType>();
  EXPECT_EQ(func_ptr(input), output);
}

TEST_F(FilterTests, BasicTest) {
  auto builder = [](JITFunction* func) {
    auto var = func->createVariable(JITTypeTag::INT32, "var");
    *var = func->getArgument(0);
    // if (var <= 5) {
    //   var = var + 5;
    // }
    // return var;

    // var
    auto col_var =
        std::make_shared<Analyzer::ColumnVar>(SQLTypeInfo(SQLTypes::kINT), 100, 1, 0);
    // col_var->set_expr_value(std::vector{var.get()});
    std::vector<JITValuePointer> vec;
    vec.emplace_back(std::move(var));
    col_var->set_expr_value(std::move(vec));

    // constant
    auto const_var = makeConstant(5);

    // var <= 5
    auto cmp_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kBOOLEAN, SQLOps::kLE, SQLQualifier::kONE, col_var, const_var);

    // var + 5
    auto add_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kINT, SQLOps::kPLUS, SQLQualifier::kONE, col_var, const_var);
    auto sink = std::make_unique<SinkTranslator>(std::vector<ExprPtr>{add_expr});

    FilterTranslator trans(std::vector<ExprPtr>{cmp_expr}, std::move(sink));

    Context context(func);
    trans.consume(context);

    func->createReturn(context.out[0]->get_value());
  };

  executeFilterTest<JITTypeTag::INT32>(1, 6, builder);
  executeFilterTest<JITTypeTag::INT32>(5, 5, builder);
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
