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

#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/translator/filter.h"
#include "tests/TestHelpers.h"
#include "type/plan/Analyzer.h"

using namespace cider::jitlib;
using namespace cider::exec::nextgen::translator;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;

class FilterTests : public ::testing::Test {};

std::shared_ptr<Analyzer::ColumnVar> makeColumnVar() {
  SQLTypes subtypes{SQLTypes::kNULLT};
  SQLTypes intType{SQLTypes::kINT};
  SQLTypeInfo col_info(intType, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

  return std::make_shared<Analyzer::ColumnVar>(col_info, 100, 1, 0);
}

ExprPtr makeConstant(int32_t val) {
  SQLTypes subtypes{SQLTypes::kNULLT};
  SQLTypes intType{SQLTypes::kINT};
  SQLTypeInfo col_info(intType, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

  Datum d;
  d.intval = val;
  return std::make_shared<Analyzer::Constant>(col_info, false, d);
}

ExprPtr makeCmp(ExprPtr lhs, ExprPtr rhs) {
  SQLTypes subtypes{SQLTypes::kNULLT};
  SQLTypes sqlTypes{SQLTypes::kBOOLEAN};
  SQLTypeInfo ti_boolean(
      sqlTypes, 0, 0, false, EncodingType::kENCODING_NONE, 0, subtypes);

  return std::make_shared<Analyzer::BinOper>(
      ti_boolean, false, SQLOps::kLT, SQLQualifier::kONE, lhs, rhs);
}

template <JITTypeTag Type, typename NativeType, typename Builder>
void executeFilterTest(NativeType input, bool output, Builder builder) {
  LLVMJITModule module("Test");
  JITFunctionPointer func = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_filter_op",
      .ret_type = JITFunctionParam{.type = JITTypeTag::BOOL},
      .params_type = {JITFunctionParam{.name = "x", .type = Type}},
  });
  auto var = func->createVariable("var", JITTypeTag::INT32);
  *var = func->getArgument(0);

  builder(func.get(), var);

  // func->createReturn(ans);

  func->finish();
  module.finish();

  auto func_ptr = func->getFunctionPointer<bool, NativeType>();
  EXPECT_EQ(func_ptr(input), output);
}

TEST_F(FilterTests, BasicTest) {
  executeFilterTest<JITTypeTag::INT32>(
      10, true, [](JITFunction* func, JITValuePointer& var) {
        auto var_expr = makeColumnVar();
        var_expr->set_value_and_null(var.get());
        // var < 5
        FilterTranslator trans({makeCmp(var_expr, makeConstant(5))});

        Context context(func);
        trans.consume(context);
      });
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
