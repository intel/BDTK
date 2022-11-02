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
#include "exec/nextgen/jitlib/JITLib.h"

#include <gtest/gtest.h>
#include <memory>

#include "exec/nextgen/translator/filter.h"
#include "tests/TestHelpers.h"

using namespace cider::jitlib;

class FilterTests : public ::testing::Test {};

using ExprPtr = std::shared_ptr<Analyzer::Expr>;

ExprPtr makeColumnVar() {
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

TEST_F(FilterTests, BasicTest) {
  //   LLVMJITModule module("Test");
  //   JITFunctionPointer func = module.createJITFunction(JITFunctionDescriptor{
  //       .function_name = "test_filter_func",
  //       .ret_type = JITFunctionParam{.type = JITTypeTag::INT32},
  //       .params_type = {JITFunctionParam{.name = "x", .type = JITTypeTag::INT32}},
  //   });

  // col < 5
  FilterTranslator trans({makeCmp(makeColumnVar(), makeConstant(5))});

  Context context;
  JITTuple input_col;
  trans.consume(context, input_col);

  //   func->finish();

  //   module.finish();

  //   auto ptr1 = func->getFunctionPointer<int32_t, int32_t>();
  //   EXPECT_EQ(ptr1(12), 123);
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
