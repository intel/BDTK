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
#include <functional>

#include "exec/nextgen/jitlib/JITLib.h"
#include "tests/TestHelpers.h"

using namespace cider::jitlib;

class JITLibTests : public ::testing::Test {};

template <JITTypeTag Type, typename NativeType, typename BuilderType>
void executeSingleParamTest(NativeType input, NativeType output, BuilderType builder) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func",
      .ret_type = JITFunctionParam{.type = Type},
      .params_type = {JITFunctionParam{.name = "x", .type = Type}},
  });
  builder(function.get());
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<NativeType, NativeType>();
  EXPECT_EQ(func_ptr(input), output);
}

using OpFunc = JITValuePointer(JITValue&, JITValue&);
template <JITTypeTag Type, typename T>
void executeBinaryOp(T left, T right, T output, OpFunc op) {
  using NativeType = typename JITTypeTraits<Type>::NativeType;
  executeSingleParamTest<Type>(
      static_cast<NativeType>(left),
      static_cast<NativeType>(output),
      [&, right = static_cast<NativeType>(right)](JITFunction* func) {
        auto left = func->createVariable("left", Type);
        *left = func->getArgument(0);

        auto right_const = func->createConstant(Type, right);
        auto ans = op(left, right_const);

        func->createReturn(ans);
      });
}

TEST_F(JITLibTests, ArithmeticOPTest) {
  // Sum
  executeBinaryOp<JITTypeTag::INT8>(
      10, 20, 31, [](JITValue& a, JITValue& b) { return a + b + 1; });
  executeBinaryOp<JITTypeTag::INT16>(
      10, 20, 31, [](JITValue& a, JITValue& b) { return a + 1 + b; });
  executeBinaryOp<JITTypeTag::INT32>(
      10, 20, 31, [](JITValue& a, JITValue& b) { return 1 + a + b; });
  executeBinaryOp<JITTypeTag::INT64>(
      10, 20, 31, [](JITValue& a, JITValue& b) { return a + b + 1; });
  executeBinaryOp<JITTypeTag::FLOAT>(
      10.0, 20.0, 30.5, [](JITValue& a, JITValue& b) { return a + b + 0.5; });
  executeBinaryOp<JITTypeTag::DOUBLE>(
      10.0, 20.0, 30.5, [](JITValue& a, JITValue& b) { return a + 0.5 + b; });

  // Sub
  executeBinaryOp<JITTypeTag::INT8>(
      20, 10, 9, [](JITValue& a, JITValue& b) { return a - b - 1; });
  executeBinaryOp<JITTypeTag::INT16>(
      20, 10, 9, [](JITValue& a, JITValue& b) { return a - 1 - b; });
  executeBinaryOp<JITTypeTag::INT32>(
      20, 10, 9, [](JITValue& a, JITValue& b) { return a - b - 1; });
  executeBinaryOp<JITTypeTag::INT64>(
      20, 10, 9, [](JITValue& a, JITValue& b) { return a - 1 - b; });
  executeBinaryOp<JITTypeTag::FLOAT>(
      20.0, 10.0, 9.5, [](JITValue& a, JITValue& b) { return a - b - 0.5; });
  executeBinaryOp<JITTypeTag::DOUBLE>(
      20.0, 10.0, 9.5, [](JITValue& a, JITValue& b) { return a - 0.5 - b; });

  // Multi
  executeBinaryOp<JITTypeTag::INT8>(
      2, 2, 12, [](JITValue& a, JITValue& b) { return a * b * 3; });
  executeBinaryOp<JITTypeTag::INT16>(
      2, 2, 12, [](JITValue& a, JITValue& b) { return a * 3 * b; });
  executeBinaryOp<JITTypeTag::INT32>(
      2, 2, 12, [](JITValue& a, JITValue& b) { return 3 * a * b; });
  executeBinaryOp<JITTypeTag::INT64>(
      2, 2, 12, [](JITValue& a, JITValue& b) { return a * b * 3; });
  executeBinaryOp<JITTypeTag::FLOAT>(
      20.0, 10.0, 100.0, [](JITValue& a, JITValue& b) { return a * b * 0.5; });
  executeBinaryOp<JITTypeTag::DOUBLE>(
      20.0, 10.0, 100.0, [](JITValue& a, JITValue& b) { return a * 0.5 * b; });

  // Div
  executeBinaryOp<JITTypeTag::INT8>(
      100, 2, 10, [](JITValue& a, JITValue& b) { return a / b / 5; });
  executeBinaryOp<JITTypeTag::INT16>(
      100, 2, 10, [](JITValue& a, JITValue& b) { return a / b / 5; });
  executeBinaryOp<JITTypeTag::INT32>(
      100, 2, 10, [](JITValue& a, JITValue& b) { return a / b / 5; });
  executeBinaryOp<JITTypeTag::INT64>(
      100, 2, 10, [](JITValue& a, JITValue& b) { return a / b / 5; });
  executeBinaryOp<JITTypeTag::FLOAT>(
      20.0, 10.0, 4.0, [](JITValue& a, JITValue& b) { return a / b / 0.5; });
  executeBinaryOp<JITTypeTag::DOUBLE>(
      20.0, 10.0, 4.0, [](JITValue& a, JITValue& b) { return a / b / 0.5; });

  // Mod
  executeBinaryOp<JITTypeTag::INT8>(
      109, 10, 4, [](JITValue& a, JITValue& b) { return a % b % 5; });
  executeBinaryOp<JITTypeTag::INT16>(
      109, 10, 4, [](JITValue& a, JITValue& b) { return a % b % 5; });
  executeBinaryOp<JITTypeTag::INT32>(
      109, 10, 4, [](JITValue& a, JITValue& b) { return a % b % 5; });
  executeBinaryOp<JITTypeTag::INT64>(
      109, 10, 4, [](JITValue& a, JITValue& b) { return a % b % 5; });
  executeBinaryOp<JITTypeTag::FLOAT>(
      25.5, 10.0, 0.5, [](JITValue& a, JITValue& b) { return a % b % 1.0; });
  executeBinaryOp<JITTypeTag::DOUBLE>(
      25.5, 10.0, 0.5, [](JITValue& a, JITValue& b) { return a % b % 1.0; });
}

TEST_F(JITLibTests, LogicalOpTest) {
  // Not
  executeBinaryOp<JITTypeTag::BOOL>(
      true, false, false, [](JITValue& a, JITValue& b) { return !a; });
  executeBinaryOp<JITTypeTag::BOOL>(
      false, true, true, [](JITValue& a, JITValue& b) { return !a; });
}

TEST_F(JITLibTests, BasicIFControlFlowWithoutElseTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func",
      .ret_type = JITFunctionParam{.type = JITTypeTag::INT32},
      .params_type = {JITFunctionParam{.name = "x", .type = JITTypeTag::INT32},
                      JITFunctionParam{.name = "condition", .type = JITTypeTag::BOOL}}});
  {
    JITValuePointer ret = function->createVariable("ret", JITTypeTag::INT32);
    ret = function->getArgument(0);
    auto if_builder = function->createIfBuilder();
    if_builder
        ->condition([&]() {
          auto condition = function->getArgument(1);
          return condition;
        })
        ->ifTrue([&]() { ret = ret + 1; })
        ->build();
    function->createReturn(ret);
  }
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, false), 123);
  EXPECT_EQ(func_ptr(123, true), 124);
}

TEST_F(JITLibTests, BasicIFControlFlowWithElseTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func",
      .ret_type = JITFunctionParam{.type = JITTypeTag::INT32},
      .params_type = {JITFunctionParam{.name = "x", .type = JITTypeTag::INT32},
                      JITFunctionParam{.name = "condition", .type = JITTypeTag::BOOL}}});
  {
    JITValuePointer ret = function->createVariable("ret", JITTypeTag::INT32);
    ret = function->getArgument(0);
    auto if_builder = function->createIfBuilder();
    if_builder
        ->condition([&]() {
          auto condition = function->getArgument(1);
          return condition;
        })
        ->ifTrue([&]() { ret = ret + 1; })
        ->ifFalse([&]() { ret = ret + 10; })
        ->build();
    function->createReturn(ret);
  }
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, false), 133);
  EXPECT_EQ(func_ptr(123, true), 124);
}

TEST_F(JITLibTests, NestedIFControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func",
      .ret_type = JITFunctionParam{.type = JITTypeTag::INT32},
      .params_type = {JITFunctionParam{.name = "x", .type = JITTypeTag::INT32},
                      JITFunctionParam{.name = "condition", .type = JITTypeTag::BOOL}}});
  {
    JITValuePointer ret = function->createVariable("ret", JITTypeTag::INT32);
    ret = function->getArgument(0);
    auto if_builder = function->createIfBuilder();
    if_builder
        ->condition([&]() {
          auto condition = function->getArgument(1);
          return condition;
        })
        ->ifTrue([&]() {
          auto if_builder = function->createIfBuilder();
          if_builder
              ->condition([&]() {
                auto condition = function->getArgument(1);
                return condition;
              })
              ->ifTrue([&]() { ret = ret + 10; })
              ->ifFalse([&]() { ret = ret + 1; })
              ->build();
        })
        ->ifFalse([&]() {
          auto if_builder = function->createIfBuilder();
          if_builder
              ->condition([&]() {
                auto condition = function->getArgument(1);
                return condition;
              })
              ->ifTrue([&]() { ret = ret + 10; })
              ->ifFalse([&]() { ret = ret + 1; })
              ->build();
        })
        ->build();
    function->createReturn(ret);
  }
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, true), 133);
  EXPECT_EQ(func_ptr(123, false), 124);
}

TEST_F(JITLibTests, BasicForControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(
      JITFunctionDescriptor{.function_name = "test_func",
                            .ret_type = JITFunctionParam{.type = JITTypeTag::INT32}});
  {
    JITValuePointer ret = function->createVariable("ret", JITTypeTag::INT32);
    ret = function->createConstant(JITTypeTag::INT32, 0);

    auto loop_builder = function->createLoopBuilder();
    JITValuePointer index = function->createVariable("index", JITTypeTag::INT32);
    *index = function->createConstant(JITTypeTag::INT32, 9);

    loop_builder->condition([&]() { return index + 0; })
        ->loop([&]() { ret = ret + index; })
        ->update([&]() { index = index - 1; })
        ->build();

    function->createReturn(ret);
  }
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<int32_t>();
  EXPECT_EQ(func_ptr(), 45);
}

TEST_F(JITLibTests, NestedForControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer function = module.createJITFunction(
      JITFunctionDescriptor{.function_name = "test_func",
                            .ret_type = JITFunctionParam{.type = JITTypeTag::INT32}});
  {
    JITValuePointer ret = function->createVariable("ret", JITTypeTag::INT32);
    ret = function->createConstant(JITTypeTag::INT32, 0);

    int levels = 5;
    std::function<void()> nested_loop_builder = [&]() {
      if (0 == levels) {
        ret = ret + 1;
        return;
      }
      --levels;

      auto loop_builder = function->createLoopBuilder();
      JITValuePointer index = function->createVariable("index", JITTypeTag::INT32);
      *index = function->createConstant(JITTypeTag::INT32, 10);

      loop_builder->condition([&]() { return index + 0; })
          ->loop([&]() { nested_loop_builder(); })
          ->update([&]() { index = index - 1; })
          ->build();
    };
    nested_loop_builder();

    function->createReturn(ret);
  }
  function->finish();
  module.finish();

  auto func_ptr = function->getFunctionPointer<int32_t>();
  EXPECT_EQ(func_ptr(), 100000);
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
