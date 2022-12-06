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

  JITFunctionPointer func = JITFunctionBuilder()
                                .setFuncName("test_func")
                                .registerModule(module)
                                .addParameter(Type, "x")
                                .addReturn(Type)
                                .addProcedureBuilder(builder)
                                .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<NativeType, NativeType>();
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
        auto left = func->createVariable(Type, "left");
        *left = func->getArgument(0);

        auto right_const = func->createConstant(Type, right);
        auto ans = op(left, right_const);

        func->createReturn(ans);
      });
}

template <JITTypeTag Type, typename NativeType, typename BuilderType>
void executeCompareSingleParamTest(NativeType input, bool output, BuilderType builder) {
  LLVMJITModule module("TestOperator");
  JITFunctionPointer func = JITFunctionBuilder()
                                .setFuncName("test_operator")
                                .registerModule(module)
                                .addParameter(Type, "x")
                                .addReturn(JITTypeTag::BOOL)
                                .addProcedureBuilder(builder)
                                .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<bool, NativeType>();
  EXPECT_EQ(func_ptr(input), output);
}

template <JITTypeTag Type, typename T>
void executeCompareOp(T left, T right, bool output, OpFunc op) {
  using NativeType = typename JITTypeTraits<Type>::NativeType;
  executeCompareSingleParamTest<Type>(
      static_cast<NativeType>(left),
      output,
      [&, right = static_cast<NativeType>(right)](JITFunction* func) {
        auto left = func->createVariable(Type, "left");
        *left = func->getArgument(0);

        auto right_const = func->createConstant(Type, right);
        auto ans = op(left, right_const);
        CHECK(ans->getValueTypeTag() == JITTypeTag::BOOL);

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

  // and
  executeBinaryOp<JITTypeTag::BOOL>(
      true, false, false, [](JITValue& a, JITValue& b) { return a && b && true; });
  executeBinaryOp<JITTypeTag::BOOL>(
      true, true, true, [](JITValue& a, JITValue& b) { return a && true && b; });
  executeBinaryOp<JITTypeTag::BOOL>(
      true, false, false, [](JITValue& a, JITValue& b) { return false && a && b; });
  executeBinaryOp<JITTypeTag::BOOL>(
      false, false, false, [](JITValue& a, JITValue& b) { return true && a && b; });

  // or
  executeBinaryOp<JITTypeTag::BOOL>(
      true, false, true, [](JITValue& a, JITValue& b) { return a || false || b; });
  executeBinaryOp<JITTypeTag::BOOL>(
      true, true, true, [](JITValue& a, JITValue& b) { return a || b || false; });
  executeBinaryOp<JITTypeTag::BOOL>(
      false, true, true, [](JITValue& a, JITValue& b) { return true || a || b; });
  executeBinaryOp<JITTypeTag::BOOL>(
      false, false, false, [](JITValue& a, JITValue& b) { return a || false || b; });
}

TEST_F(JITLibTests, CompareOpTest) {
  // eq
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, false, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a == 100; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 100.0, true, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 20.0, false, [](JITValue& a, JITValue& b) { return a == b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 20.0, true, [](JITValue& a, JITValue& b) { return 20.0 == b; });

  // ne
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a != b; });
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a != 40; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, false, [](JITValue& a, JITValue& b) { return a != b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 20, true, [](JITValue& a, JITValue& b) { return a != b; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 100, false, [](JITValue& a, JITValue& b) { return a != b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 100.0, false, [](JITValue& a, JITValue& b) { return a != b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 100.0, false, [](JITValue& a, JITValue& b) { return 100.0 != b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 2.0, true, [](JITValue& a, JITValue& b) { return a != b; });

  // gt
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a > b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, false, [](JITValue& a, JITValue& b) { return a > b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, false, [](JITValue& a, JITValue& b) { return a > 200; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a > b; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 99, true, [](JITValue& a, JITValue& b) { return a > b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, false, [](JITValue& a, JITValue& b) { return a > b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, true, [](JITValue& a, JITValue& b) { return 102.0 > b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 100.0, false, [](JITValue& a, JITValue& b) { return a > b; });

  // ge
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a >= b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a >= b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a >= b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 2, true, [](JITValue& a, JITValue& b) { return a >= 30; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 99, true, [](JITValue& a, JITValue& b) { return a >= b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, false, [](JITValue& a, JITValue& b) { return a >= b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, false, [](JITValue& a, JITValue& b) { return 99.2 >= b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 100.0, true, [](JITValue& a, JITValue& b) { return a >= b; });

  // lt
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, false, [](JITValue& a, JITValue& b) { return a < b; });
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, false, [](JITValue& a, JITValue& b) { return 4 < b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, false, [](JITValue& a, JITValue& b) { return a < b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 2, false, [](JITValue& a, JITValue& b) { return a < b; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 101, true, [](JITValue& a, JITValue& b) { return a < b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, true, [](JITValue& a, JITValue& b) { return a < b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, true, [](JITValue& a, JITValue& b) { return b < 103.2; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 100.0, false, [](JITValue& a, JITValue& b) { return a < b; });

  // le
  executeCompareOp<JITTypeTag::INT8>(
      100, 2, false, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::INT16>(
      100, 100, true, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::INT32>(
      100, 101, true, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 99, false, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::INT64>(
      100, 99, true, [](JITValue& a, JITValue& b) { return 97 <= b; });
  executeCompareOp<JITTypeTag::FLOAT>(
      100.0, 101.0, true, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 100.0, true, [](JITValue& a, JITValue& b) { return a <= b; });
  executeCompareOp<JITTypeTag::DOUBLE>(
      100.0, 100.0, true, [](JITValue& a, JITValue& b) { return a <= 100.0; });
}

TEST_F(JITLibTests, ExternalModuleTest) {
  LLVMJITModule module("Test Module", true);
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_func")
          .registerModule(module)
          .addReturn(JITTypeTag::INT32)
          .addProcedureBuilder([](JITFunction* function) {
            JITValuePointer x = function->createVariable(JITTypeTag::INT32, "x1");
            JITValuePointer a = function->createConstant(JITTypeTag::INT32, 123);
            JITValuePointer b = function->createConstant(JITTypeTag::INT32, 876);
            *x = *function->emitRuntimeFunctionCall(
                "external_call_test_sum",
                JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT32,
                                          .params_vector = {a.get(), b.get()}});
            function->createReturn(*x);
          })
          .build();
  module.finish();

  auto ptr = func->getFunctionPointer<int32_t>();
  EXPECT_EQ(ptr(), 999);
}

TEST_F(JITLibTests, BasicIFControlFlowWithoutElseTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func = JITFunctionBuilder()
                                .setFuncName("test_func")
                                .registerModule(module)
                                .addParameter(JITTypeTag::INT32, "x")
                                .addParameter(JITTypeTag::BOOL, "condition")
                                .addReturn(JITTypeTag::INT32)
                                .addProcedureBuilder([](JITFunction* function) {
                                  JITValuePointer ret =
                                      function->createVariable(JITTypeTag::INT32, "ret");
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
                                })
                                .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, false), 123);
  EXPECT_EQ(func_ptr(123, true), 124);
}

TEST_F(JITLibTests, BasicIFControlFlowWithElseTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func = JITFunctionBuilder()
                                .setFuncName("test_func")
                                .registerModule(module)
                                .addParameter(JITTypeTag::INT32, "x")
                                .addParameter(JITTypeTag::BOOL, "condition")
                                .addReturn(JITTypeTag::INT32)
                                .addProcedureBuilder([](JITFunction* function) {
                                  JITValuePointer ret =
                                      function->createVariable(JITTypeTag::INT32, "ret");
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
                                })
                                .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, false), 133);
  EXPECT_EQ(func_ptr(123, true), 124);
}

TEST_F(JITLibTests, NestedIFControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func = JITFunctionBuilder()
                                .setFuncName("test_func")
                                .registerModule(module)
                                .addParameter(JITTypeTag::INT32, "x")
                                .addParameter(JITTypeTag::BOOL, "condition")
                                .addReturn(JITTypeTag::INT32)
                                .addProcedureBuilder([](JITFunction* function) {
                                  JITValuePointer ret =
                                      function->createVariable(JITTypeTag::INT32, "ret");
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
                                })
                                .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<int32_t, int32_t, bool>();
  EXPECT_EQ(func_ptr(123, true), 133);
  EXPECT_EQ(func_ptr(123, false), 124);
}

TEST_F(JITLibTests, BasicForControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_func")
          .registerModule(module)
          .addReturn(JITTypeTag::INT32)
          .addProcedureBuilder([](JITFunction* function) {
            JITValuePointer ret = function->createVariable(JITTypeTag::INT32, "ret");
            ret = function->createConstant(JITTypeTag::INT32, 0);

            auto loop_builder = function->createLoopBuilder();
            JITValuePointer index = function->createVariable(JITTypeTag::INT32, "index");
            *index = function->createConstant(JITTypeTag::INT32, 9);

            loop_builder->condition([&]() { return index + 0; })
                ->loop([&]() { ret = ret + index; })
                ->update([&]() { index = index - 1; })
                ->build();

            function->createReturn(ret);
          })
          .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<int32_t>();
  EXPECT_EQ(func_ptr(), 45);
}

TEST_F(JITLibTests, NestedForControlFlowTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_func")
          .registerModule(module)
          .addReturn(JITTypeTag::INT32)
          .addProcedureBuilder([](JITFunction* function) {
            JITValuePointer ret = function->createVariable(JITTypeTag::INT32, "ret");
            ret = function->createConstant(JITTypeTag::INT32, 0);

            int levels = 5;
            std::function<void()> nested_loop_builder = [&]() {
              if (0 == levels) {
                ret = ret + 1;
                return;
              }
              --levels;

              auto loop_builder = function->createLoopBuilder();
              JITValuePointer index =
                  function->createVariable(JITTypeTag::INT32, "index");
              *index = function->createConstant(JITTypeTag::INT32, 10);

              loop_builder->condition([&]() { return index + 0; })
                  ->loop([&]() { nested_loop_builder(); })
                  ->update([&]() { index = index - 1; })
                  ->build();
            };
            nested_loop_builder();

            function->createReturn(ret);
          })
          .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<int32_t>();
  EXPECT_EQ(func_ptr(), 100000);
}

TEST_F(JITLibTests, PointerCounterTest) {
  LLVMJITModule module("Test Module");
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_pointer")
          .registerModule(module)
          .addReturn(JITTypeTag::INT32)
          .addParameter(JITTypeTag::INT32, "a")
          .addProcedureBuilder([](JITFunction* function) {
            JITValuePointer a = function->getArgument(0);
            JITValuePointer b = function->createConstant(JITTypeTag::INT32, 876);

            std::vector<JITValuePointer> test_vector1;
            {
              std::vector<JITValuePointer> test_vector2;
              {
                JITValuePointer res = function->createVariable(JITTypeTag::INT32, "x1");
                JITValuePointer temp_c = function->createConstant(JITTypeTag::INT32, 111);

                test_vector1.push_back(res);
                test_vector2.push_back(res);
                EXPECT_EQ(res.getRefNum(), 3);

                test_vector2.push_back(temp_c);
                EXPECT_EQ(temp_c.getRefNum(), 2);
              }
              EXPECT_EQ(test_vector1[0].getRefNum(), 2);
            }
            EXPECT_EQ(test_vector1[0].getRefNum(), 1);

            test_vector1[0] = a + b;
            function->createReturn(test_vector1[0]);
          })
          .build();
  module.finish();

  auto ptr1 = func->getFunctionPointer<int32_t, int32_t>();
  EXPECT_EQ(ptr1(123), 999);
}

TEST_F(JITLibTests, ReadOnlyJITPointerTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_func")
          .registerModule(module)
          .addParameter(JITTypeTag::POINTER, "array", JITTypeTag::INT32)
          .addParameter(JITTypeTag::INT32, "len")
          .addReturn(JITTypeTag::INT32, "ret")
          .addProcedureBuilder([](JITFunction* function) {
            JITValuePointer ret = function->createVariable(JITTypeTag::INT32, "ret");
            ret = function->createConstant(JITTypeTag::INT32, 0);

            auto loop_builder = function->createLoopBuilder();
            JITValuePointer index = function->createVariable(JITTypeTag::INT32, "index");

            auto array_ptr = function->getArgument(0);
            auto array_len = function->getArgument(1);
            *index = array_len - 1;

            loop_builder->condition([&]() { return index + 0; })
                ->loop([&]() { ret = ret + array_ptr[index]; })
                ->update([&]() { index = index - 1; })
                ->build();

            function->createReturn(ret);
          })
          .build();
  module.finish();

  int32_t data[10] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto func_ptr = func->getFunctionPointer<int32_t, int32_t*, int32_t>();
  EXPECT_EQ(func_ptr(data, 10), 45);
}

TEST_F(JITLibTests, ReadAndWriteJITPointerTest) {
  LLVMJITModule module("TestModule");
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test function name")
          .registerModule(module)
          .addParameter(JITTypeTag::POINTER, "array_in", JITTypeTag::INT8)
          .addParameter(JITTypeTag::POINTER, "array_out", JITTypeTag::INT32)
          .addParameter(JITTypeTag::INT32, "len")
          .addReturn(JITTypeTag::POINTER, "return", JITTypeTag::INT32)
          .addProcedureBuilder([](JITFunction* function) {
            auto loop_builder = function->createLoopBuilder();
            JITValuePointer index = function->createVariable(JITTypeTag::INT32, "index");

            auto array_in_ptr = function->getArgument(0);
            auto array_in_ptr_i32 = array_in_ptr->castPointerSubType(JITTypeTag::INT32);
            auto array_out_ptr = function->getArgument(1);
            auto array_len = function->getArgument(2);
            *index = array_len - 1;

            loop_builder->condition([&]() { return index + 0; })
                ->loop([&]() { array_out_ptr[index] = array_in_ptr_i32[index]; })
                ->update([&]() { index = index - 1; })
                ->build();

            function->createReturn(array_out_ptr);
          })
          .build();
  module.finish();

  int32_t data_in[10]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  int32_t data_out[10]{0};
  auto func_ptr = func->getFunctionPointer<int32_t*, int8_t*, int32_t*, int32_t>();
  int32_t* out = func_ptr(reinterpret_cast<int8_t*>(data_in), data_out, 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(out[i], data_in[i]);
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
