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

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "tests/TestHelpers.h"

using namespace jitlib;

class JITLibTests : public ::testing::Test {};

TEST_F(JITLibTests, BasicTest) {
  LLVMJITModule module("Test");

  JITFunctionPointer function1 = module.createJITFunction(JITFunctionDescriptor{
      .function_name = "test_func1",
      .ret_type = JITFunctionParam{.type = INT32},
      .params_type = {JITFunctionParam{.name = "x", .type = INT32}},
  });
  {
    JITValuePointer x1 = function1->createVariable("x1", INT32);
    JITValuePointer x = function1->getArgument(0);
    *x1 = x;
    auto sum = x1 + 1;
    function1->createReturn(sum);
  }
  function1->finish();
  module.finish();

  auto ptr1 = function1->getFunctionPointer<int32_t, int32_t>();
  EXPECT_EQ(ptr1(12), 13);
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
