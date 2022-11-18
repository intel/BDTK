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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "tests/TestHelpers.h"
#include "type/data/sqltypes.h"

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class ContextTest : public ::testing::Test {};

TEST_F(ContextTest, ContextTest) {
  cider::exec::nextgen::context::CodegenContext context;
  auto module = cider::jitlib::LLVMJITModule("test", true);
  cider::jitlib::JITFunctionPointer function =
      cider::jitlib::JITFunctionBuilder()
          .registerModule(module)
          .setFuncName("query_func")
          .addReturn(cider::jitlib::JITTypeTag::VOID)
          .addParameter(cider::jitlib::JITTypeTag::POINTER,
                        "context",
                        cider::jitlib::JITTypeTag::INT8)
          .addParameter(cider::jitlib::JITTypeTag::POINTER,
                        "input",
                        cider::jitlib::JITTypeTag::INT8)
          .addProcedureBuilder([&context](cider::jitlib::JITFunction* func) {
            context.setJITFunction(func);
            auto output_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "output");
            auto loop1_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "loop1_output");
            auto loop2_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "loop2_output");
            func->createReturn();
          })
          .build();
  module.finish();

  auto runtime_ctx = context.generateRuntimeCTX(allocator);
  EXPECT_EQ(runtime_ctx->getContextItemNum(), 3);
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(1), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(2), nullptr);
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
