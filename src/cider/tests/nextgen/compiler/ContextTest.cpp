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

class ContextTests : public ::testing::Test {};

TEST_F(ContextTests, ContextTest) {
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
          .addProcedureBuilder([&context](cider::jitlib::JITFunctionPointer func) {
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

TEST_F(ContextTests, ContextBufferTest) {
  cider::exec::nextgen::context::CodegenContext context;
  auto module = cider::jitlib::LLVMJITModule("test_context_buffer", true);
  cider::jitlib::JITFunctionPointer function =
      cider::jitlib::JITFunctionBuilder()
          .registerModule(module)
          .setFuncName("allocate_buffer")
          .addReturn(cider::jitlib::JITTypeTag::VOID)
          .addParameter(cider::jitlib::JITTypeTag::POINTER,
                        "context",
                        cider::jitlib::JITTypeTag::INT8)
          .addProcedureBuilder([&context](cider::jitlib::JITFunctionPointer func) {
            context.setJITFunction(func);
            auto output_buffer1 = context.registerBuffer(1 * 8, "output_buffer1");
            auto output_buffer2 = context.registerBuffer(2 * 8, "output_buffer2");

            auto cast_output_buffer1 =
                output_buffer1->castPointerSubType(cider::jitlib::JITTypeTag::INT64);
            auto cast_output_buffer2 =
                output_buffer2->castPointerSubType(cider::jitlib::JITTypeTag::INT64);

            auto index_0 = func->createLiteral(cider::jitlib::JITTypeTag::INT32, 0);
            auto index_1 = func->createLiteral(cider::jitlib::JITTypeTag::INT32, 1);

            cast_output_buffer1[index_0] =
                func->createLiteral(cider::jitlib::JITTypeTag::INT64, 1l);
            cast_output_buffer2[index_1] =
                func->createLiteral(cider::jitlib::JITTypeTag::INT64, 2l);

            func->createReturn();
          })
          .build();
  module.finish();

  auto runtime_ctx = context.generateRuntimeCTX(allocator);

  auto query_func = function->getFunctionPointer<void, int8_t*>();
  query_func((int8_t*)runtime_ctx.get());

  EXPECT_EQ(runtime_ctx->getContextItemNum(), 2);
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(1), nullptr);

  auto output_buffer1 = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
      runtime_ctx->getContextItem(0));
  auto raw_buffer1 = reinterpret_cast<int64_t*>(output_buffer1->getBuffer());
  EXPECT_EQ(raw_buffer1[0], 1);

  auto output_buffer2 = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
      runtime_ctx->getContextItem(1));
  auto raw_buffer2 = reinterpret_cast<int64_t*>(output_buffer2->getBuffer());
  EXPECT_EQ(raw_buffer2[1], 2);
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
