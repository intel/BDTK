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

#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class AggTest : public ::testing::Test {
 public:
  void executeTest(const std::string& sql) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, create_ddl_);
    ::substrait::Plan plan;
    google::protobuf::util::JsonStringToMessage(json, &plan);

    generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
    auto eu = substrait2eu.createRelAlgExecutionUnit();

    // Pipeline Building
    auto pipeline = parsers::toOpPipeline(eu);
    transformer::Transformer transformer;
    auto translators = transformer.toTranslator(pipeline);

    // Codegen
    context::CodegenContext codegen_ctx;
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
            .addProcedureBuilder(
                [&codegen_ctx, &translators](cider::jitlib::JITFunctionPointer func) {
                  codegen_ctx.setJITFunction(func);
                  translators->consume(codegen_ctx);
                  func->createReturn();
                })
            .build();
    module.finish();
    auto query_func = function->getFunctionPointer<void, int8_t*, int8_t*>();

    // Execution
    auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);
    auto input_builder = ArrowArrayBuilder();
    auto&& [schema, array] =
        input_builder.setRowNum(10)
            .addColumn<int64_t>(
                "a", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3, 1, 2, 4, 1, 2, 3, 4})
            .addColumn<int32_t>("b",
                                CREATE_SUBSTRAIT_TYPE(I32),
                                {1, 11, 111, 2, 22, 222, 3, 33, 333, 555})
            .build();

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    auto buffer = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
        runtime_ctx->getContextItem(0));
    auto under_buffer64 = reinterpret_cast<int64_t*>(buffer->getBuffer());
    EXPECT_EQ(under_buffer64[0], 23);
    auto under_buffer32 = reinterpret_cast<int32_t*>(buffer->getBuffer() + 8);
    EXPECT_EQ(under_buffer32[0], 1293);
  }

 private:
  std::string create_ddl_ = "CREATE TABLE test(a BIGINT, b INTEGER);";
};

TEST_F(AggTest, NonGroupby) {
  executeTest("select sum(a), sum(b) from test");
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
