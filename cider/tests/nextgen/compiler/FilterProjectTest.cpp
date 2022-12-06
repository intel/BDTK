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
#include <type_traits>

#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class FilterProjectTest : public ::testing::Test {
 public:
  template <typename COL_TYPE = int64_t>
  void executeTest(const std::string& create_ddl, const std::string& sql) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, create_ddl);
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
                [&codegen_ctx, &translators](cider::jitlib::JITFunction* func) {
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
            .addColumn<COL_TYPE>(
                "a",
                CREATE_SUBSTRAIT_TYPE(I64),
                {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                {true, false, false, false, false, true, false, false, false, false})
            .template addColumn<COL_TYPE>(
                "b", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
            .build();

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();
    EXPECT_EQ(output_batch_array->length, 4);

    auto check_array =
        [](ArrowArray* array, size_t expect_len, std::vector<COL_TYPE>& expect_res) {
          EXPECT_EQ(array->length, expect_len);
          COL_TYPE* data_buffer = (COL_TYPE*)array->buffers[1];
          for (size_t i = 0; i < expect_len; ++i) {
            EXPECT_EQ(data_buffer[i], expect_res[i]);
          }
        };

    // a   1 2 3 4
    // b   2 3 4 5
    // a+b 3 5 7 9
    // b+a 3 5 7 9
    // a+a 2 4 6 8
    // b+b 4 6 8 10
    std::vector<COL_TYPE> expect_res = {3, 5, 7, 9};
    check_array(output_batch_array->children[0], 4, expect_res);
    expect_res = {3, 5, 7, 9};
    check_array(output_batch_array->children[1], 4, expect_res);
    expect_res = {2, 4, 6, 8};
    check_array(output_batch_array->children[2], 4, expect_res);
    expect_res = {4, 6, 8, 10};
    check_array(output_batch_array->children[3], 4, expect_res);
  }
};

TEST_F(FilterProjectTest, FrameworkTest) {
  // nullable + not null, not null + nullable, nullable + nullable, not null + not null
  executeTest<int8_t>("CREATE TABLE test(a TINYINT, b TINYINT NOT NULL);",
                      "select a + b, b + a, a + a, b + b from test where a < b");
  executeTest<int16_t>("CREATE TABLE test(a SMALLINT, b SMALLINT NOT NULL);",
                       "select a + b, b + a, a + a, b + b from test where a < b");
  executeTest<int32_t>("CREATE TABLE test(a INT, b INT NOT NULL);",
                       "select a + b, b + a, a + a, b + b from test where a < b");
  executeTest<int64_t>("CREATE TABLE test(a BIGINT, b BIGINT NOT NULL);",
                       "select a + b, b + a, a + a, b + b from test where a < b");
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
