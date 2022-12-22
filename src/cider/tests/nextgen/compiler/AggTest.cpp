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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "exec/nextgen/Nextgen.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"

#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"
#include "util/Logger.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

operators::TranslatorPtr initSqlToTranslators(const std::string& sql,
                                              const std::string& create_ddl) {
  // SQL Parsing
  auto json = RunIsthmus::processSql(sql, create_ddl);
  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);

  generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
  auto eu = substrait2eu.createRelAlgExecutionUnit();

  // Pipeline Building
  auto pipeline = parsers::toOpPipeline(eu);
  transformer::Transformer transformer;
  return transformer.toTranslator(pipeline);
}

template <typename TYPE>
void check_array(ArrowArray* array, size_t expect_len, std::vector<TYPE> expect_values) {
  EXPECT_EQ(array->length, expect_len);
  TYPE* data_buffer = (TYPE*)array->buffers[1];
  for (size_t i = 0; i < expect_len; ++i) {
    EXPECT_EQ(data_buffer[0], expect_values[0]);
  }
}

context::RuntimeCtxPtr executeAndReturnRuntimeCtx(const std::string& create_ddl,
                                                  const std::string& sql,
                                                  ArrowArray* array) {
  auto translators = initSqlToTranslators(sql, create_ddl);

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
  query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

  return runtime_ctx;
}

class NonGroupbyAggTest : public ::testing::Test {
 public:
  void executeTestBuffer(const std::string& create_ddl,
                         const std::string& sql,
                         ArrowArray* array) {
    auto runtime_ctx = executeAndReturnRuntimeCtx(create_ddl, sql, array);

    auto buffer = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
        runtime_ctx->getContextItem(1));
    auto under_buffer64 = reinterpret_cast<int64_t*>(buffer->getBuffer());
    EXPECT_EQ(under_buffer64[0], 22);
    auto under_buffer32 = reinterpret_cast<int32_t*>(buffer->getBuffer() + 8);
    EXPECT_EQ(under_buffer32[0], 1293);
  }

  template <typename COL_TYPE>
  void executeTestResult(const std::string& create_ddl,
                         const std::string& sql,
                         ArrowArray* array,
                         std::vector<COL_TYPE> res) {
    auto runtime_ctx = executeAndReturnRuntimeCtx(create_ddl, sql, array);

    auto output_batch_array = runtime_ctx->getNonGroupByAggOutputBatch()->getArray();
    EXPECT_EQ(output_batch_array->length, 1);

    for (size_t i = 0; i < output_batch_array->n_children; i++) {
      check_array<COL_TYPE>(output_batch_array->children[i], 1, {res[i]});
    }
  }
};

TEST_F(NonGroupbyAggTest, TestBuffer) {
  auto input_builder = ArrowArrayBuilder();
  auto&& [_, input_data] =
      input_builder.setRowNum(10)
          .addColumn<int64_t>(
              "a",
              CREATE_SUBSTRAIT_TYPE(I64),
              {1, 2, 3, 1, 2, 4, 1, 2, 3, 4},
              {true, false, false, false, false, false, false, false, false, false})
          .addColumn<int32_t>(
              "b", CREATE_SUBSTRAIT_TYPE(I32), {1, 11, 111, 2, 22, 222, 3, 33, 333, 555})
          .build();

  executeTestBuffer("CREATE TABLE test(a BIGINT, b INT);",
                    "select sum(a), sum(b) from test",
                    input_data);
}

TEST_F(NonGroupbyAggTest, TestResultSumInt32NotNull) {
  auto input_builder = ArrowArrayBuilder();
  auto [_, input_data] =
      input_builder.setRowNum(10)
          .addColumn<int32_t>(
              "a",
              CREATE_SUBSTRAIT_TYPE(I32),
              {1, 2, 3, 1, 2, 4, 1, 2, 3, 4},
              {true, false, false, false, false, false, false, false, false, false})
          .addColumn<int32_t>(
              "b", CREATE_SUBSTRAIT_TYPE(I32), {1, 11, 111, 2, 22, 222, 3, 33, 333, 555})
          .build();

  executeTestResult<int32_t>("CREATE TABLE test(a INT NOT NULL, b INT NOT NULL);",
                             "select sum(a), sum(b) from test",
                             input_data,
                             {23, 1293});
}

TEST_F(NonGroupbyAggTest, TestResultSumInt64Nullable) {
  auto input_builder = ArrowArrayBuilder();
  auto [_, input_data] =
      input_builder.setRowNum(10)
          .addColumn<int64_t>(
              "a",
              CREATE_SUBSTRAIT_TYPE(I64),
              {1, 2, 3, 1, 2, 4, 1, 2, 3, 4},
              {true, false, false, false, false, false, false, false, false, false})
          .addColumn<int64_t>(
              "b", CREATE_SUBSTRAIT_TYPE(I64), {1, 11, 111, 2, 22, 222, 3, 33, 333, 555})
          .build();

  executeTestResult<int64_t>("CREATE TABLE test(a BIGINT, b BIGINT);",
                             "select sum(a), sum(b) from test",
                             input_data,
                             {22, 1293});
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err = RUN_ALL_TESTS();
  return err;
}
