/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#include "tests/utils/CiderInt128.h"
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::exec::nextgen;
using namespace cider::test::util;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class DecimalCompilerTest : public ::testing::Test {};

template <typename NativeType>
void executeBinOpTest(const std::string& create_ddl,
                      const std::string& sql,
                      std::vector<NativeType> operand_a,
                      std::vector<NativeType> operand_b,
                      std::vector<std::string> expected_c) {
  // SQL Parsing
  auto json = RunIsthmus::processSql(sql, create_ddl);
  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);

  generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
  auto eu = substrait2eu.createRelAlgExecutionUnit();

  // Pipeline Building
  context::CodegenContext codegen_ctx;
  auto pipeline = parsers::toOpPipeline(eu, codegen_ctx);
  transformer::Transformer transformer;
  auto translators = transformer.toTranslator(pipeline);

  // Codegen
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
      input_builder.setRowNum(operand_a.size())
          .template addColumn<__int128_t>("a", CREATE_SUBSTRAIT_TYPE(Decimal), operand_a)
          .template addColumn<__int128_t>("b", CREATE_SUBSTRAIT_TYPE(Decimal), operand_b)
          .build();

  runtime_ctx->resetBatch(allocator);
  query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

  auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();
  auto output_batch_schema = runtime_ctx->getOutputBatch()->getSchema();

  NativeType* data_buffer = (NativeType*)output_batch_array->children[0]->buffers[1];
  const char* format_str = output_batch_schema->children[0]->format;
  std::string::size_type sz;
  int precision = std::stoi(&format_str[2], &sz);
  int scale = std::stoi(&format_str[2 + sz + 1], &sz);
  for (size_t i = 0; i < expected_c.size(); ++i) {
    EXPECT_EQ(expected_c[i],
              CiderInt128Utils::Decimal128ToString(data_buffer[i], precision, scale));
  }
}

TEST_F(DecimalCompilerTest, DecimalArithmeticTest) {
  std::string create_ddl =
      "CREATE TABLE test(a DECIMAL(13,1) NOT NULL, b DECIMAL(13,2) NOT NULL);";
  // operand unscaled value
  std::vector<__int128_t> a_uv = {1, 2, 3, 4, 5};
  std::vector<__int128_t> b_uv = {10, 20, 30, 40, 50};
  // expected real value
  std::vector<std::string> add_rv = {"0.20", "0.40", "0.60", "0.80", "1.00"};
  std::vector<std::string> sub_rv = {"0.00", "0.00", "0.00", "0.00", "0.00"};
  std::vector<std::string> mul_rv = {"0.010", "0.040", "0.090", "0.160", "0.250"};
  std::vector<std::string> div_rv = {
      "1.00000", "1.00000", "1.00000", "1.00000", "1.00000"};
  executeBinOpTest(create_ddl, "select a + b from test", a_uv, b_uv, add_rv);
  executeBinOpTest(create_ddl, "select a - b from test", a_uv, b_uv, sub_rv);
  executeBinOpTest(create_ddl, "select a * b from test", a_uv, b_uv, mul_rv);
  executeBinOpTest(create_ddl, "select a / b from test", a_uv, b_uv, div_rv);
}

TEST_F(DecimalCompilerTest, DecimalCompareTest) {
  std::string create_ddl =
      "CREATE TABLE test(a DECIMAL(13,1) NOT NULL, b DECIMAL(13,2) NOT NULL);";
  // operand unscaled value
  std::vector<__int128_t> a_uv = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<__int128_t> b_uv = {0, 10, 20, 30, 40, 51, 61, 71, 81, 91};
  // expected real value
  std::vector<std::string> lt_rv = {"0.255", "0.366", "0.497", "0.648", "0.819"};
  std::vector<std::string> ge_rv = {"0.00", "0.11", "0.24", "0.39", "0.56"};
  executeBinOpTest(create_ddl, "select a * b from test where a < b", a_uv, b_uv, lt_rv);
  executeBinOpTest(
      create_ddl, "select a * a + b from test where a >= b", a_uv, b_uv, ge_rv);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
