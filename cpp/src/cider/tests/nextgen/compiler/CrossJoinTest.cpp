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
#include "exec/nextgen/context/Batch.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/Utils.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CrossJoinTest : public ::testing::Test {
 public:
  template <typename COL_TYPE = int64_t>
  void executeTest(const std::string& ddl,
                   const std::string& sql,
                   const std::vector<std::vector<COL_TYPE>>& expected_res,
                   context::Batch& build_array) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, ddl);
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
    cider::CompilationOptions co;
    co.dump_ir = true;
    auto module = cider::jitlib::LLVMJITModule("test", true, co);
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
    auto input_builder = ArrowArrayBuilder();
    auto&& [schema, array] =
        input_builder.setRowNum(3)
            .addColumn<COL_TYPE>(
                "l_a", CREATE_SUBSTRAIT_TYPE(I64), {2, 3, 4}, {false, false, false})
            .template addColumn<COL_TYPE>(
                "l_b", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3}, {true, false, true})
            .template addColumn<COL_TYPE>(
                "l_c", CREATE_SUBSTRAIT_TYPE(I64), {222, 333, 444})
            .build();

    codegen_ctx.setBuildTable(std::make_shared<context::Batch>(build_array));
    auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    // check
    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();

    CHECK(!expected_res.empty());
    size_t expected_row_len = expected_res[0].size();

    EXPECT_EQ(output_batch_array->length, expected_row_len);
    auto check_array = [expected_row_len](ArrowArray* array,
                                          const std::vector<COL_TYPE>& expected_cols) {
      EXPECT_EQ(array->length, expected_row_len);
      COL_TYPE* data_buffer = (COL_TYPE*)array->buffers[1];
      for (size_t i = 0; i < expected_row_len; ++i) {
        EXPECT_EQ(data_buffer[i], expected_cols[i]);
      }
    };

    for (size_t i = 0; i < expected_res.size(); ++i) {
      check_array(output_batch_array->children[i], expected_res[i]);
    }
  }
};

TEST_F(CrossJoinTest, basicINT32NotNullTest) {
  GTEST_SKIP_("Hasn't supported yet");
  auto build_table = ArrowArrayBuilder();
  auto&& [build_schema, build_array] =
      build_table.setRowNum(3)
          .addColumn<int32_t>(
              "r_a", CREATE_SUBSTRAIT_TYPE(I64), {3, 4, 5}, {true, false, true})
          .template addColumn<int32_t>(
              "r_b", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3}, {false, false, false})
          .template addColumn<int32_t>("r_c", CREATE_SUBSTRAIT_TYPE(I64), {333, 444, 555})
          .build();
  context::Batch build_batch(*build_schema, *build_array);
  std::vector<std::vector<int32_t>> expected_res = {
      {2, 2, 2, 3, 3, 3, 4, 4, 4},
      {1, 1, 1, 2, 2, 2, 3, 3, 3},
      {222, 222, 222, 333, 333, 333, 444, 444, 444},
      {3, 4, 5, 3, 4, 5, 3, 4, 5},
      {1, 2, 3, 1, 2, 3, 1, 2, 3},
      {333, 444, 555, 333, 444, 555, 333, 444, 555}};
  std::string ddl =
      "CREATE TABLE table_probe(l_a INTEGER NOT NULL, l_b INTEGER NOT NULL, l_c INTEGER "
      "NOT NULL);"
      "CREATE TABLE table_build(r_a INTEGER NOT NULL, r_b INTEGER NOT NULL, r_c INTEGER "
      "NOT NULL);";
  executeTest(
      ddl, "select * from table_probe cross join table_build", expected_res, build_batch);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
