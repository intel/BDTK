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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "exec/nextgen/Nextgen.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"

#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"
#include "util/Logger.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class CiderArrayTest : public ::testing::Test {
 public:
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
};

TEST_F(CiderArrayTest, ArrayTestWithNull) {
  //[[1,2,null,4,5],null]
  auto input_builder = ArrowArrayBuilder();
  auto [_, input_data] =
      input_builder.setRowNum(2)
          .addSingleDimensionArrayColumn<int8_t>(
              "a",
              CREATE_SUBSTRAIT_LIST_TYPE(I8),
              {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}},
              {false, true},
              {{false, false, true, false, false}, {true, false, true, false, true}})
          .build();
  auto runtime_ctx = executeAndReturnRuntimeCtx(
      "CREATE TABLE test(a TINYINT ARRAY);", "select * from test", input_data);
  auto output_batch = runtime_ctx->getOutputBatch();
  auto output_array = output_batch->getArray();
  auto output_schema = output_batch->getSchema();

  EXPECT_EQ(std::string(output_schema->format), "+s");
  EXPECT_EQ(output_schema->n_children, 1);
  EXPECT_EQ(std::string(output_schema->children[0]->format), "+l");
  EXPECT_EQ(output_schema->children[0]->n_children, 1);
  EXPECT_EQ(std::string(output_schema->children[0]->children[0]->format), "c");
  EXPECT_EQ(output_schema->children[0]->children[0]->n_children, 0);

  EXPECT_EQ(output_array->length, 2);
  EXPECT_EQ(output_array->n_buffers, 1);
  EXPECT_EQ(output_array->n_children, 1);
  EXPECT_EQ(output_array->children[0]->length, 2);
  EXPECT_EQ(output_array->children[0]->n_buffers, 2);
  EXPECT_EQ(output_array->children[0]->n_children, 1);
  EXPECT_TRUE(
      CiderBitUtils::isBitSetAt((uint8_t*)(output_array->children[0]->buffers[0]), 0));
  EXPECT_TRUE(
      CiderBitUtils::isBitClearAt((uint8_t*)(output_array->children[0]->buffers[0]), 1));
  EXPECT_EQ(output_array->children[0]->null_count, 1);
  int32_t* offsets = (int32_t*)(output_array->children[0]->buffers[1]);
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 5);
  EXPECT_EQ(offsets[2], 5);  // The second row is null
  EXPECT_EQ(output_array->children[0]->children[0]->length, 5);
  EXPECT_EQ(output_array->children[0]->children[0]->n_buffers, 2);
  EXPECT_EQ(output_array->children[0]->children[0]->n_children, 0);
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 0));
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 1));
  EXPECT_TRUE(CiderBitUtils::isBitClearAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 2));
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 3));
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 4));
  EXPECT_EQ(output_array->children[0]->children[0]->null_count, 1);
  int8_t* values = (int8_t*)(output_array->children[0]->children[0]->buffers[1]);
  EXPECT_EQ(values[0], 1);
  EXPECT_EQ(values[1], 2);
  EXPECT_EQ(values[3], 4);
  EXPECT_EQ(values[4], 5);
}

TEST_F(CiderArrayTest, ArrayTestWithoutNull) {
  //[[1,2,null,4,5],[null,2,null,4,null],[1,null,3,null]]
  auto input_builder = ArrowArrayBuilder();
  auto [_, input_data] = input_builder.setRowNum(3)
                             .addSingleDimensionArrayColumn<int8_t>(
                                 "a",
                                 CREATE_SUBSTRAIT_LIST_TYPE(I8),
                                 {{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4}},
                                 {false, false, false},
                                 {{false, false, true, false, false},
                                  {true, false, true, false, true},
                                  {false, true, false, true}})
                             .build();
  auto runtime_ctx = executeAndReturnRuntimeCtx(
      "CREATE TABLE test(a TINYINT ARRAY);", "select * from test", input_data);
  auto output_batch = runtime_ctx->getOutputBatch();
  auto output_array = output_batch->getArray();
  auto output_schema = output_batch->getSchema();

  EXPECT_EQ(std::string(output_schema->format), "+s");
  EXPECT_EQ(output_schema->n_children, 1);
  EXPECT_EQ(std::string(output_schema->children[0]->format), "+l");
  EXPECT_EQ(output_schema->children[0]->n_children, 1);
  EXPECT_EQ(std::string(output_schema->children[0]->children[0]->format), "c");
  EXPECT_EQ(output_schema->children[0]->children[0]->n_children, 0);

  EXPECT_EQ(output_array->length, 3);
  EXPECT_EQ(output_array->n_buffers, 1);
  EXPECT_EQ(output_array->n_children, 1);
  EXPECT_EQ(output_array->children[0]->length, 3);
  EXPECT_EQ(output_array->children[0]->n_buffers, 2);
  EXPECT_EQ(output_array->children[0]->n_children, 1);
  EXPECT_TRUE(
      CiderBitUtils::isBitSetAt((uint8_t*)(output_array->children[0]->buffers[0]), 0));
  EXPECT_TRUE(
      CiderBitUtils::isBitClearAt((uint8_t*)(output_array->children[0]->buffers[0]), 1));
  EXPECT_EQ(output_array->children[0]->null_count, 0);
  int32_t* offsets = (int32_t*)(output_array->children[0]->buffers[1]);
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 5);
  EXPECT_EQ(offsets[2], 10);
  EXPECT_EQ(offsets[3], 14);
  EXPECT_EQ(output_array->children[0]->children[0]->length, 14);
  EXPECT_EQ(output_array->children[0]->children[0]->n_buffers, 2);
  EXPECT_EQ(output_array->children[0]->children[0]->n_children, 0);
  EXPECT_EQ(*(uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 0b01011011);
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 8));
  EXPECT_TRUE(CiderBitUtils::isBitClearAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 9));
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 10));
  EXPECT_TRUE(CiderBitUtils::isBitClearAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 11));
  EXPECT_TRUE(CiderBitUtils::isBitSetAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 12));
  EXPECT_TRUE(CiderBitUtils::isBitClearAt(
      (uint8_t*)(output_array->children[0]->children[0]->buffers[0]), 13));
  EXPECT_EQ(output_array->children[0]->children[0]->null_count, 6);
  int8_t* values = (int8_t*)(output_array->children[0]->children[0]->buffers[1]);
  EXPECT_EQ(values[0], 1);
  EXPECT_EQ(values[1], 2);
  EXPECT_EQ(values[3], 4);
  EXPECT_EQ(values[4], 5);
  EXPECT_EQ(values[6], 2);
  EXPECT_EQ(values[8], 4);
  EXPECT_EQ(values[10], 1);
  EXPECT_EQ(values[12], 3);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
