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
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;
using namespace cider::test::util;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class NextgenCompilerTest : public ::testing::Test {
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
    cider::jitlib::CompilationOptions co;
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
    auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);
    auto input_builder = ArrowArrayBuilder();
    auto&& [schema, array] =
        input_builder.setRowNum(10)
            .addColumn<int64_t>(
                "a", CREATE_SUBSTRAIT_TYPE(I64), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
            .addColumn<int64_t>(
                "b", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
            .build();

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();
    EXPECT_EQ(output_batch_array->length, 5);

    auto check_array = [](ArrowArray* array, size_t expect_len) {
      EXPECT_EQ(array->length, expect_len);
      int64_t* data_buffer = (int64_t*)array->buffers[1];
      for (size_t i = 0; i < expect_len; ++i) {
        std::cout << data_buffer[i] << " ";
      }
      std::cout << std::endl;
    };

    check_array(output_batch_array->children[0], 5);
    check_array(output_batch_array->children[1], 5);
  }

 private:
  std::string create_ddl_ = "CREATE TABLE test(a BIGINT, b BIGINT NOT NULL);";
};

TEST_F(NextgenCompilerTest, FrameworkTest1) {
  executeTest("select a + b, a - b from test where a < b");
}

TEST_F(NextgenCompilerTest, FrameworkTest2) {
  executeTest("select a + b, a - b from test");
}

class CiderNextgenCompilerTestBase : public CiderNextgenTestBase {
 public:
  CiderNextgenCompilerTestBase() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_1 BIGINT NOT NULL, col_2 BIGINT NOT NULL, col_3 BIGINT "
        "NOT NULL)";
    QueryArrowDataGenerator::generateBatchByTypes(input_schema_,
                                                  input_array_,
                                                  99,
                                                  {"col_1", "col_2", "col_3"},
                                                  {CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(I64),
                                                   CREATE_SUBSTRAIT_TYPE(I64)});
  }
};

TEST_F(CiderNextgenCompilerTestBase, integerFilterTest) {
  assertQuery("SELECT col_1 + col_2 FROM test WHERE col_1 <= col_2");
}

TEST_F(CiderNextgenCompilerTestBase, noGroupbyTest) {
  assertQuery("SELECT SUM(col_1), SUM(col_2) FROM test WHERE col_1 <= col_2");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
