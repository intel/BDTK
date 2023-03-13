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

#include <gflags/gflags.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::exec::nextgen;
using namespace cider::test::util;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class NextgenCompilerTest : public ::testing::Test {
 public:
  template <typename T>
  void executeTest(const std::string& sql, T&& checker) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, create_ddl_);
    ::substrait::Plan plan;
    google::protobuf::util::JsonStringToMessage(json, &plan);

    generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
    auto eu = substrait2eu.createRelAlgExecutionUnit();

    // Pipeline Building
    auto pipeline = parsers::toOpPipeline(eu);
    transformer::Transformer transformer;
    context::CodegenOptions codegen_co;
    codegen_co.enable_vectorize = true;
    auto translators = transformer.toTranslator(pipeline, codegen_co);

    // Codegen
    context::CodegenContext codegen_ctx;
    cider::jitlib::CompilationOptions co;
    co.enable_vectorize = true;
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
            .addBoolColumn<bool>(
                "c", {true, false, false, true, true, false, false, true, true, false})
            .addBoolColumn<bool>(
                "d",
                {true, false, true, false, true, false, true, false, true, false},
                {false, false, false, false, true, true, true, true, false, false})
            .build();

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();

    checker(output_batch_array);
  }

 private:
  std::string create_ddl_ =
      "CREATE TABLE test(a BIGINT, b BIGINT NOT NULL, c BOOLEAN NOT NULL, d BOOLEAN);";
};

void integerChecker(ArrowArray* array, int expected_len) {
  EXPECT_EQ(array->length, expected_len);

  auto check_array = [](ArrowArray* array, size_t expect_len) {
    EXPECT_EQ(array->length, expect_len);
    int64_t* data_buffer = (int64_t*)array->buffers[1];
    for (size_t i = 0; i < expect_len; ++i) {
      std::cout << data_buffer[i] << " ";
    }
    std::cout << std::endl;
  };

  check_array(array->children[0], expected_len);
  check_array(array->children[1], expected_len);
}

TEST_F(NextgenCompilerTest, FrameworkTest1) {
  executeTest("select a + b, a - b from test where a < b",
              [](ArrowArray* array) { integerChecker(array, 5); });
}

TEST_F(NextgenCompilerTest, FrameworkTest2) {
  executeTest("select a + b, a - b from test",
              [](ArrowArray* array) { integerChecker(array, 10); });
}

void boolChecker(ArrowArray* array, int expected_len) {
  EXPECT_EQ(array->length, expected_len);

  auto check_array = [](ArrowArray* array, size_t expect_len) {
    EXPECT_EQ(array->length, expect_len);
    uint8_t* data_buffer = (uint8_t*)array->buffers[1];
    uint8_t* null_buffer = (uint8_t*)array->buffers[0];
    if (null_buffer) {
      // Nullable
      for (size_t i = 0; i < expect_len; ++i) {
        if (CiderBitUtils::isBitSetAt(null_buffer, i)) {
          std::cout << (CiderBitUtils::isBitSetAt(data_buffer, i) ? "T" : "F") << " ";
        } else {
          std::cout << "N"
                    << " ";
        }
      }
    } else {
      // Not nullable
      for (size_t i = 0; i < expect_len; ++i) {
        std::cout << (CiderBitUtils::isBitSetAt(data_buffer, i) ? "T" : "F") << " ";
      }
    }
    std::cout << std::endl;
  };

  for (size_t i = 0; i < array->n_children; ++i) {
    check_array(array->children[i], expected_len);
  }
}

TEST_F(NextgenCompilerTest, FrameworkTest3) {
  executeTest("select c AND d, c OR d, NOT c, NOT d from test",
              [](ArrowArray* array) { boolChecker(array, 10); });
}

TEST_F(NextgenCompilerTest, FrameworkTest4) {
  executeTest("select b > a AND c from test",
              [](ArrowArray* array) { boolChecker(array, 10); });
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

TEST_F(CiderNextgenCompilerTestBase, multiExpressions) {
  assertQuery("SELECT (col_1 + col_2) * col_3, (col_1 + col_2) + 2 FROM test");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
