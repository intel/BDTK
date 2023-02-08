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
#include <fstream>

#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/jsons/";
}

std::string get_json_data(std::string file_name) {
  auto json_file = getDataFilesPath() + file_name;
  std::ifstream sub_json(json_file);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  return buffer.str();
}

class CiderExprNextgenTest : public ::testing::Test {
 public:
  void executeTest(const std::string& create_ddl,
                   const std::string& sql,
                   const int32_t& expected_length,
                   const bool is_file = false) {
    // SQL Parsing
    auto json = is_file ? get_json_data(sql) : RunIsthmus::processSql(sql, create_ddl);
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
    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array_);
    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();

    EXPECT_EQ(output_batch_array->length, expected_length);
    auto check_array = [](ArrowArray* array, size_t expect_len) {
      EXPECT_EQ(array->length, expect_len);
      int32_t* data_buffer = (int32_t*)array->buffers[1];
      for (size_t i = 0; i < expect_len; ++i) {
        std::cout << data_buffer[i] << " ";
      }
      std::cout << std::endl;
    };
    check_array(output_batch_array->children[0], expected_length);
  }

 public:
  ArrowArray* array_ = nullptr;
};

TEST_F(CiderExprNextgenTest, logicalOpTest) {
  std::vector<bool> vec0{true, true, true, false, true, true, false, false};
  std::vector<bool> vec1{true, true, true, false, false, false, true, true};
  std::vector<int32_t> vec2{1, 2, 3, 4, 5, 6, 7, 8};
  std::vector<bool> vec_null0{false, true, true, false, true, false, false, false};
  std::vector<bool> vec_null1{false, true, false, true, false, false, true, false};
  std::vector<bool> vec_null2{false, false, false, false, false, false, false, false};

  // AND:                     true,  NULL, NULL, false, false, false, false, false
  // OR:                      true,  NULL, true, NULL,  NULL, true, NULL, true
  ArrowSchema* schema = nullptr;
  std::tie(schema, array_) =
      ArrowArrayBuilder()
          .setRowNum(8)
          .addBoolColumn<bool>("col_int_a", vec0, vec_null0)
          .addBoolColumn<bool>("col_int_b", vec1, vec_null1)
          .addColumn<int>("col_int_c", CREATE_SUBSTRAIT_TYPE(I32), vec2, vec_null2)
          .build();
  executeTest(
      "CREATE TABLE test(col_int_a BOOLEAN, col_int_b BOOLEAN, col_int_c INTEGER);",
      "select col_int_a from test",
      8);
  executeTest(
      "CREATE TABLE test(col_int_a BOOLEAN, col_int_b BOOLEAN, col_int_c INTEGER);",
      "select col_int_c from test where col_int_a and col_int_b",
      1);
  executeTest(
      "CREATE TABLE test(col_int_a BOOLEAN, col_int_b BOOLEAN, col_int_c INTEGER);",
      "select col_int_c from test where col_int_a or col_int_b",
      4);
}

TEST_F(CiderExprNextgenTest, isNullAndDistinctFromTest) {
  std::vector<int> vec0{1, 3, 2, 1, 6};
  std::vector<int> vec1{1, 2, 3, 4, 5};
  std::vector<bool> vec_null{false, false, true, false, true};
  ArrowSchema* schema = nullptr;
  std::tie(schema, array_) =
      ArrowArrayBuilder()
          .setRowNum(5)
          .addColumn<int>("col_int_a", CREATE_SUBSTRAIT_TYPE(I32), vec0, vec_null)
          .addColumn<int>("col_int_b", CREATE_SUBSTRAIT_TYPE(I32), vec1, vec_null)
          .build();
  executeTest("CREATE TABLE test(col_int_a INTEGER, col_int_b INTEGER);",
              "select col_int_b from test where col_int_a IS NULL",
              2);
  executeTest("CREATE TABLE test(col_int_a INTEGER, col_int_b INTEGER);",
              "select col_int_b from test where col_int_a IS NOT NULL",
              3);
  executeTest(
      "CREATE TABLE test(col_int_a INTEGER NOT NULL, col_int_b INTEGER NOT NULL);",
      "select col_int_b from test where col_int_a IS NULL",
      0);
  executeTest(
      "CREATE TABLE test(col_int_a INTEGER NOT NULL, col_int_b INTEGER NOT NULL);",
      "select col_int_b from test where col_int_a IS NOT NULL",
      5);
  executeTest("CREATE TABLE test(col_int_a INTEGER, col_int_b INTEGER);",
              "is_distinct_from.json",
              2,
              true);
  executeTest("CREATE TABLE test(col_int_a INTEGER, col_int_b INTEGER);",
              "is_not_distinct_from.json",
              3,
              true);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
