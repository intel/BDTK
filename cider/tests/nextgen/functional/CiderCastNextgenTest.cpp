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

class CiderCastNextgenTest : public ::testing::Test {
 public:
  void executeTest(ArrowArray* array,
                   const std::string& create_ddl,
                   const std::string& sql,
                   std::vector<size_t> check_bytes,
                   std::vector<int8_t*> check_vecs) {
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

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();
    // check value only
    for (int i = 0; i < check_vecs.size(); i++) {
      const int8_t* out_buf =
          reinterpret_cast<const int8_t*>(output_batch_array->children[i]->buffers[1]);
      EXPECT_EQ(0, memcmp(check_vecs[i], out_buf, check_bytes[i]));
    }
  }
};

TEST_F(CiderCastNextgenTest, castFloatTest) {
  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;
  std::vector<float> col_float{1.1, 2.1, 3.1, 4.1, 5.1};
  std::vector<double> col_double{1.1, 1.9, 2.8, 3.7, 4.9};
  std::vector<int32_t> col_int{1, 2, 3, 4, 5};
  const int32_t row_num = 5;
  std::vector<bool> vec_null(row_num, false);
  std::tie(schema, array) =
      ArrowArrayBuilder()
          .setRowNum(row_num)
          .addColumn<float>("col_float", CREATE_SUBSTRAIT_TYPE(Fp32), col_float, vec_null)
          .addColumn<double>(
              "col_double", CREATE_SUBSTRAIT_TYPE(Fp64), col_double, vec_null)
          .addColumn<int32_t>("col_int", CREATE_SUBSTRAIT_TYPE(I32), col_int, vec_null)
          .build();

  std::string ddl =
      "CREATE TABLE test(col_float FLOAT, col_double DOUBLE, col_int INTEGER);";

  executeTest(array,
              ddl,
              "select cast(col_float as int), cast(col_double as int) from test",
              {row_num * sizeof(int32_t), row_num * sizeof(int32_t)},
              {reinterpret_cast<int8_t*>(col_int.data()),
               reinterpret_cast<int8_t*>(col_int.data())});
}

TEST_F(CiderCastNextgenTest, castIntegerTest) {
  ArrowArray* array = nullptr;
  ArrowSchema* schema = nullptr;
  std::vector<int8_t> col_tinyint{1, 2, 3, 4, 5};
  std::vector<int64_t> col_bigint{1, 2, 3, 4, 5};
  std::vector<int32_t> col_int{1, 2, 3, 4, 5};
  const int32_t row_num = 5;
  std::vector<bool> vec_null(row_num, false);
  std::tie(schema, array) =
      ArrowArrayBuilder()
          .setRowNum(row_num)
          .addColumn<int8_t>(
              "col_tinyint", CREATE_SUBSTRAIT_TYPE(I8), col_tinyint, vec_null)
          .addColumn<int64_t>(
              "col_bigint", CREATE_SUBSTRAIT_TYPE(I64), col_bigint, vec_null)
          .addColumn<int32_t>("col_int", CREATE_SUBSTRAIT_TYPE(I32), col_int, vec_null)
          .build();

  std::string ddl =
      "CREATE TABLE test(col_tinyint TINYINT, col_bigint BIGINT, col_int INTEGER);";

  executeTest(
      array,
      ddl,
      "select cast(col_tinyint as int), cast(col_bigint as int), cast(col_int as bigint) "
      "from test",
      {row_num * sizeof(int32_t), row_num * sizeof(int32_t), row_num * sizeof(int64_t)},
      {reinterpret_cast<int8_t*>(col_int.data()),
       reinterpret_cast<int8_t*>(col_int.data()),
       reinterpret_cast<int8_t*>(col_bigint.data())});
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
