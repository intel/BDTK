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
#include "exec/nextgen/context/Batch.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

class HashJoinTest : public ::testing::Test {
 public:
  template <typename COL_TYPE = int64_t>
  void executeTest(const std::string& ddl,
                   const std::string& sql,
                   const std::vector<std::vector<COL_TYPE>> expected_res,
                   context::Batch& build_batch) {
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
    auto input_builder = ArrowArrayBuilder();
    auto&& [schema, array] =
        input_builder.setRowNum(4)
            .addColumn<COL_TYPE>("l_a",
                                 CREATE_SUBSTRAIT_TYPE(I64),
                                 {3, 4, 5, 6},
                                 {false, false, false, true})
            .template addColumn<COL_TYPE>("l_b",
                                          CREATE_SUBSTRAIT_TYPE(I64),
                                          {1, 2, 3, 4},
                                          {true, false, true, false})
            .template addColumn<COL_TYPE>(
                "l_c", CREATE_SUBSTRAIT_TYPE(I64), {555, 666, 777, 888})
            .build();

    cider::exec::processor::JoinHashTable hm;
    auto join_key = build_batch.getArray()->children[1];
    for (int64_t i = 0; i < join_key->length; i++) {
      // if (!*((reinterpret_cast<bool*>(const_cast<void*>(join_key->buffers[0]))) + i)) {
      int key =
          *((reinterpret_cast<COL_TYPE*>(const_cast<void*>(join_key->buffers[1]))) + i);
      hm.emplace(key, {&build_batch, i});
      // }
    }

    // TODO(Xinyi) : sethashtable in velox hashjoinbuild
    auto tmp = hm.findAll(1);
    codegen_ctx.setHashTable(hm);
    auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    // check
    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();

    CHECK(!expected_res.empty());
    size_t expected_row_len = expected_res[0].size();

    // for test only, to print result
    auto print_array = [](ArrowArray* array, size_t expect_len) {
      // EXPECT_EQ(array->length, expect_len);
      int64_t* data_buffer = (int64_t*)array->buffers[1];
      for (size_t i = 0; i < array->length; ++i) {
        std::cout << data_buffer[i] << " ";
      }
      std::cout << std::endl;
    };
    print_array(output_batch_array->children[0], 4);
    print_array(output_batch_array->children[1], 4);
    print_array(output_batch_array->children[2], 4);
    print_array(output_batch_array->children[3], 4);

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

TEST_F(HashJoinTest, basicINT32NotNullTest) {
  auto build_table = ArrowArrayBuilder();
  auto&& [build_schema, build_array] =
      build_table.setRowNum(4)
          .addColumn<int32_t>(
              "r_a", CREATE_SUBSTRAIT_TYPE(I64), {6, 7, 8, 9}, {true, false, true, false})
          .template addColumn<int32_t>("r_b",
                                       CREATE_SUBSTRAIT_TYPE(I64),
                                       {1, 2, 3, 4},
                                       {false, false, false, false})
          .template addColumn<int32_t>(
              "r_c", CREATE_SUBSTRAIT_TYPE(I64), {666, 777, 888, 999})
          .build();
  context::Batch build_batch(*build_schema, *build_array);
  std::vector<std::vector<int32_t>> expected_res = {{3, 4, 5, 6},
                                                    {1, 2, 3, 4},
                                                    {555, 666, 777, 888},
                                                    {6, 7, 8, 9},
                                                    {1, 2, 3, 4},
                                                    {666, 777, 888, 999}};
  std::string ddl =
      "CREATE TABLE table_probe(l_a INTEGER NOT NULL, l_b INTEGER NOT NULL, l_c INTEGER "
      "NOT NULL);"
      "CREATE TABLE table_build(r_a INTEGER NOT NULL, r_b INTEGER NOT NULL, r_c INTEGER "
      "NOT NULL);";
  executeTest(ddl,
              "select * from table_probe join table_build on table_probe.l_b = "
              "table_build.r_b",
              expected_res,
              build_batch);
}

TEST_F(HashJoinTest, basicINT64NullableTest) {
  auto build_table = ArrowArrayBuilder();
  auto&& [build_schema, build_array] =
      build_table.setRowNum(4)
          .addColumn<int32_t>(
              "r_a", CREATE_SUBSTRAIT_TYPE(I64), {6, 7, 8, 9}, {true, false, true, false})
          .template addColumn<int32_t>("r_b",
                                       CREATE_SUBSTRAIT_TYPE(I64),
                                       {1, 2, 3, 4},
                                       {false, false, false, false})
          .template addColumn<int32_t>(
              "r_c", CREATE_SUBSTRAIT_TYPE(I64), {666, 777, 888, 999})
          .build();
  context::Batch build_batch(*build_schema, *build_array);
  std::vector<std::vector<int32_t>> expected_res = {
      {4, 6}, {2, 4}, {666, 888}, {7, 9}, {2, 4}, {777, 999}};
  std::string ddl =
      "CREATE TABLE table_probe(l_a INTEGER, l_b INTEGER, l_c INTEGER "
      "NOT NULL);"
      "CREATE TABLE table_build(r_a INTEGER, r_b INTEGER, r_c INTEGER "
      "NOT NULL);";
  executeTest(ddl,
              "select * from table_probe join table_build on table_probe.l_b = "
              "table_build.r_b",
              expected_res,
              build_batch);
}

TEST_F(HashJoinTest, basicINT64Test) {
  auto build_table = ArrowArrayBuilder();
  auto&& [build_schema, build_array] =
      build_table.setRowNum(4)
          .addColumn<int64_t>(
              "r_a", CREATE_SUBSTRAIT_TYPE(I64), {6, 7, 8, 9}, {true, false, true, false})
          .template addColumn<int64_t>("r_b",
                                       CREATE_SUBSTRAIT_TYPE(I64),
                                       {1, 2, 3, 4},
                                       {false, false, true, false})
          .template addColumn<int64_t>(
              "r_c", CREATE_SUBSTRAIT_TYPE(I64), {666, 777, 888, 999})
          .build();
  context::Batch build_batch(*build_schema, *build_array);
  std::vector<std::vector<int64_t>> expected_res = {
      {3, 4, 5, 6}, {555, 666, 777, 888}, {1, 2, 3, 4}, {666, 777, 888, 999}};
  std::string ddl =
      "CREATE TABLE table_probe(l_a BIGINT NOT NULL, l_b BIGINT NOT NULL, l_c BIGINT NOT "
      "NULL);"
      "CREATE TABLE table_build(r_a BIGINT NOT NULL, r_b BIGINT NOT NULL, r_c BIGINT NOT "
      "NULL);";
  executeTest(
      ddl,
      "select l_a, l_c, r_b, r_c from table_probe join table_build on table_probe.l_b = "
      "table_build.r_b",
      expected_res,
      build_batch);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
