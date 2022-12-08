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

template <typename T>
void check_arrow_array(ArrowArray* array, const std::vector<T>& expected_res) {
  EXPECT_EQ(array->length, expected_res.size());
  T* data_buffer = (T*)array->buffers[1];
  for (size_t i = 0; i < expected_res.size(); ++i) {
    EXPECT_EQ(data_buffer[i], expected_res[i]);
  }
}

class HashJoinTest : public ::testing::Test {
 public:
  void executeTest(const std::string& sql) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, create_ddl_ + " " + build_ddl_);
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
            .addColumn<int8_t>("l_a", CREATE_SUBSTRAIT_TYPE(I8), {3, 4, 5, 6})
            .addColumn<int32_t>("l_b", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4})
            .addColumn<int64_t>("l_c", CREATE_SUBSTRAIT_TYPE(I64), {555, 666, 777, 888})
            .build();
    auto build_table = ArrowArrayBuilder();
    auto&& [build_schema, build_array] =
        build_table.setRowNum(4)
            .addColumn<int8_t>("r_a", CREATE_SUBSTRAIT_TYPE(I8), {6, 7, 8, 9})
            .addColumn<int32_t>("r_b", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4})
            .addColumn<int64_t>("r_c", CREATE_SUBSTRAIT_TYPE(I64), {666, 777, 888, 999})
            .build();

    context::Batch build_batch(*build_schema, *build_array);
    auto batch = new context::Batch(build_batch);

    cider_hashtable::LinearProbeHashTable<int,
                                          std::pair<context::Batch*, int64_t>,
                                          context::murmurHash,
                                          context::Equal>
        hm(16, 0);
    auto join_key = batch->getArray()->children[1];
    for (int64_t i = 0; i < join_key->length; i++) {
      int key =
          *((reinterpret_cast<int32_t*>(const_cast<void*>(join_key->buffers[1]))) + i);
      hm.insert(key, std::make_pair(batch, i));
    }

    // TODO(Xinyi) : sethashtable in velox hashjoinbuild
    codegen_ctx.setHashTable(hm);
    auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);

    query_func((int8_t*)runtime_ctx.get(), (int8_t*)array);

    // check
    CHECK(batch->getArray());
    auto expected_row_len = batch->getArray()->children[0]->length;
    auto output_batch_array = runtime_ctx->getOutputBatch()->getArray();

    EXPECT_EQ(output_batch_array->length, expected_row_len);
    check_arrow_array<int8_t>(output_batch_array->children[0], {3, 4, 5, 6});
    check_arrow_array<int32_t>(output_batch_array->children[1], {1, 2, 3, 4});
    check_arrow_array<int64_t>(output_batch_array->children[2], {555, 666, 777, 888});
    check_arrow_array<int8_t>(output_batch_array->children[3], {6, 7, 8, 9});
    check_arrow_array<int32_t>(output_batch_array->children[4], {1, 2, 3, 4});
    check_arrow_array<int64_t>(output_batch_array->children[5], {666, 777, 888, 999});

    delete batch;
  }

 private:
  std::string create_ddl_ =
      "CREATE TABLE table_probe(l_a TINYINT NOT NULL, l_b INTEGER NOT NULL, l_c BIGINT "
      "NOT NULL);";
  std::string build_ddl_ =
      "CREATE TABLE table_build(r_a TINYINT NOT NULL, r_b INTEGER NOT NULL, r_c BIGINT "
      "NOT NULL);";
};

TEST_F(HashJoinTest, basicTest) {
  executeTest(
      "select * from table_probe join table_build on table_probe.l_b = "
      "table_build.r_b");
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);
  testing::InitGoogleTest(&argc, argv);
  int err = RUN_ALL_TESTS();
  return err;
}
