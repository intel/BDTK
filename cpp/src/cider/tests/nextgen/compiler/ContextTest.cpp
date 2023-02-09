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
#include <gflags/gflags.h>
#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/operator/join/CiderJoinHashTable.h"
#include "exec/operator/join/CiderStdUnorderedHashTable.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "type/data/sqltypes.h"

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();
using namespace cider::exec::nextgen::context;

class ContextTests : public ::testing::Test {};

TEST_F(ContextTests, ContextTest) {
  cider::exec::nextgen::context::CodegenContext context;
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
          .addProcedureBuilder([&context](cider::jitlib::JITFunctionPointer func) {
            context.setJITFunction(func);
            auto output_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "output");
            auto loop1_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "loop1_output");
            auto loop2_batch =
                context.registerBatch(SQLTypeInfo(SQLTypes::kBIGINT), "loop2_output");
            func->createReturn();
          })
          .build();
  module.finish();

  auto runtime_ctx = context.generateRuntimeCTX(allocator);
  EXPECT_EQ(runtime_ctx->getContextItemNum(), 3);
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(1), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(2), nullptr);
}

TEST_F(ContextTests, ContextBufferTest) {
  cider::exec::nextgen::context::CodegenContext context;
  auto module = cider::jitlib::LLVMJITModule("test_context_buffer", true);
  cider::jitlib::JITFunctionPointer function =
      cider::jitlib::JITFunctionBuilder()
          .registerModule(module)
          .setFuncName("allocate_buffer")
          .addReturn(cider::jitlib::JITTypeTag::VOID)
          .addParameter(cider::jitlib::JITTypeTag::POINTER,
                        "context",
                        cider::jitlib::JITTypeTag::INT8)
          .addProcedureBuilder([&context](cider::jitlib::JITFunctionPointer func) {
            context.setJITFunction(func);
            auto output_buffer1 = context.registerBuffer(1 * 8, "output_buffer1");
            auto output_buffer2 = context.registerBuffer(2 * 8, "output_buffer2");

            auto cast_output_buffer1 =
                output_buffer1->castPointerSubType(cider::jitlib::JITTypeTag::INT64);
            auto cast_output_buffer2 =
                output_buffer2->castPointerSubType(cider::jitlib::JITTypeTag::INT64);

            auto index_0 = func->createLiteral(cider::jitlib::JITTypeTag::INT32, 0);
            auto index_1 = func->createLiteral(cider::jitlib::JITTypeTag::INT32, 1);

            cast_output_buffer1[index_0] =
                func->createLiteral(cider::jitlib::JITTypeTag::INT64, 1l);
            cast_output_buffer2[index_1] =
                func->createLiteral(cider::jitlib::JITTypeTag::INT64, 2l);

            func->createReturn();
          })
          .build();
  module.finish();

  auto runtime_ctx = context.generateRuntimeCTX(allocator);

  auto query_func = function->getFunctionPointer<void, int8_t*>();
  query_func((int8_t*)runtime_ctx.get());

  EXPECT_EQ(runtime_ctx->getContextItemNum(), 2);
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);
  EXPECT_NE(runtime_ctx->getContextItem(1), nullptr);

  auto output_buffer1 = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
      runtime_ctx->getContextItem(0));
  auto raw_buffer1 = reinterpret_cast<int64_t*>(output_buffer1->getBuffer());
  EXPECT_EQ(raw_buffer1[0], 1);

  auto output_buffer2 = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(
      runtime_ctx->getContextItem(1));
  auto raw_buffer2 = reinterpret_cast<int64_t*>(output_buffer2->getBuffer());
  EXPECT_EQ(raw_buffer2[1], 2);
}

TEST_F(ContextTests, RegisterHashTableTest) {
  CodegenContext codegen_ctx;
  auto module = cider::jitlib::LLVMJITModule("test_register_hashtable", true);
  cider::jitlib::JITFunctionPointer function =
      cider::jitlib::JITFunctionBuilder()
          .registerModule(module)
          .setFuncName("register_hashtable")
          .addReturn(cider::jitlib::JITTypeTag::VOID)
          .addParameter(cider::jitlib::JITTypeTag::POINTER,
                        "context",
                        cider::jitlib::JITTypeTag::INT8)
          .addProcedureBuilder([&codegen_ctx](cider::jitlib::JITFunctionPointer func) {
            codegen_ctx.setJITFunction(func);
            auto ret = codegen_ctx.registerHashTable("hash_table");
            func->createReturn();
          })
          .build();
  module.finish();

  auto query_func = function->getFunctionPointer<void, int8_t*>();

  auto input_builder = ArrowArrayBuilder();
  auto&& [schema, array] =
      input_builder.setRowNum(10)
          .addColumn<int32_t>(
              "a", CREATE_SUBSTRAIT_TYPE(I32), {1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
          .addColumn<int32_t>(
              "b", CREATE_SUBSTRAIT_TYPE(I32), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
          .addColumn<int32_t>("c",
                              CREATE_SUBSTRAIT_TYPE(I32),
                              {100, 111, 222, 333, 444, 555, 666, 777, 888, 999})
          .build();
  StdMapDuplicateKeyWrapper<int, std::pair<Batch*, int>> dup_map;

  Batch build_batch(*schema, *array);
  cider::exec::processor::JoinHashTable hm;
  auto batch = new Batch(build_batch);
  for (int i = 0; i < 10; i++) {
    int key = *(
        (reinterpret_cast<int*>(const_cast<void*>(array->children[1]->buffers[1]))) + i);
    hm.emplace(key, {batch, i});
    dup_map.insert(std::move(key), std::make_pair(batch, i));
  }

  codegen_ctx.setHashTable(std::make_shared<cider::exec::processor::JoinHashTable>(hm));
  auto runtime_ctx = codegen_ctx.generateRuntimeCTX(allocator);

  query_func((int8_t*)runtime_ctx.get());

  EXPECT_EQ(runtime_ctx->getContextItemNum(), 1);
  EXPECT_NE(runtime_ctx->getContextItem(0), nullptr);

  auto hash_table = reinterpret_cast<cider::exec::processor::JoinHashTable*>(
      runtime_ctx->getContextItem(0));

  auto check_array = [](ArrowArray* array, size_t expect_len) {
    EXPECT_EQ(array->length, expect_len);
    int32_t* data_buffer = (int32_t*)array->buffers[1];
    for (size_t i = 0; i < expect_len; ++i) {
      std::cout << data_buffer[i] << " ";
    }
    std::cout << std::endl;
  };

  for (auto key_iter : dup_map.getMap()) {
    auto dup_res_vec = dup_map.findAll(key_iter.first);
    auto hm_res_vec = hash_table->findAll(key_iter.first);

    for (int i = 0; i < hm_res_vec.size(); ++i) {
      auto pair = hm_res_vec.at(i);
      auto batch = pair.batch_ptr;
      check_array(batch->getArray()->children[0], 10);
      check_array(batch->getArray()->children[1], 10);
      check_array(batch->getArray()->children[2], 10);
    }
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
