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
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/plan/parser/TypeUtils.h"
#include "exec/template/CodegenColValues.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "tests/TestHelpers.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "tests/utils/CiderTestBase.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "tests/utils/Utils.h"

using namespace cider::exec::nextgen;

// static const std::shared_ptr<CiderAllocator> allocator =
//     std::make_shared<CiderDefaultAllocator>();

class NextgenExprCompilerTest : public ::testing::Test {
 public:
  std::unique_ptr<context::CodegenContext> compile(const std::string& create_ddl,
                                                   const std::string& sql) {
    // SQL Parsing
    auto json = RunIsthmus::processSql(sql, create_ddl);
    ::substrait::Plan plan;
    google::protobuf::util::JsonStringToMessage(json, &plan);

    generator::SubstraitToRelAlgExecutionUnit substrait2eu(plan);
    auto eu = substrait2eu.createRelAlgExecutionUnit();

    // Pipeline Building
    std::vector<InputColDescriptor> input_descs;
    for (auto& input_desc : eu.input_col_descs) {
      input_descs.emplace_back(*input_desc);
    }
    cider::jitlib::CompilationOptions co;
    // co.dump_ir = true;
    return cider::exec::nextgen::compile(eu.shared_target_exprs, input_descs, co);
  }

  template <typename R, typename... Args>
  auto getFunction(context::CodegenContext* codegen_ctx) {
    auto func = codegen_ctx->getJITFunction();
    return func->getFunctionPointer<R, Args...>();
  }
};

TEST_F(NextgenExprCompilerTest, NotNullExpr) {
  std::string create_ddl = "CREATE TABLE test(a BIGINT NOT NULL, b BIGINT NOT NULL);";
  std::string sql = "select a + b, a - b from test";

  auto ctx = compile(create_ddl, sql);
  auto func = getFunction<void, int8_t*, int64_t, int64_t, int64_t*, int64_t*>(ctx.get());

  std::vector<std::array<int64_t, 4>> arr = {
      {1, -1, 0, 2}, {2, 3, 5, -1}, {3, 0, 3, 3}, {4, 5, 9, -1}, {5, 1, 6, 4}};
  for (auto& [a, b, expected_res1, expected_res2] : arr) {
    int64_t res1 = 0, res2 = 0;
    // we don't use context by now
    func(nullptr, a, b, &res1, &res2);
    EXPECT_EQ(res1, expected_res1);
    EXPECT_EQ(res2, expected_res2);
  }
}

TEST_F(NextgenExprCompilerTest, NullableExpr) {
  std::string create_ddl = "CREATE TABLE test(a BIGINT, b BIGINT NOT NULL);";
  std::string sql = "select a + b, a - b from test";

  auto ctx = compile(create_ddl, sql);
  auto func = getFunction<void,
                          int8_t*,
                          bool,
                          int64_t,
                          int64_t,
                          bool*,
                          int64_t*,
                          bool*,
                          int64_t*>(ctx.get());

  std::vector<std::array<int64_t, 7>> arr = {{true, 1, -1, true, 0, true, 2},
                                             {false, 2, 3, false, 5, false, -1},
                                             {false, 3, 0, false, 3, false, 3},
                                             {false, 4, 5, false, 9, false, -1},
                                             {false, 5, 1, false, 6, false, 4}};
  for (auto& [a_null,
              a,
              b,
              expected_res1_null,
              expected_res1,
              expected_res2_null,
              expected_res2] : arr) {
    int64_t res1 = 0, res2 = 0;
    bool res1_null, res2_null = false;
    // we don't use context by now
    func(nullptr, a_null, a, b, &res1_null, &res1, &res2_null, &res2);
    EXPECT_EQ(res1_null, expected_res1_null);
    EXPECT_EQ(res1, expected_res1);
    EXPECT_EQ(res2_null, expected_res2_null);
    EXPECT_EQ(res2, expected_res2);
  }
}

int main(int argc, char** argv) {
  TestHelpers::init_logger_stderr_only(argc, argv);

  testing::InitGoogleTest(&argc, argv);

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
