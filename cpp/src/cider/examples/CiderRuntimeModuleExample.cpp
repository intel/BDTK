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

#include <memory>
#include "cider/CiderAllocator.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"
#include "exec/plan/builder/SubstraitExprBuilder.h"
#include "exec/plan/parser/LiteralUtils.h"
#include "exec/plan/parser/TypeUtils.h"
#include "substrait/algebra.pb.h"
#include "substrait/function.pb.h"
#include "substrait/type.pb.h"
#include "tests/utils/CiderBatchBuilder.h"
#include "tests/utils/CiderBatchChecker.h"

// use std::allocator to manage memory
// you can difine your allocator to do something specifial.
class UserAllocator : public CiderAllocator {
 public:
  int8_t* allocate(size_t size) final { return allocator_.allocate(size); }
  void deallocate(int8_t* p, size_t size) final { allocator_.deallocate(p, size); }

 private:
  std::allocator<int8_t> allocator_{};
};

int main(int argc, char** argv) {
  // generate for expression "a * b + b"
  SubstraitExprBuilder builder({"a", "b"},
                               {CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false),
                                CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false)});
  ::substrait::Expression* field0 = builder.makeFieldReference(0);
  ::substrait::Expression* field1 = builder.makeFieldReference("b");
  ::substrait::Expression* multiply_expr = builder.makeScalarExpr(
      "multiply", {field0, field1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  ::substrait::Expression* add_expr = builder.makeScalarExpr(
      "add", {multiply_expr, field1}, CREATE_SUBSTRAIT_TYPE_FULL_PTR(I64, false));
  // Set up exec option and compilation option
  auto exec_option = CiderExecutionOption::defaults();
  auto compile_option = CiderCompilationOption::defaults();

  // Construct an allocator
  auto allocator = std::make_shared<UserAllocator>();

  auto ciderCompileModule = CiderCompileModule::Make(allocator);
  auto result = ciderCompileModule->compile({add_expr},
                                            *builder.getSchema(),
                                            builder.funcsInfo(),
                                            generator::ExprType::ProjectExpr,
                                            compile_option,
                                            exec_option);
  // Create runtimemodule
  auto runner = std::make_shared<CiderRuntimeModule>(
      result, compile_option, exec_option, allocator);

  // Make batch for evaluation, data should be transferred from frontend in real case
  auto input_batch = CiderBatchBuilder()
                         .setRowNum(2)
                         .addColumn<int64_t>("a", CREATE_SUBSTRAIT_TYPE(I64), {1, 4})
                         .addColumn<int64_t>("b", CREATE_SUBSTRAIT_TYPE(I64), {4, 3})
                         .build();
  runner->processNextBatch(input_batch);
  auto [_, out_batch] = runner->fetchResults();

  // Verify result
  auto expect_batch = CiderBatchBuilder()
                          .setRowNum(2)
                          .addColumn<int64_t>("0", CREATE_SUBSTRAIT_TYPE(I64), {8, 15})
                          .build();
  assert(CiderBatchChecker::checkEq(std::make_shared<CiderBatch>(std::move(*out_batch)),
                                    std::make_shared<CiderBatch>(expect_batch)));
}
