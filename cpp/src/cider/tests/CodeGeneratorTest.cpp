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

#include <gtest/gtest.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "TestHelpers.h"
#include "exec/template/CodeGenerator.h"
#include "exec/template/Execute.h"
#include "exec/template/IRCodegenUtils.h"
#include "type/plan/Analyzer.h"

TEST(CodeGeneratorTest, IntegerConstant) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID).get();
  auto llvm_module = llvm::CloneModule(*executor->get_rt_module());
  ScalarCodeGenerator code_generator(std::move(llvm_module));
  CompilationOptions co = CompilationOptions::defaults();
  co.hoist_literals = false;

  Datum d;
  d.intval = 42;
  auto constant = makeExpr<Analyzer::Constant>(kINT, false, d);
  const auto compiled_expr = code_generator.compile(constant.get(), true, co);
  verify_function_ir(compiled_expr.func);
  ASSERT_TRUE(compiled_expr.inputs.empty());

  using FuncPtr = int (*)(int*);
  auto func_ptr = reinterpret_cast<FuncPtr>(
      code_generator.generateNativeCode(compiled_expr, co).front());
  CHECK(func_ptr);
  int out;
  int err = func_ptr(&out);
  ASSERT_EQ(err, 0);
  ASSERT_EQ(out, d.intval);
}

TEST(CodeGeneratorTest, IntegerAdd) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID).get();
  auto llvm_module = llvm::CloneModule(*executor->get_rt_module());
  ScalarCodeGenerator code_generator(std::move(llvm_module));
  CompilationOptions co = CompilationOptions::defaults();
  co.hoist_literals = false;

  Datum d;
  d.intval = 42;
  auto lhs = makeExpr<Analyzer::Constant>(kINT, false, d);
  auto rhs = makeExpr<Analyzer::Constant>(kINT, false, d);
  auto plus = makeExpr<Analyzer::BinOper>(kINT, kPLUS, kONE, lhs, rhs);
  const auto compiled_expr = code_generator.compile(plus.get(), true, co);
  verify_function_ir(compiled_expr.func);
  ASSERT_TRUE(compiled_expr.inputs.empty());

  using FuncPtr = int (*)(int*);
  auto func_ptr = reinterpret_cast<FuncPtr>(
      code_generator.generateNativeCode(compiled_expr, co).front());
  CHECK(func_ptr);
  int out;
  int err = func_ptr(&out);
  ASSERT_EQ(err, 0);
  ASSERT_EQ(out, d.intval + d.intval);
}

TEST(CodeGeneratorTest, IntegerColumn) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID).get();
  auto llvm_module = llvm::CloneModule(*executor->get_rt_module());
  ScalarCodeGenerator code_generator(std::move(llvm_module));
  CompilationOptions co = CompilationOptions::defaults();
  co.hoist_literals = false;

  SQLTypeInfo ti(kINT, false);
  int table_id = 1;
  int column_id = 5;
  int rte_idx = 0;
  auto col = makeExpr<Analyzer::ColumnVar>(ti, table_id, column_id, rte_idx);
  const auto compiled_expr = code_generator.compile(col.get(), true, co);
  verify_function_ir(compiled_expr.func);
  ASSERT_EQ(compiled_expr.inputs.size(), size_t(1));
  ASSERT_TRUE(*compiled_expr.inputs.front() == *col);

  using FuncPtr = int (*)(int*, int);
  auto func_ptr = reinterpret_cast<FuncPtr>(
      code_generator.generateNativeCode(compiled_expr, co).front());
  CHECK(func_ptr);
  int out;
  int err = func_ptr(&out, 17);
  ASSERT_EQ(err, 0);
  ASSERT_EQ(out, 17);
}

TEST(CodeGeneratorTest, IntegerExpr) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID).get();
  auto llvm_module = llvm::CloneModule(*executor->get_rt_module());
  ScalarCodeGenerator code_generator(std::move(llvm_module));
  CompilationOptions co = CompilationOptions::defaults();
  co.hoist_literals = false;

  SQLTypeInfo ti(kINT, false);
  int table_id = 1;
  int column_id = 5;
  int rte_idx = 0;
  auto lhs = makeExpr<Analyzer::ColumnVar>(ti, table_id, column_id, rte_idx);
  Datum d;
  d.intval = 42;
  auto rhs = makeExpr<Analyzer::Constant>(kINT, false, d);
  auto plus = makeExpr<Analyzer::BinOper>(kINT, kPLUS, kONE, lhs, rhs);
  const auto compiled_expr = code_generator.compile(plus.get(), true, co);
  verify_function_ir(compiled_expr.func);
  ASSERT_EQ(compiled_expr.inputs.size(), size_t(1));
  ASSERT_TRUE(*compiled_expr.inputs.front() == *lhs);

  using FuncPtr = int (*)(int*, int);
  auto func_ptr = reinterpret_cast<FuncPtr>(
      code_generator.generateNativeCode(compiled_expr, co).front());
  CHECK(func_ptr);
  int out;
  int err = func_ptr(&out, 58);
  ASSERT_EQ(err, 0);
  ASSERT_EQ(out, 100);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
