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

#include "benchmark/benchmark.h"

#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "tests/nextgen/operator/MockSink.h"
#include "type/plan/Analyzer.h"
#include "util/sqldefs.h"

using namespace cider::jitlib;
using namespace cider::exec::nextgen;
using namespace cider::exec::nextgen::operators;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;

ExprPtr makeConstant(int32_t val) {
  Datum d;
  d.intval = val;
  return std::make_shared<Analyzer::Constant>(kINT, false, d);
}

void base_func(int32_t input, int32_t* output) {
  if (input >= 100 && input <= 500 && input != 256) {
    *output = input + 9999;
  }
}

void bench_filter_base(benchmark::State& state) {
  int32_t output = 0;
  for (auto _ : state) {
    for (int32_t input = 0; input < state.range(0); ++input) {
      base_func(input, &output);
    }
  }
}

template <llvm::CodeGenOpt::Level level>
void bench_filter(benchmark::State& state) {
  auto builder = [=](JITFunction* function) {
    auto input = function->getArgument(0);

    // var
    auto col_var =
        std::make_shared<Analyzer::ColumnVar>(SQLTypeInfo(SQLTypes::kINT), 100, 1, 0);
    col_var->set_expr_value({std::move(input)});

    // (var >= 100) and (var <= 500) and (var != 256)
    // var >= 100
    auto cmp_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kBOOLEAN, SQLOps::kGE, SQLQualifier::kONE, col_var, makeConstant(100));
    // var <= 500
    auto cmp_expr1 = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kBOOLEAN, SQLOps::kLE, SQLQualifier::kONE, col_var, makeConstant(500));
    // var != 200
    auto cmp_expr2 = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kBOOLEAN, SQLOps::kNE, SQLQualifier::kONE, col_var, makeConstant(500));

    // var + 9999
    auto add_expr = std::make_shared<Analyzer::BinOper>(
        SQLTypes::kINT, SQLOps::kPLUS, SQLQualifier::kONE, col_var, makeConstant(9999));

    // filter -> project -> sink
    auto trans = FilterTranslator({cmp_expr, cmp_expr1, cmp_expr2},
                                  std::make_unique<ProjectTranslator>(
                                      add_expr, std::make_unique<MockSinkTranslator>(1)));

    Context context(function);
    trans.consume(context);

    function->createReturn();
  };

  LLVMJITModule module("Test",
                       CompilationOptions{.optimize_level = OptimizeLevel::RELEASE,
                                          .codegen_level = level,
                                          .dump_ir = false});
  JITFunctionPointer func =
      JITFunctionBuilder()
          .setFuncName("test_filter_op")
          .registerModule(module)
          .addReturn(JITTypeTag::VOID)
          .addParameter(JITTypeTag::INT32, "in")
          .addParameter(JITTypeTag::POINTER, "out", JITTypeTag::INT32)
          .addProcedureBuilder(builder)
          .build();
  module.finish();

  auto func_ptr = func->getFunctionPointer<void, int32_t, int32_t*>();

  int32_t output = 0;
  for (auto _ : state) {
    for (int32_t input = 0; input < state.range(0); ++input) {
      func_ptr(input, &output);
    }
  }
}

BENCHMARK(bench_filter_base)->RangeMultiplier(2)->Range(8, 1024);
BENCHMARK_TEMPLATE(bench_filter, llvm::CodeGenOpt::None)
    ->RangeMultiplier(2)
    ->Range(8, 1024);
BENCHMARK_TEMPLATE(bench_filter, llvm::CodeGenOpt::Less)
    ->RangeMultiplier(2)
    ->Range(8, 1024);
BENCHMARK_TEMPLATE(bench_filter, llvm::CodeGenOpt::Default)
    ->RangeMultiplier(2)
    ->Range(8, 1024);
BENCHMARK_TEMPLATE(bench_filter, llvm::CodeGenOpt::Aggressive)
    ->RangeMultiplier(2)
    ->Range(8, 1024);

// Run the benchmark
BENCHMARK_MAIN();
