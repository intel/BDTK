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

#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "DataConvertor.h"
#include "ExprEvalUtils.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"
#include "exec/template/InputMetadata.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// #include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This file refers velox/velox/benchmarks/basic/SimpleArithmetic.cpp
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::plugin;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();
namespace {

// simple multiply function.
template <typename T>
struct MultiplyVoidOutputFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::multiply(a, b);
  }
};

// Checked Arithmetic.
template <typename T>
struct PlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::plus(a, b);
  }
};

class CiderSimpleArithmeticBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit CiderSimpleArithmeticBenchmark(size_t vectorSize) : FunctionBenchmarkBase() {
    registerFunction<MultiplyVoidOutputFunction, double, double, double>({"multiply"});
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    // behavior of plus function is different when registered using above method
    // and registerAllScalarFunctions
    // functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    // Set input schema.
    inputType_ = ROW({
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"c", BIGINT()},
        {"d", BIGINT()},
        {"constant", DOUBLE()},
        {"half_null", DOUBLE()},
    });
    vectorSize_ = vectorSize;
    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // B
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));  // C
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));  // D
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // fake constant

    opts.nullRatio = 0.5;  // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // HalfNull

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));
  }

  // benchmark for IR generation
  size_t runCiderCompile(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(expression, options);
    auto typedExpr = core::Expressions::inferTypes(untyped, inputType_, execCtx_.pool());
    auto rowType = std::dynamic_pointer_cast<const RowType>(inputType_);
    auto relAlgEU = ExprEvalUtils::getMockedRelAlgEU(typedExpr, rowType, "arithmetic");
    auto ciderCompileModule = CiderCompileModule::Make();
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    suspender.dismiss();
    auto result = ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos);
    return 1;
  }

  std::shared_ptr<CiderRuntimeModule> getCiderRuntimeModule(
      const std::string& text,
      const std::shared_ptr<const Type> type,
      memory::MemoryPool* pool,
      const std::string euType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    auto typedExpr = core::Expressions::inferTypes(untyped, type, pool);
    auto rowType = std::dynamic_pointer_cast<const RowType>(type);
    auto relAlgEU = ExprEvalUtils::getMockedRelAlgEU(typedExpr, rowType, euType);
    auto ciderCompileModule = CiderCompileModule::Make();
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    auto result = ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos);
    auto ciderRuntimeModule = std::make_shared<CiderRuntimeModule>(result);
    return ciderRuntimeModule;
  }

  // Runs `expression` `times` times in Cider
  size_t runCiderCompute(const std::string& expression, int times) {
    folly::BenchmarkSuspender suspender;
    // prepare runtime module
    auto ciderRuntimeModule =
        getCiderRuntimeModule(expression, inputType_, execCtx_.pool(), "arithmetic");
    // convert data to CiderBatch
    auto batchConvertor = DataConvertor::create(CONVERT_TYPE::DIRECT, allocator);
    std::chrono::microseconds convertorInternalCounter;
    auto inBatch = batchConvertor->convertToCider(
        rowVector_, vectorSize_, &convertorInternalCounter);
    // TODO :process the batch and return result
    size_t max_out = inBatch.row_num();
    std::vector<const int8_t*> col_buffer;
    double* col_0 =
        reinterpret_cast<double*>(allocator->allocate(sizeof(double) * max_out * 2));
    int num_rows = max_out;
    col_buffer.push_back(reinterpret_cast<const int8_t*>(col_0));
    CiderBatch outBatch(num_rows, col_buffer);
    suspender.dismiss();
    size_t rows_size = 0;
    for (auto i = 0; i < times; i++) {
      ciderRuntimeModule->processNextBatch(inBatch);
      auto [_, outBatch] = ciderRuntimeModule->fetchResults();
      rows_size += vectorSize_;
    }
    return rows_size;
  }

  // Runs `expression` `times` times in Velox
  size_t runVelox(const std::string& expression, int times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();
    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
  size_t vectorSize_;
};

std::unique_ptr<CiderSimpleArithmeticBenchmark> benchmark;

BENCHMARK(multiply) {
  benchmark->runVelox("multiply(a, b)", 100);
}

BENCHMARK_RELATIVE(multiplyCiderCompute) {
  benchmark->runCiderCompute("multiply(a, b)", 100);
}

BENCHMARK(multiplyCiderCompile) {
  benchmark->runCiderCompile("multiply(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplySameColumn) {
  benchmark->runVelox("multiply(a, a)", 100);
}

BENCHMARK_RELATIVE(multiplySameColumnCiderCompute) {
  benchmark->runCiderCompute("multiply(a, a)", 100);
}

BENCHMARK(multiplySameColumnCiderCompile) {
  benchmark->runCiderCompile("multiply(a, a)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyHalfNull) {
  benchmark->runVelox("multiply(a, half_null)", 100);
}

BENCHMARK_RELATIVE(multiplyHalfNullCiderCompute) {
  benchmark->runCiderCompute("multiply(a, half_null)", 100);
}

BENCHMARK(multiplyHalfNullCiderCompile) {
  benchmark->runCiderCompile("multiply(a, half_null)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyNested) {
  benchmark->runVelox("multiply(multiply(a, b), b)", 100);
}

BENCHMARK_RELATIVE(multiplyNestedCiderCompute) {
  benchmark->runCiderCompute("multiply(multiply(a, b), b)", 100);
}

BENCHMARK(multiplyNestedCiderCompile) {
  benchmark->runCiderCompile("multiply(multiply(a, b), b)");
}

// BENCHMARK_DRAW_LINE();

// BENCHMARK(multiplyAndPlusArithmetic) {
//   benchmark->runVelox(
//       "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0",
//       100);
// }

// BENCHMARK_RELATIVE(multiplyAndPlusArithmeticCiderCompute) {
//   benchmark->runCiderCompute(
//       "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0",
//       100);
// }

// BENCHMARK(multiplyAndPlusArithmeticCiderCompile) {
//   benchmark->runCiderCompile(
//       "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0");
// }

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyNestedDeep) {
  benchmark->runVelox(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      100);
}

BENCHMARK_RELATIVE(multiplyNestedDeepCiderCompute) {
  benchmark->runCiderCompute(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      100);
}

BENCHMARK(multiplyNestedDeepCiderCompile) {
  benchmark->runCiderCompile(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(plusUnchecked) {
  benchmark->runVelox("plus(c, d)", 100);
}

BENCHMARK_RELATIVE(plusUncheckedCiderCompute) {
  benchmark->runCiderCompute("plus(c, d)", 100);
}

BENCHMARK(plusUncheckedCiderCompile) {
  benchmark->runCiderCompile("plus(c, d)");
}
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<CiderSimpleArithmeticBenchmark>(5'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
