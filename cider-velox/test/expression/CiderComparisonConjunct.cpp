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
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "DataConvertor.h"
#include "ExprEvalUtils.h"
#include "cider/CiderRuntimeModule.h"
#include "exec/template/InputMetadata.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// This file refers velox/velox/benchmarks/basic/ComparisonConjunct.cpp.
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::functions;
using namespace facebook::velox::plugin;

static const std::shared_ptr<CiderAllocator> ciderAllocator =
    std::make_shared<CiderDefaultAllocator>();

namespace {

class ComparisonBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ComparisonBenchmark(size_t vectorSize) : FunctionBenchmarkBase() {
    vectorSize_ = vectorSize;
    registerBinaryScalar<EqFunction, bool>({"eq"});
    registerBinaryScalar<NeqFunction, bool>({"neq"});
    registerBinaryScalar<LtFunction, bool>({"lt"});
    registerBinaryScalar<GtFunction, bool>({"gt"});
    registerBinaryScalar<LteFunction, bool>({"lte"});
    registerBinaryScalar<GteFunction, bool>({"gte"});
    registerFunction<BetweenFunction, bool, double, double, double>({"btw"});

    // Set input schema.
    inputType_ = ROW({
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"c", DOUBLE()},
        {"d", BOOLEAN()},
        {"e", BOOLEAN()},
        {"half_null", DOUBLE()},
        {"bool_half_null", BOOLEAN()},
    });

    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // B
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // C
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));  // D
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));  // E

    opts.nullRatio = 0.5;  // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // HalfNull
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));  // BoolHalfNull

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));
  }

  RelAlgExecutionUnit getEU(const std::string& text,
                            const std::shared_ptr<const RowType>& rowType = nullptr) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    auto typedExpr = core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
    RelAlgExecutionUnit relAlgEU = ExprEvalUtils::getMockedRelAlgEU(
        typedExpr, std::dynamic_pointer_cast<const RowType>(inputType_), "comparison");

    return relAlgEU;
  }

  // Run `expression` `times` times.
  // Function runVelox records Velox execution time.
  size_t runVelox(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

  // Function runCiderCompile records Cider compile time.
  size_t runCiderCompile(const std::string& expression) {
    folly::BenchmarkSuspender suspender;

    // Get EU
    RelAlgExecutionUnit relAlgEU =
        getEU(expression, std::dynamic_pointer_cast<const RowType>(inputType_));

    // Prepare runtime module
    auto ciderCompileModule = CiderCompileModule::Make();
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();

    suspender.dismiss();
    auto result = ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos);
    return 1;
  }

  // Function runCiderCompute records Cider execution time.
  size_t runCiderCompute(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;

    // Get EU
    RelAlgExecutionUnit relAlgEU =
        getEU(expression, std::dynamic_pointer_cast<const RowType>(inputType_));

    // Prepare runtime module
    auto ciderCompileModule = CiderCompileModule::Make();
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    auto result = ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos);
    std::shared_ptr<CiderRuntimeModule> ciderRuntimeModule =
        std::make_shared<CiderRuntimeModule>(result);

    // Convert data to CiderBatch
    auto batchConvertor = DataConvertor::create(CONVERT_TYPE::DIRECT, ciderAllocator);
    std::chrono::microseconds convertorInternalCounter;
    auto inBatch = batchConvertor->convertToCider(
        rowVector_, vectorSize_, &convertorInternalCounter);

    suspender.dismiss();

    size_t count = 0;
    for (int i = 0; i < times; i++) {
      ciderRuntimeModule->processNextBatch(inBatch);
      auto [_, outBatch] = ciderRuntimeModule->fetchResults();
      // TODO: the result total_matched is not right now
      count += vectorSize_;
    }
    return count;
  }

 private:
  size_t vectorSize_;
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<ComparisonBenchmark> benchmark;

// Use Velox execution as a baseline.
BENCHMARK(eqVelox) {
  benchmark->runVelox("eq(a, b)", 100);
}

BENCHMARK_RELATIVE(eqCiderCompute) {
  benchmark->runCiderCompute("eq(a, b)", 100);
}

BENCHMARK(eqCiderCompile) {
  benchmark->runCiderCompile("eq(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(neqVelox) {
  benchmark->runVelox("neq(a, b)", 100);
}

BENCHMARK_RELATIVE(neqCiderCompute) {
  benchmark->runCiderCompute("neq(a, b)", 100);
}

BENCHMARK(neqCiderCompile) {
  benchmark->runCiderCompile("neq(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(gtVelox) {
  benchmark->runVelox("gt(a, b)", 100);
}

BENCHMARK_RELATIVE(gtCiderCompute) {
  benchmark->runCiderCompute("gt(a, b)", 100);
}

BENCHMARK(gtCiderCompile) {
  benchmark->runCiderCompile("gt(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(ltVelox) {
  benchmark->runVelox("lt(a, b)", 100);
}

BENCHMARK_RELATIVE(ltCiderCompute) {
  benchmark->runCiderCompute("lt(a, b)", 100);
}

BENCHMARK(ltCiderCompile) {
  benchmark->runCiderCompile("lt(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(eqHalfNullVelox) {
  benchmark->runVelox("eq(a, half_null)", 100);
}

BENCHMARK_RELATIVE(eqHalfNullCiderCompute) {
  benchmark->runCiderCompute("eq(a, half_null)", 100);
}

BENCHMARK(eqHalfNullCiderCompile) {
  benchmark->runCiderCompile("eq(a, half_null)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(eqBoolsVelox) {
  benchmark->runVelox("eq(d, e)", 100);
}

BENCHMARK_RELATIVE(eqBoolsCiderCompute) {
  benchmark->runCiderCompute("eq(d, e)", 100);
}

BENCHMARK(eqBoolCiderCompiles) {
  benchmark->runCiderCompile("eq(d, e)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(andConjunctVelox) {
  benchmark->runVelox("d AND e", 100);
}

BENCHMARK_RELATIVE(andConjunctCiderCompute) {
  benchmark->runCiderCompute("d AND e", 100);
}

BENCHMARK(andConjunctCiderCompile) {
  benchmark->runCiderCompile("d AND e");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(orConjunctVelox) {
  benchmark->runVelox("d OR e", 100);
}

BENCHMARK_RELATIVE(orConjunctCiderCompute) {
  benchmark->runCiderCompute("d OR e", 100);
}

BENCHMARK(orConjunctCiderCompile) {
  benchmark->runCiderCompile("d OR e");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(andHalfNullVelox) {
  benchmark->runVelox("d AND bool_half_null", 100);
}

BENCHMARK_RELATIVE(andHalfNullCiderCompute) {
  benchmark->runCiderCompute("d AND bool_half_null", 100);
}

BENCHMARK(andHalfNullCiderCompile) {
  benchmark->runCiderCompile("d AND bool_half_null");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(conjunctsNestedVelox) {
  benchmark->runVelox("(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))", 100);
}

BENCHMARK_RELATIVE(conjunctsNestedCiderCompute) {
  benchmark->runCiderCompute("(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))",
                             100);
}

BENCHMARK(conjunctsNestedCiderCompile) {
  benchmark->runCiderCompile("(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))");
}

}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<ComparisonBenchmark>(1'000'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
