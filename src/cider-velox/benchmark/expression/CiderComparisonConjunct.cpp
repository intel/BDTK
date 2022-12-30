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

#define SHARED_LOGGER_H  // ignore util/Logger.h
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "Allocator.h"
#include "ExprEvalUtils.h"
#include "VeloxToCiderExpr.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// This file refers velox/velox/benchmarks/basic/ComparisonConjunct.cpp.
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(batch_size, 1000, "batch size for one loop");
DEFINE_int64(loop_count, 5000, "loop count for benchmark");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::functions;
using namespace facebook::velox::plugin;

namespace {
static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

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

  // Function runVelox records Velox execution time.
  size_t runVelox(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

  // Function runCiderCompile records Cider compile time.
  size_t runCiderCompile(const std::string& expression,
                         bool use_nextgen_compiler = false) {
    folly::BenchmarkSuspender suspender;

    // Get EU
    RelAlgExecutionUnit relAlgEU =
        ExprEvalUtils::getEU(expression,
                             std::dynamic_pointer_cast<const RowType>(inputType_),
                             "comparison",
                             execCtx_.pool());

    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    // get output column types
    auto schema = ExprEvalUtils::getOutputTableSchema(relAlgEU);

    // Prepare runtime module
    FLAGS_use_cider_data_format = true;
    FLAGS_use_nextgen_compiler = use_nextgen_compiler;
    auto ciderCompileModule = CiderCompileModule::Make(allocator);

    suspender.dismiss();

    auto result =
        ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos, schema);
    return 1;
  }

  // Function runCiderCompute records Cider execution time.
  size_t runCiderCompute(const std::string& expression,
                         bool use_nextgen_compiler = false) {
    folly::BenchmarkSuspender suspender;

    // Get EU
    RelAlgExecutionUnit relAlgEU =
        ExprEvalUtils::getEU(expression,
                             std::dynamic_pointer_cast<const RowType>(inputType_),
                             "comparison",
                             execCtx_.pool());

    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    // get output column types
    auto schema = ExprEvalUtils::getOutputTableSchema(relAlgEU);

    // Prepare runtime module
    FLAGS_use_cider_data_format = true;
    FLAGS_use_nextgen_compiler = use_nextgen_compiler;
    auto ciderCompileModule = CiderCompileModule::Make(allocator);
    auto result =
        ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos, schema);
    std::shared_ptr<CiderRuntimeModule> ciderRuntimeModule =
        std::make_shared<CiderRuntimeModule>(result);

    // Convert data to CiderBatch
    auto inBatch = veloxVectorToCiderBatch(rowVector_, execCtx_.pool());

    suspender.dismiss();

    size_t count = 0;
    for (int i = 0; i < FLAGS_loop_count; i++) {
      ciderRuntimeModule->processNextBatch(*inBatch);
      auto [_, outBatch] = ciderRuntimeModule->fetchResults();
      count += outBatch->getLength();
    }
    return count;
  }

 private:
  size_t vectorSize_;
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<ComparisonBenchmark> benchmark;

#define BENCHMARK_GROUP(name, expr)                                                    \
  BENCHMARK(name##Velox) { benchmark->runVelox(expr); }                                \
  BENCHMARK_RELATIVE(name##Cider) { benchmark->runCiderCompute(expr); }                \
  BENCHMARK_RELATIVE(name##Nextgen) { benchmark->runCiderCompute(expr, true); }        \
  BENCHMARK(name##CiderCompile) { benchmark->runCiderCompile(expr); }                  \
  BENCHMARK_RELATIVE(name##NextgenCompile) { benchmark->runCiderCompile(expr, true); } \
  BENCHMARK_DRAW_LINE()

// Use Velox execution as a baseline.
BENCHMARK_GROUP(eq, "eq(a, b)");
BENCHMARK_GROUP(neq, "neq(a, b)");
BENCHMARK_GROUP(gt, "gt(a, b)");
BENCHMARK_GROUP(lt, "lt(a, b)");
BENCHMARK_GROUP(eqHalfNull, "eq(a, half_null)");
BENCHMARK_GROUP(eqBool, "eq(d, e)");
BENCHMARK_GROUP(and, "d AND e");
BENCHMARK_GROUP(or, "d or e");
BENCHMARK_GROUP(andHalfNull, "d AND bool_half_null");
BENCHMARK_GROUP(conjunctsNested,
                "(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))");
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<ComparisonBenchmark>(FLAGS_batch_size);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
