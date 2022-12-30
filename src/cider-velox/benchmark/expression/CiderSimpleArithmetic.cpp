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
#include "cider/processor/BatchProcessorContext.h"
#include "exec/processor/StatelessProcessor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// #include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This file refers velox/velox/benchmarks/basic/SimpleArithmetic.cpp
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(batch_size, 1000, "batch size for one loop");
DEFINE_int64(loop_count, 1000000, "loop count for benchmark");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::functions;
using namespace cider::exec::processor;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

namespace {

class CiderSimpleArithmeticBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit CiderSimpleArithmeticBenchmark(size_t vectorSize) : FunctionBenchmarkBase() {
    // registerAllScalarFunctions() just register checked version for integer
    // we register uncheck version for integer and checkedPlus for compare
    registerFunction<MultiplyFunction, int8_t, int8_t, int8_t>({"multiply"});
    registerFunction<MultiplyFunction, int16_t, int16_t, int16_t>({"multiply"});
    registerFunction<MultiplyFunction, int32_t, int32_t, int32_t>({"multiply"});
    registerFunction<MultiplyFunction, int64_t, int64_t, int64_t>({"multiply"});
    registerFunction<MultiplyFunction, double, double, double>({"multiply"});
    registerFunction<PlusFunction, int8_t, int8_t, int8_t>({"plus"});
    registerFunction<PlusFunction, int16_t, int16_t, int16_t>({"plus"});
    registerFunction<PlusFunction, int32_t, int32_t, int32_t>({"plus"});
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    registerFunction<PlusFunction, double, double, double>({"plus"});

    // compare with uncheck plus
    registerFunction<CheckedPlusFunction, int64_t, int64_t, int64_t>({"checkedPlus"});

    // Set input schema.
    inputType_ = ROW({
        {"i8", TINYINT()},
        {"i16", SMALLINT()},
        {"i32", INTEGER()},
        {"i64", BIGINT()},
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
    children.emplace_back(fuzzer.fuzzFlat(TINYINT()));   // i8
    children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));  // i16
    children.emplace_back(fuzzer.fuzzFlat(INTEGER()));   // i32
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));    // i64
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // B
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));    // C
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));    // D
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // fake constant

    opts.nullRatio = 0.5;  // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // HalfNull

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));
  }

  size_t runNextgenCompile(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    google::protobuf::Arena arena;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);
    suspender.dismiss();

    auto allocator = std::make_shared<CiderDefaultAllocator>();
    auto context = std::make_shared<BatchProcessorContext>(allocator);
    auto processor = makeBatchProcessor(plan, context);
    return 1;
  }

  size_t runNextgenCompute(const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    google::protobuf::Arena arena;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);

    auto allocator = std::make_shared<CiderDefaultAllocator>();
    auto context = std::make_shared<BatchProcessorContext>(allocator);
    auto processor = makeBatchProcessor(plan, context);

    auto [input_array, _] = veloxVectorToArrow(rowVector_, execCtx_.pool());
    // hack: make processor don't release input_array, otherwise we can't use input_array
    // multi times.
    input_array->release = nullptr;

    suspender.dismiss();

    size_t rows_size = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      processor->processNextBatch(input_array);

      struct ArrowArray output_array;
      struct ArrowSchema output_schema;
      processor->getResult(output_array, output_schema);
      rows_size += output_array.length;

      output_array.release(&output_array);
      output_schema.release(&output_schema);
    }
    return rows_size;
  }

  // benchmark for IR generation
  size_t runCiderCompile(const std::string& expression,
                         bool use_nextgen_compiler = false) {
    folly::BenchmarkSuspender suspender;

    RelAlgExecutionUnit relAlgEU =
        ExprEvalUtils::getEU(expression,
                             std::dynamic_pointer_cast<const RowType>(inputType_),
                             "arithmetic",
                             execCtx_.pool());
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    auto schema = ExprEvalUtils::getOutputTableSchema(relAlgEU);
    suspender.dismiss();

    FLAGS_use_cider_data_format = true;
    FLAGS_use_nextgen_compiler = use_nextgen_compiler;
    auto ciderCompileModule = CiderCompileModule::Make(allocator);

    auto result =
        ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos, schema);
    return 1;
  }

  std::shared_ptr<CiderRuntimeModule> getCiderRuntimeModule(
      const std::string& text,
      const std::shared_ptr<const Type> type,
      memory::MemoryPool* pool,
      const std::string eu_group,
      bool use_nextgen_compiler) {
    RelAlgExecutionUnit relAlgEU =
        ExprEvalUtils::getEU(text,
                             std::dynamic_pointer_cast<const RowType>(inputType_),
                             eu_group,
                             execCtx_.pool());
    auto schema = ExprEvalUtils::getOutputTableSchema(relAlgEU);

    FLAGS_use_cider_data_format = true;
    FLAGS_use_nextgen_compiler = use_nextgen_compiler;
    auto ciderCompileModule = CiderCompileModule::Make(allocator);
    std::vector<InputTableInfo> queryInfos = ExprEvalUtils::buildInputTableInfo();
    auto result =
        ciderCompileModule->compile((void*)&relAlgEU, (void*)&queryInfos, schema);
    return std::make_shared<CiderRuntimeModule>(result);
  }

  size_t runCiderCompute(const std::string& expression,
                         bool use_nextgen_compiler = false) {
    folly::BenchmarkSuspender suspender;
    // prepare runtime module
    auto ciderRuntimeModule = getCiderRuntimeModule(
        expression, inputType_, execCtx_.pool(), "arithmetic", use_nextgen_compiler);
    // Convert data to CiderBatch
    auto inBatch = veloxVectorToCiderBatch(rowVector_, execCtx_.pool());
    suspender.dismiss();

    size_t rows_size = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      ciderRuntimeModule->processNextBatch(*inBatch);
      auto [_, outBatch] = ciderRuntimeModule->fetchResults();
      rows_size += outBatch->getLength();
    }
    return rows_size;
  }

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

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
  size_t vectorSize_;
};

std::unique_ptr<CiderSimpleArithmeticBenchmark> benchmark;

// for profile
// BENCHMARK(single) {
//   benchmark->runVelox("multiply(i8, i8)");
// }
// BENCHMARK_RELATIVE(singleCider) {
//   benchmark->runCiderCompute("multiply(i8, i8)", true);
// }
// BENCHMARK_RELATIVE(singleNextgen) {
//   benchmark->runNextgenCompute("multiply(i8, i8)");
// }

#define BENCHMARK_GROUP(name, expr)                                                     \
  BENCHMARK(name##Velox) { benchmark->runVelox(expr); }                                 \
  BENCHMARK_RELATIVE(name##Cider) { benchmark->runCiderCompute(expr); }                 \
  BENCHMARK_RELATIVE(name##Nextgen) { benchmark->runCiderCompute(expr, true); }         \
  BENCHMARK_RELATIVE(name##NextgenBatch) { benchmark->runNextgenCompute(expr); }        \
  BENCHMARK(name##CiderCompile) { benchmark->runCiderCompile(expr); }                   \
  BENCHMARK_RELATIVE(name##NextgenCompile) { benchmark->runCiderCompile(expr, true); }  \
  BENCHMARK_RELATIVE(name##NextgenBatchCompile) { benchmark->runNextgenCompile(expr); } \
  BENCHMARK_DRAW_LINE()

BENCHMARK_GROUP(i8_mul_i8, "multiply(i8, i8)");
BENCHMARK_GROUP(i16_mul_i16, "multiply(i16, i16)");
BENCHMARK_GROUP(i32_mul_i32, "multiply(i32, i32)");
BENCHMARK_GROUP(i64_mul_i64, "multiply(i64, i64)");

BENCHMARK_GROUP(double_mul_double, "multiply(a, a)");
BENCHMARK_GROUP(multiplyDouble, "multiply(a, b)");
BENCHMARK_GROUP(multiplyHalfNull, "multiply(a, half_null)");
BENCHMARK_GROUP(multiplyNested, "multiply(multiply(a, b), b)");
BENCHMARK_GROUP(multiplyNestedDeep,
                "multiply(multiply(multiply(a, b), a), "
                "multiply(a, multiply(a, b)))");
BENCHMARK(plusCheckedVelox) {
  benchmark->runVelox("checkedPlus(c, d)");
}
BENCHMARK_GROUP(plusUnchecked, "plus(c, d)");
BENCHMARK_GROUP(multiplyAndAddArithmetic, "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0");
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<CiderSimpleArithmeticBenchmark>(FLAGS_batch_size);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
