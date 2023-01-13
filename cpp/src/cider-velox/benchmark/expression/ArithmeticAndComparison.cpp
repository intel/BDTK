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
#include "cider/CiderOptions.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// #include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This file refers velox/velox/benchmarks/basic/SimpleArithmetic.cpp
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(batch_size, 1'000, "batch size for one loop");
DEFINE_int64(loop_count, 2'000'000, "loop count for benchmark");

using namespace cider::exec::processor;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::functions;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

namespace {

std::pair<ArrowArray*, ArrowSchema*> veloxVectorToArrow(RowVectorPtr vec,
                                                        MemoryPool* pool) {
  for (size_t i = 0; i < vec->childrenSize(); i++) {
    vec->childAt(i)->mutableRawNulls();
  }
  ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
  exportToArrow(vec, *inputArrowArray, pool);
  ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
  exportToArrow(vec, *inputArrowSchema);

  return {inputArrowArray, inputArrowSchema};
}

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

    // for comparision
    registerBinaryScalar<EqFunction, bool>({"eq"});
    registerBinaryScalar<NeqFunction, bool>({"neq"});
    registerBinaryScalar<LtFunction, bool>({"lt"});
    registerBinaryScalar<GtFunction, bool>({"gt"});
    registerBinaryScalar<LteFunction, bool>({"lte"});
    registerBinaryScalar<GteFunction, bool>({"gte"});
    registerFunction<BetweenFunction, bool, double, double, double>({"btw"});

    // Set input schema.
    inputType_ = ROW({
        {"i8", TINYINT()},
        {"i16", SMALLINT()},
        {"i32", INTEGER()},
        {"i64", BIGINT()},
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"constant", DOUBLE()},
        {"d", BOOLEAN()},
        {"e", BOOLEAN()},
    });
    vectorSize_ = vectorSize;
    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.nullRatio = 0.5;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(TINYINT()));   // i8
    children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));  // i16
    children.emplace_back(fuzzer.fuzzFlat(INTEGER()));   // i32
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));    // i64
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // B
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));    // fake constant
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));   // D
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));   // E

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));
  }

  size_t nextgenCompile(const std::string& expression) {
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

  size_t nextgenCompute(const std::string& expression,
                        bool check_bit_vector_clear_opt = false,
                        bool set_null_bit_vector_opt = false,
                        bool branchless_logic = false,
                        bool null_separate = false) {
    folly::BenchmarkSuspender suspender;
    google::protobuf::Arena arena;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);

    FLAGS_check_bit_vector_clear_opt = check_bit_vector_clear_opt;
    FLAGS_set_null_bit_vector_opt = set_null_bit_vector_opt;
    FLAGS_branchless_logic = branchless_logic;
    FLAGS_null_separate = null_separate;
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

    // reset options
    FLAGS_check_bit_vector_clear_opt = false;
    FLAGS_set_null_bit_vector_opt = false;
    FLAGS_branchless_logic = false;
    FLAGS_null_separate = false;

    return rows_size;
  }

  size_t veloxCompute(const std::string& expression) {
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
auto profile_expr = "i8 * i8";
// auto profile_expr = "d AND e";
BENCHMARK(velox) {
  benchmark->veloxCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen) {
  benchmark->nextgenCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen_opt) {
  // benchmark->nextgenCompute(profile_expr, true);
  benchmark->nextgenCompute(profile_expr, false, false, false, true);
}

#define BENCHMARK_GROUP(name, expr)                                                    \
  BENCHMARK(name##Velox_________Base) { benchmark->veloxCompute(expr); }               \
  BENCHMARK_RELATIVE(name##NextGen) { benchmark->nextgenCompute(expr); }               \
  BENCHMARK(name##NextGen_______Base) { benchmark->nextgenCompute(expr); }             \
  BENCHMARK_RELATIVE(name##NextgenBitClear) { benchmark->nextgenCompute(expr, true); } \
  BENCHMARK_RELATIVE(name##NextgenSetNull) {                                           \
    benchmark->nextgenCompute(expr, false, true);                                      \
  }                                                                                    \
  BENCHMARK_RELATIVE(name##NextgenBranchlessLogic) {                                   \
    benchmark->nextgenCompute(expr, false, false, true);                               \
  }                                                                                    \
  BENCHMARK_RELATIVE(name##NextgenNullSeparate) {                                      \
    benchmark->nextgenCompute(expr, false, false, true);                               \
  }                                                                                    \
  BENCHMARK_RELATIVE(name##NextgenAllOpt) {                                            \
    benchmark->nextgenCompute(expr, true, true, true, true);                           \
  }                                                                                    \
  BENCHMARK(name##Compile) { benchmark->nextgenCompile(expr); }                        \
  BENCHMARK_DRAW_LINE()

// BENCHMARK_GROUP(mulI8, "multiply(i8, i8)");
// BENCHMARK_GROUP(mulI8Nested, "i8*i8*i8");
// BENCHMARK_GROUP(mulI8NestedDeep, "i8*i8*i8*i8");
// BENCHMARK_GROUP(mulI16, "multiply(i16, i16)");
// BENCHMARK_GROUP(mulI32, "multiply(i32, i32)");
// BENCHMARK_GROUP(mulI64, "multiply(i64, i64)");

// BENCHMARK_GROUP(mulDouble, "multiply(a, b)");
// BENCHMARK_GROUP(mulDoubleSameColumn, "multiply(a, a)");
// BENCHMARK_GROUP(mulDoubleConstant, "multiply(a, constant)");
// BENCHMARK_GROUP(mulDoubleNested, "multiply(multiply(a, b), b)");
// BENCHMARK_GROUP(mulDoubleNestedDeep,
//                 "multiply(multiply(multiply(a, b), a), "
//                 "multiply(a, multiply(a, b)))");

// BENCHMARK(PlusCheckedVeloxI64) {
//   benchmark->veloxCompute("checkedPlus(i64, i64)");
// }
// BENCHMARK_GROUP(plusI64, "plus(i64, i64)");

// BENCHMARK_GROUP(multiplyAndAddArithmetic, "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0");

// // comparison
// BENCHMARK_GROUP(eq, "eq(a, b)");
// BENCHMARK_GROUP(eqConstant, "eq(a, constant)");
// BENCHMARK_GROUP(eqBool, "eq(d, e)");
// BENCHMARK_GROUP(neq, "neq(a, b)");
// BENCHMARK_GROUP(gt, "gt(a, b)");
// BENCHMARK_GROUP(lt, "lt(a, b)");
// BENCHMARK_GROUP(and, "d AND e");
// BENCHMARK_GROUP(or, "d OR e");
// BENCHMARK_GROUP(conjunctsNested,
//                 "(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))");
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<CiderSimpleArithmeticBenchmark>(FLAGS_batch_size);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
