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
#include "velox/core/PlanNode.h"
#include "velox/exec/FilterProject.h"
#include "velox/vector/ComplexVector.h"
#define SHARED_LOGGER_H  // ignore util/Logger.h
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "Allocator.h"
#include "cider/CiderOptions.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "util/CiderBitUtils.h"
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
DEFINE_double(ratio, 0.5, "NULL ratio in batch");
DEFINE_int64(batch_size, 1'024, "batch size for one loop");
DEFINE_int64(loop_count, 1'000'000, "loop count for benchmark");
DEFINE_bool(dump_ir, false, "dump llvm ir");

using namespace cider::exec::processor;
using namespace cider::exec::nextgen::context;
using namespace facebook::velox;
using namespace facebook::velox::memory;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::functions;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

namespace {

inline CodegenOptions getBaseOption() {
  CodegenOptions cgo;
  cgo.branchless_logic = true;
  cgo.enable_vectorize = true;
  cgo.co.enable_vectorize = true;
  cgo.co.enable_avx2 = true;
  cgo.co.enable_avx512 = false;
  return cgo;
}

class PipelineOperator : public functions::test::FunctionBenchmarkBase {
 public:
  explicit PipelineOperator() : FunctionBenchmarkBase() {
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
    rowVector_ = generateData();
  }

  RowVectorPtr generateData() {
    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.nullRatio = FLAGS_ratio;
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

    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, FLAGS_batch_size, std::move(children));
  }

  __attribute__((noinline)) size_t veloxCompute(const std::string& expression) {
    auto exprSet = compileExpression(expression, inputType_);

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

  __attribute__((noinline)) size_t nextgenCompute(const std::string& expression,
                                                  CodegenOptions cgo = CodegenOptions{}) {
    folly::BenchmarkSuspender suspender;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    cgo.co.dump_ir = FLAGS_dump_ir;
    suspender.dismiss();

    compile(veloxPlan, cgo);

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      addInput(rowVector_);
      count += getOutput()->size();
    }

    return count;
  }

  __attribute__((noinline)) size_t nextgenComputeOpt(
      const std::string& expression,
      CodegenOptions cgo = CodegenOptions{}) {
    folly::BenchmarkSuspender suspender;
    auto veloxPlan = PlanBuilder().values({rowVector_}).project({expression}).planNode();
    cgo.co.dump_ir = FLAGS_dump_ir;
    suspender.dismiss();

    compile(veloxPlan, cgo);

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      addInputOpt(rowVector_);
      count += getOutput()->size();
    }

    return count;
  }

 private:
  // mimic CiderPipelineOperator::CiderPipelineOperator(...)
  void compile(core::PlanNodePtr veloxPlan, CodegenOptions cgo) {
    std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
        std::make_shared<VeloxToSubstraitPlanConvertor>();
    google::protobuf::Arena arena;
    auto plan = v2SPlanConvertor->toSubstrait(arena, veloxPlan);

    auto allocator = std::make_shared<PoolAllocator>(pool());
    auto context = std::make_shared<BatchProcessorContext>(allocator);

    batchProcessor_ = makeBatchProcessor(plan, context, cgo);
  }

  // mimic `void CiderPipelineOperator::addInput(RowVectorPtr input)`
  void addInput(RowVectorPtr input) {
    for (size_t i = 0; i < input->childrenSize(); i++) {
      input->childAt(i)->mutableRawNulls();
    }
    // this->input_ = std::move(input);
    this->input_ = input;
    ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
    exportToArrow(input_, *inputArrowArray);
    ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
    exportToArrow(input_, *inputArrowSchema);

    batchProcessor_->processNextBatch(inputArrowArray, inputArrowSchema);
  }
  void addInputOpt(RowVectorPtr input) {
    for (size_t i = 0; i < input->childrenSize(); i++) {
      input->childAt(i)->mutableRawNulls();
    }
    // this->input_ = std::move(input);
    ArrowArray inputArrowArray;
    exportToArrow(input, inputArrowArray);
    ArrowSchema inputArrowSchema;
    exportToArrow(input, inputArrowSchema);

    batchProcessor_->processNextBatch(&inputArrowArray, &inputArrowSchema);
  }

  // mimic `void CiderPipelineOperator::addInput(RowVectorPtr input)`
  RowVectorPtr getOutput() {
    struct ArrowArray array;
    struct ArrowSchema schema;

    batchProcessor_->getResult(array, schema);
    if (array.length) {
      VectorPtr baseVec = importFromArrowAsOwner(schema, array, pool());
      return std::reinterpret_pointer_cast<RowVector>(baseVec);
    }
    return nullptr;
  }

  // mimic CiderPipelineOperator
  cider::exec::processor::BatchProcessorPtr batchProcessor_;
  RowVectorPtr input_;

  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<PipelineOperator> benchmark;

// for profile
// auto profile_expr = "d AND e";
auto profile_expr = "i32*i32*i32";
BENCHMARK(velox) {
  benchmark->veloxCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen) {
  auto cgo = getBaseOption();
  benchmark->nextgenCompute(profile_expr, cgo);
}
BENCHMARK_RELATIVE(nextgenOpt) {
  auto cgo = getBaseOption();
  benchmark->nextgenComputeOpt(profile_expr, cgo);
}
BENCHMARK_DRAW_LINE();

#define BENCHMARK_GROUP(name, expr)                                      \
  BENCHMARK(name##Velox_________Base) { benchmark->veloxCompute(expr); } \
  BENCHMARK_RELATIVE(name##Nextgen) {                                    \
    CodegenOptions cgo;                                                  \
    benchmark->nextgenCompute(expr, cgo);                                \
  }                                                                      \
  BENCHMARK_RELATIVE(name##NextgenOpt) {                                 \
    CodegenOptions cgo;                                                  \
    benchmark->nextgenComputeOpt(expr, cgo);                             \
  }                                                                      \
  BENCHMARK_DRAW_LINE()

// BENCHMARK_GROUP(mulI8, "i8*i8");
// BENCHMARK_GROUP(mulI8Deep, "i8*i8*i8*i8");
// BENCHMARK_GROUP(mulI16, "i16*i16");
// BENCHMARK_GROUP(mulI32, "i32*i32");
// BENCHMARK_GROUP(mulI32Deep, "i32*i32*i32*i32");
// BENCHMARK_GROUP(mulI64, "i64*i64");

// BENCHMARK_GROUP(mulDouble, "a*b");
// BENCHMARK_GROUP(mulDoubleSameColumn, "a*a");
// BENCHMARK_GROUP(mulDoubleConstant, "a*constant");
// BENCHMARK_GROUP(mulDoubleNested, "a*b*b");
// BENCHMARK_GROUP(mulDoubleNestedDeep, "(a*b*a)*(a*(a*b))");

// BENCHMARK(PlusCheckedVeloxI64) {
//   benchmark->veloxCompute("checkedPlus(i64, i64)");
// }
// BENCHMARK_GROUP(plusI64, "plus(i64, i64)");

// BENCHMARK_GROUP(mulAndAdd, "a * 2.0 + a * 3.0 + a * 4.0 + a * 5.0");

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

  benchmark = std::make_unique<PipelineOperator>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
