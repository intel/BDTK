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

#include <folly/Benchmark.h>
#include <gflags/gflags.h>
#include <google/protobuf/util/json_util.h>
#include <memory>

#include "Allocator.h"
#include "cider/CiderOptions.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "util/CiderBitUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

// #include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This file refers velox/velox/benchmarks/basic/SimpleArithmetic.cpp
DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_double(ratio, 0.5, "NULL ratio in batch");
DEFINE_int64(batch_size, 1'0240, "batch size for one loop");
DEFINE_int64(loop_count, 1'000'000, "loop count for benchmark");
DEFINE_bool(dump_ir, false, "dump llvm ir");
DEFINE_bool(dump_plan, false, "dump substrait plan");

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
  PipelineOperator() : FunctionBenchmarkBase() {
    // registerAllScalarFunctions() just register checked version for integer
    // we register uncheck version for compare
    registerFunction<MultiplyFunction, int8_t, int8_t, int8_t>({"multiply"});
    registerFunction<MultiplyFunction, int16_t, int16_t, int16_t>({"multiply"});
    registerFunction<MultiplyFunction, int32_t, int32_t, int32_t>({"multiply"});
    registerFunction<MultiplyFunction, int64_t, int64_t, int64_t>({"multiply"});
    registerFunction<MultiplyFunction, double, double, double>({"multiply"});
    registerFunction<CheckedDivideFunction, int8_t, int8_t, int8_t>({"divide"});
    registerFunction<DivideFunction, int16_t, int16_t, int16_t>({"divide"});
    registerFunction<DivideFunction, int32_t, int32_t, int32_t>({"divide"});
    registerFunction<DivideFunction, int64_t, int64_t, int64_t>({"divide"});
    registerFunction<DivideFunction, double, double, double>({"divide"});
    registerFunction<PlusFunction, int8_t, int8_t, int8_t>({"plus"});
    registerFunction<PlusFunction, int16_t, int16_t, int16_t>({"plus"});
    registerFunction<PlusFunction, int32_t, int32_t, int32_t>({"plus"});
    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    registerFunction<PlusFunction, double, double, double>({"plus"});
    registerFunction<MinusFunction, int8_t, int8_t, int8_t>({"minus"});
    registerFunction<MinusFunction, int16_t, int16_t, int16_t>({"minus"});
    registerFunction<MinusFunction, int32_t, int32_t, int32_t>({"minus"});
    registerFunction<MinusFunction, int64_t, int64_t, int64_t>({"minus"});
    registerFunction<MinusFunction, double, double, double>({"minus"});
  }

  void generateData(
      std::initializer_list<std::pair<const std::string, std::shared_ptr<const Type>>>&&
          pairs) {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.nullRatio = FLAGS_ratio;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    // overhead of import to arrow array is very high
    std::vector<VectorPtr> children;
    for (auto& [_, type] : pairs) {
      children.emplace_back(fuzzer.fuzzFlat(type));  // i64
    }

    inputType_ = ROW(std::move(pairs));
    rowVector_ = std::make_shared<RowVector>(
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
      auto shared = rowVector_;
      addInput(shared);
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
    if (FLAGS_dump_plan) {
      std::string output;
      auto status = google::protobuf::util::MessageToJsonString(plan, &output);
      if (!status.ok()) {
        std::cerr << "Failed to dump plan as json: " + status.message().as_string()
                  << std::endl;
      }
      std::cout << output << std::endl;
    }

    auto allocator = std::make_shared<PoolAllocator>(pool());
    auto context = std::make_shared<BatchProcessorContext>(allocator);

    batchProcessor_ = makeBatchProcessor(plan, context, cgo);
  }

  // mimic `void CiderPipelineOperator::addInput(RowVectorPtr input)`
  void addInput(RowVectorPtr input) {
    for (size_t i = 0; i < input->childrenSize(); i++) {
      input->childAt(i)->mutableRawNulls();
    }
    this->input_ = std::move(input);
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
    ArrowArray inputArrowArray;
    exportToArrow(input, inputArrowArray);

    batchProcessor_->processNextBatch(&inputArrowArray);
  }

  // mimic `void CiderPipelineOperator::getOutput()`
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

// {
//     // {"i8", TINYINT()},
//     // {"i16", SMALLINT()},
//     // {"i32", INTEGER()},
//     // {"i64", BIGINT()},
//     // {"b", BOOLEAN()},
//     // {"d", DOUBLE()},
// });
auto profile_expr = "i16*i32";
BENCHMARK(velox) {
  benchmark->generateData({{"i16", SMALLINT()}, {"i32", INTEGER()}});
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
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<PipelineOperator>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
