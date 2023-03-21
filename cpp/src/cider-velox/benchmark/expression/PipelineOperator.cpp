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
#include <initializer_list>
#include <memory>

#include "Allocator.h"
#include "cider/CiderOptions.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/plan/lookup/FunctionLookupEngine.h"
#include "util/CiderBitUtils.h"
#include "utils.h"
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
DEFINE_int64(batch_size, 10'240, "batch size for one loop");
DEFINE_int64(loop_count, 10'000, "loop count for benchmark");
DEFINE_bool(profile_mode, false, "profile mode which just run once");
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

inline cider::CodegenOptions getBaseOption() {
  cider::CodegenOptions cgo;
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
    // prestosql::registerAllScalarFunctions();
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

    // for comparision
    registerBinaryScalar<EqFunction, bool>({"eq"});
    registerBinaryScalar<NeqFunction, bool>({"neq"});
    registerBinaryScalar<LtFunction, bool>({"lt"});
    registerBinaryScalar<GtFunction, bool>({"gt"});
    registerBinaryScalar<LteFunction, bool>({"lte"});
    registerBinaryScalar<GteFunction, bool>({"gte"});
  }

  void initialize(std::vector<std::string> colNames,
                  const std::initializer_list<std::string>& exprs) {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.nullRatio = FLAGS_ratio;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    // overhead of import to arrow array is very high
    // just add column which will be used in current test
    std::vector<std::shared_ptr<const Type>> types;
    std::vector<VectorPtr> children;
    for (auto& name : colNames) {
      // if name start with 'i8' (eg. i8, i8_1), then treat it as TINYINT
      for (auto& [prefix, type] : typeDict_) {
        if (name.starts_with(prefix)) {
          types.emplace_back(type);
          children.emplace_back(fuzzer.fuzzFlat(type));
          break;
        }
      }
    }

    inputType_ = ROW(std::move(colNames), std::move(types));
    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, FLAGS_batch_size, std::move(children));

    // prepare velox expression set
    std::vector<core::TypedExprPtr> expressions;
    for (auto& expr : exprs) {
      auto untypedExpr = parse::parseExpr(expr, options_);
      expressions.push_back(
          core::Expressions::inferTypes(untypedExpr, inputType_, execCtx_.pool()));
    }
    exprSet_ = std::make_unique<exec::ExprSet>(expressions, &execCtx_);

    // prepare velox plan for cider
    veloxPlan_ = PlanBuilder().values({rowVector_}).project(exprs).planNode();
    VirtualTableTrimer::trim(veloxPlan_);
  }

  __attribute__((noinline)) size_t veloxCompute(
      const std::initializer_list<std::string>& exprs) {
    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      count += evaluate(*exprSet_, rowVector_)->size();
    }
    return count;
  }

  __attribute__((noinline)) size_t nextgenCompute(
      cider::CodegenOptions cgo,
      const std::initializer_list<std::string>& exprs) {
    compile(veloxPlan_, cgo);

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      auto shared = rowVector_;
      addInput(shared);
      count += getOutput()->size();
    }

    return count;
  }

  __attribute__((noinline)) size_t nextgenComputeOpt(
      cider::CodegenOptions cgo,
      const std::initializer_list<std::string>& exprs) {
    compile(veloxPlan_, cgo);

    size_t count = 0;
    for (auto i = 0; i < FLAGS_loop_count; i++) {
      addInputOpt(rowVector_);
      count += getOutput()->size();
    }

    return count;
  }

 private:
  // mimic CiderPipelineOperator::CiderPipelineOperator(...)
  void compile(core::PlanNodePtr veloxPlan, cider::CodegenOptions cgo) {
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

    batchProcessor_ = cider::exec::processor::BatchProcessor::Make(plan, context, cgo);
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
    ArrowSchema inputArrowSchema;
    exportToArrow(input, inputArrowArray);
    exportToArrow(input, inputArrowSchema);

    batchProcessor_->processNextBatch(&inputArrowArray, &inputArrowSchema);
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
  std::unordered_map<std::string, std::shared_ptr<const Type>> typeDict_ = {
      {"i8", TINYINT()},
      {"i16", SMALLINT()},
      {"i32", INTEGER()},
      {"i64", BIGINT()},
      {"b", BOOLEAN()},
      {"d", DOUBLE()},
      {"c", VARCHAR()},
  };
  std::unique_ptr<exec::ExprSet> exprSet_;
  core::PlanNodePtr veloxPlan_;
};

std::unique_ptr<PipelineOperator> benchmark;
std::initializer_list<std::string> profile_expr;

BENCHMARK(velox) {
  benchmark->veloxCompute(profile_expr);
}
BENCHMARK_RELATIVE(nextgen) {
  auto cgo = getBaseOption();
  benchmark->nextgenCompute(cgo, profile_expr);
}
BENCHMARK_RELATIVE(nextgenOpt) {
  auto cgo = getBaseOption();
  benchmark->nextgenComputeOpt(cgo, profile_expr);
}
}  // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // warmup: preload YAML
  FunctionLookupEngine::getInstance(PlatformType::PrestoPlatform);

  benchmark = std::make_unique<PipelineOperator>();
  // mimic tpch q1 project expression
  benchmark->initialize(
      {"c", "c_1", "d", "d_1", "d_2", "d_3"},
      {"c", "c_1", "d", "d_1", "d_1*(1.0-d_2)", "d_1*(1.0-d_2)*(1.0+d_3)", "d_2"});
  // {"i64", "i64_1", "d", "d_1", "d_2", "d_3"},
  // {"i64", "i64_1", "d", "d_1", "d_1*(1.0-d_2)", "d_1*(1.0-d_2)*(1.0+d_3)", "d_2"});

  if (FLAGS_profile_mode) {
    CpuWallTiming t;
    {
      CpuWallTimer timer(t);
      benchmark->veloxCompute(profile_expr);
    }
    std::cout << t.toString() << std::endl;
    t.clear();

    auto cgo = getBaseOption();
    {
      CpuWallTimer timer(t);
      benchmark->nextgenCompute(cgo, profile_expr);
    }
    std::cout << t.toString() << std::endl;
    t.clear();

    {
      CpuWallTimer timer(t);
      benchmark->nextgenComputeOpt(cgo, profile_expr);
    }
    std::cout << t.toString() << std::endl;
  } else {
    folly::runBenchmarks();
  }
  benchmark.reset();
  return 0;
}
