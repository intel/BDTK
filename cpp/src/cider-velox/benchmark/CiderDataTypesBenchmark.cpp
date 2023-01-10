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
#include <velox/type/Type.h>

#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "cider/CiderRuntimeModule.h"
#include "substrait/plan.pb.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::substrait;

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

class CiderDataTypesBenchmark : public OperatorTestBase {
 public:
  explicit CiderDataTypesBenchmark(size_t vectorSize,
                                   std::vector<RowVectorPtr>& vectors) {
    for (int32_t i = 0; i < 1000; ++i) {
      // Generate input data batch
      VectorFuzzer::Options opts;
      opts.vectorSize = vectorSize;
      opts.nullRatio = 0.1;  // 10% null
      VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

      std::vector<VectorPtr> children;
      children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));
      children.emplace_back(fuzzer.fuzzFlat(TINYINT()));
      children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(REAL()));
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

      auto vector = std::make_shared<RowVector>(
          pool(), rowType_, nullptr, vectorSize, std::move(children));
      vectors.push_back(vector);
    }

    OperatorTestBase::SetUpTestCase();
    parse::registerTypeResolver();
    CiderVeloxPluginCtx::init();
  }

  ~CiderDataTypesBenchmark() override { OperatorTestBase::TearDown(); }

  void TestBody() override {}

 protected:
  std::shared_ptr<const RowType> rowType_{ROW(
      {"c_bool", "c_tiny_int", "c_small_int", "c_int", "c_big_int", "c_real", "c_double"},
      {BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};
};

std::vector<RowVectorPtr> vectors;
CiderDataTypesBenchmark benchmark(10'000, vectors);

void benchmarkOpExecution(const std::shared_ptr<const core::PlanNode>& plan) {
  CursorParameters params;
  params.queryCtx = core::QueryCtx::createForTest();
  params.planNode = plan;

  auto result = readCursor(params, [](Task*) {});
}

// TODO:
// 1. execute with multiple process loops (now handled by adding explicit
// outside loop calls)
// 2. dismiss compile time for cider execution
// hard for now since not able to access internal compile/process functions with
// high level calls
void benchmarkOpExecution(bool forCider,
                          std::string filter,
                          std::vector<std::string> projects = {},
                          std::vector<std::string> aggs = {},
                          int iteration = 1) {
  folly::BenchmarkSuspender suspender;
  auto planBuilder = PlanBuilder().values(vectors);
  if (filter != "") {
    planBuilder.filter(filter);
  }
  if (projects.size() > 0) {
    planBuilder.project(projects);
  }
  if (aggs.size() > 0) {
    planBuilder.aggregation({}, aggs, {}, core::AggregationNode::Step::kPartial, false);
  }
  auto veloxPlan = planBuilder.planNode();
  auto ciderPlan = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  auto planNode = forCider ? ciderPlan : veloxPlan;
  suspender.dismiss();
  FOR_EACH_RANGE(i, 0, iteration) { benchmarkOpExecution(planNode); }
}

// "select c_bool from tmp"
BENCHMARK(veloxProjectWithBool, n) {
  benchmarkOpExecution(false, "", {"c_bool"}, {}, n);
}

BENCHMARK_RELATIVE(ciderProjectWithBool, n) {
  benchmarkOpExecution(true, "", {"c_bool"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_tiny_int from tmp where c_tiny_int < 20"
BENCHMARK(veloxFilterProjectWithTinyInt, n) {
  benchmarkOpExecution(false, "c_tiny_int < 20", {"c_tiny_int"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithTinyInt, n) {
  benchmarkOpExecution(true, "c_tiny_int < 20", {"c_tiny_int"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_tiny_int) from tmp where c_tiny_int < 20"
BENCHMARK(veloxAggWithTinyInt, n) {
  benchmarkOpExecution(false, "c_tiny_int < 20", {"c_tiny_int"}, {"sum(c_tiny_int)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithTinyInt, n) {
  benchmarkOpExecution(true, "c_tiny_int < 20", {"c_tiny_int"}, {"sum(c_tiny_int)"}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_small_int from tmp where c_small_int < 20"
BENCHMARK(veloxFilterProjectWithSmallInt, n) {
  benchmarkOpExecution(false, "c_small_int < 20", {"c_small_int"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithSmallInt, n) {
  benchmarkOpExecution(true, "c_small_int < 20", {"c_small_int"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_small_int) from tmp where c_small_int < 20"
BENCHMARK(veloxAggWithSmallInt, n) {
  benchmarkOpExecution(
      false, "c_small_int < 20", {"c_small_int"}, {"sum(c_small_int)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithSmallInt, n) {
  benchmarkOpExecution(
      true, "c_small_int < 20", {"c_small_int"}, {"sum(c_small_int)"}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_int from tmp where c_int < 20"
BENCHMARK(veloxFilterProjectWithInt, n) {
  benchmarkOpExecution(false, "c_int < 20", {"c_int"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithInt, n) {
  benchmarkOpExecution(true, "c_int < 20", {"c_int"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_int) from tmp where c_int < 20"
BENCHMARK(veloxAggWithInt, n) {
  benchmarkOpExecution(false, "c_int < 20", {"c_int"}, {"sum(c_int)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithInt, n) {
  benchmarkOpExecution(true, "c_int < 20", {"c_int"}, {"sum(c_int)"}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_big_int from tmp where c_big_int < 20"
BENCHMARK(veloxFilterProjectWithBigInt, n) {
  benchmarkOpExecution(false, "c_big_int < 20", {"c_big_int"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithBigInt, n) {
  benchmarkOpExecution(true, "c_big_int < 20", {"c_big_int"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_big_int) from tmp where c_big_int < 20"
BENCHMARK(veloxAggWithBigInt, n) {
  benchmarkOpExecution(false, "c_big_int < 20", {"c_big_int"}, {"sum(c_big_int)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithBigInt, n) {
  benchmarkOpExecution(true, "c_big_int < 20", {"c_big_int"}, {"sum(c_big_int)"}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_real from tmp where c_real < 20"
BENCHMARK(veloxFilterProjectWithReal, n) {
  benchmarkOpExecution(false, "c_real < 20.5", {"c_real"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithReal, n) {
  benchmarkOpExecution(true, "c_real < 20.5", {"c_real"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_real) from tmp where c_real < 20"
BENCHMARK(veloxAggWithReal, n) {
  benchmarkOpExecution(false, "c_real < 20.5", {"c_real"}, {"sum(c_real)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithReal, n) {
  benchmarkOpExecution(true, "c_real < 20.5", {"c_real"}, {"sum(c_real)"}, n);
}

BENCHMARK_DRAW_LINE();

// "select c_double from tmp where c_double < 20"
BENCHMARK(veloxFilterProjectWithDouble, n) {
  benchmarkOpExecution(false, "c_double < 20.5", {"c_double"}, {}, n);
}

BENCHMARK_RELATIVE(ciderFilterProjectWithDouble, n) {
  benchmarkOpExecution(true, "c_double < 20.5", {"c_double"}, {}, n);
}

BENCHMARK_DRAW_LINE();

// "select sum(c_double) from tmp where c_double < 20"
BENCHMARK(veloxAggWithDouble, n) {
  benchmarkOpExecution(false, "c_double < 20.5", {"c_double"}, {"sum(c_double)"}, n);
}

BENCHMARK_RELATIVE(ciderAggWithDouble, n) {
  benchmarkOpExecution(true, "c_double < 20.5", {"c_double"}, {"sum(c_double)"}, n);
}

BENCHMARK_DRAW_LINE();

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::init(&argc, &argv, false);
  folly::runBenchmarks();
  return 0;
}
