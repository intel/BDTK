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

class CiderOperatorBenchmark : public OperatorTestBase {
 public:
  explicit CiderOperatorBenchmark(size_t vectorSize, std::vector<RowVectorPtr>& vectors) {
    for (int32_t i = 0; i < 1000; ++i) {
      // Generate input data batch
      VectorFuzzer::Options opts;
      opts.vectorSize = vectorSize;
      opts.nullRatio = 0;  // no null
      VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

      std::vector<VectorPtr> children;
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));   // l_orderkey
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));  // l_linenumber
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // l_discount
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // l_extendedprice
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // l_quantity
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));   // l_shipdate

      opts.nullRatio = 0.5;  // 50%
      fuzzer.setOptions(opts);
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // HalfNull

      opts.nullRatio = 1.0;  // 100%
      fuzzer.setOptions(opts);
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // FullNull

      opts.nullRatio = (vectorSize - 1) / vectorSize;  // one not null (almost null)
      fuzzer.setOptions(opts);
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));  // OneNotNull

      auto vector = std::make_shared<RowVector>(
          pool(), rowType_, nullptr, vectorSize, std::move(children));
      vectors.push_back(vector);
    }

    OperatorTestBase::SetUpTestCase();
    parse::registerTypeResolver();
    CiderVeloxPluginCtx::init();
  }

  ~CiderOperatorBenchmark() override { OperatorTestBase::TearDown(); }

  void TestBody() override {}

 protected:
  std::shared_ptr<const RowType> rowType_{ROW({"l_orderkey",
                                               "l_linenumber",
                                               "l_discount",
                                               "l_extendedprice",
                                               "l_quantity",
                                               "l_shipdate",
                                               "half_null",
                                               "full_null",
                                               "one_notnull"},
                                              {BIGINT(),
                                               INTEGER(),
                                               DOUBLE(),
                                               DOUBLE(),
                                               DOUBLE(),
                                               DOUBLE(),
                                               DOUBLE(),
                                               DOUBLE(),
                                               DOUBLE()})};
};

std::vector<RowVectorPtr> vectors;
CiderOperatorBenchmark benchmark(10'000, vectors);

void benchmarkOpCompilation(const std::shared_ptr<const core::PlanNode>& planNode) {
  folly::BenchmarkSuspender suspender;

  auto ciderPlanNode = std::static_pointer_cast<const CiderPlanNode>(planNode);
  const ::substrait::Plan plan = ciderPlanNode->getSubstraitPlan();

  // Set up exec option and compilation option
  auto exec_option = CiderExecutionOption::defaults();
  auto compile_option = CiderCompilationOption::defaults();

  exec_option.output_columnar_hint = false;
  compile_option.max_groups_buffer_entry_guess = 16384;
  compile_option.use_cider_groupby_hash = true;
  compile_option.use_default_col_range = true;

  auto ciderCompileModule_ = CiderCompileModule::Make();

  suspender.dismiss();

  auto result = ciderCompileModule_->compile(plan, compile_option, exec_option);
}

// TODO:
// 1. execute with multiple process loops (now handled by adding explicit
// outside loop calls)
// 2. dismiss compile time for cider execution
// hard for now since not able to access internal compile/process functions with
// high level calls
void benchmarkOpExecution(const std::shared_ptr<const core::PlanNode>& plan,
                          const bool disable_adaptivity) {
  CursorParameters params;

  params.queryCtx = core::QueryCtx::createForTest();
  // if disable adaptive, should also set force_direc t_hash_ = true in cider part
  // to ensure apple-to-apple compare
  if (disable_adaptivity) {
    params.queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kHashAdaptivityEnabled, "false"}});
  }
  params.planNode = plan;

  auto result = readCursor(params, [](Task*) {});
}

// refactor refer to CiderDataTypesBenchmark.cpp
std::shared_ptr<const core::PlanNode> buildPlanNode(
    bool forCider,
    std::string filter,
    std::vector<std::string> projects = {},
    std::vector<std::string> group_keys = {},
    std::vector<std::string> aggs = {}) {
  parse::registerTypeResolver();
  auto planBuilder = PlanBuilder().values(vectors);
  if (filter != "") {
    planBuilder.filter(filter);
  }
  if (projects.size() > 0) {
    planBuilder.project(projects);
  }
  if (aggs.size() > 0) {
    planBuilder.aggregation(
        group_keys, aggs, {}, core::AggregationNode::Step::kPartial, false);
  }
  auto veloxPlan = planBuilder.planNode();
  auto ciderPlan = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  auto planNode = forCider ? ciderPlan : veloxPlan;
  return planNode;
}

void benchmarkOpCompilation(bool forCider,
                            std::string filter,
                            std::vector<std::string> projects = {},
                            std::vector<std::string> group_keys = {},
                            std::vector<std::string> aggs = {}) {
  folly::BenchmarkSuspender suspender;
  auto planNode = buildPlanNode(forCider, filter, projects, group_keys, aggs);
  suspender.dismiss();
  benchmarkOpCompilation(planNode);
}

void benchmarkOpExecution(bool forCider,
                          bool disable_adaptivity,
                          std::string filter,
                          std::vector<std::string> projects = {},
                          std::vector<std::string> group_keys = {},
                          std::vector<std::string> aggs = {}) {
  folly::BenchmarkSuspender suspender;
  auto planNode = buildPlanNode(forCider, filter, projects, group_keys, aggs);
  suspender.dismiss();
  for (int i = 0; i < 100; i++) {
    benchmarkOpExecution(planNode, disable_adaptivity);
  }
}

#define VP_BENCHMARK(name, disable_adaptivity, ...)                 \
  BENCHMARK(FB_CONCATENATE(BM_velox_exec_, name)) {                 \
    benchmarkOpExecution(false, disable_adaptivity, ##__VA_ARGS__); \
  }

#define VP_BENCHMARK_RELATIVE(name, disable_adaptivity, ...)       \
  BENCHMARK_RELATIVE(FB_CONCATENATE(BM_cider_exec_, name)) {       \
    benchmarkOpExecution(true, disable_adaptivity, ##__VA_ARGS__); \
  }

#define VP_BENCHMARK_COMPILE(name, ...)                \
  BENCHMARK(FB_CONCATENATE(BM_cider_compile_, name)) { \
    benchmarkOpCompilation(true, ##__VA_ARGS__);       \
  }

#define VP_BENCHMARK_ALL(name, disable_adaptivity, ...)           \
  VP_BENCHMARK(name, disable_adaptivity, ##__VA_ARGS__);          \
  VP_BENCHMARK_RELATIVE(name, disable_adaptivity, ##__VA_ARGS__); \
  VP_BENCHMARK_COMPILE(name, ##__VA_ARGS__);

// "select * from tmp where l_quantity < 0.5"
VP_BENCHMARK_ALL(filter_double, false, "l_quantity < 0.5");
BENCHMARK_DRAW_LINE();

// "select * from tmp where l_orderkey < 100"
VP_BENCHMARK_ALL(filter_int, false, "l_orderkey < 100");
BENCHMARK_DRAW_LINE();

// "select * from tmp where multiply(multiply(multiply(l_linenumber,
// l_quantity), l_linenumber), multiply(l_linenumber, multiply(l_linenumber,
// l_quantity))) < 100.0"
VP_BENCHMARK_ALL(filter_complex,
                 false,
                 "multiply(multiply(multiply(l_linenumber, l_quantity), l_linenumber), "
                 "multiply(l_linenumber, multiply(l_linenumber, l_quantity))) < 100.0");
BENCHMARK_DRAW_LINE();

// "select * from tmp where half_null < 0.5"
VP_BENCHMARK_ALL(filter_null, false, "half_null < 0.5");
BENCHMARK_DRAW_LINE();

// "select * from tmp where full_null < 0.5"
VP_BENCHMARK_ALL(filter_full_null, false, "full_null < 0.5");
BENCHMARK_DRAW_LINE();

// "select * from tmp where one_notnull < 0.5"
VP_BENCHMARK_ALL(filter_one_notnull, false, "one_notnull < 0.5");
BENCHMARK_DRAW_LINE();

// "select l_extendedprice * l_discount as revenue from tmp"
VP_BENCHMARK_ALL(project, false, "", {"l_extendedprice * l_discount as revenue"});
BENCHMARK_DRAW_LINE();

// "select l_extendedprice * l_discount as revenue from tmp where l_quantity <
// 0.5"
VP_BENCHMARK_ALL(filter_project,
                 false,
                 "l_quantity < 0.5",
                 {"l_extendedprice * l_discount as revenue"});
BENCHMARK_DRAW_LINE();

// "select l_orderkey, sum(revenue) from tmp where
// l_quantity < 0.5 group by l_orderkey"
VP_BENCHMARK_ALL(filter_project_groupby,
                 true,
                 "l_quantity < 0.5",
                 {"l_orderkey", "l_extendedprice * l_discount as revenue"},
                 {"l_orderkey"},
                 {"sum(revenue)"});
BENCHMARK_DRAW_LINE();

// "select * from tmp where l_linenumber > 10 and l_discount < 0.5"
VP_BENCHMARK_ALL(filter_and, false, "l_linenumber > 10 AND l_discount < 0.5");
BENCHMARK_DRAW_LINE();

// "select l_linenumber > 10 and l_discount < 0.5 from tmp"
VP_BENCHMARK_ALL(project_and, false, "", {"l_linenumber > 10 AND l_discount < 0.5"});
BENCHMARK_DRAW_LINE();

// "select l_orderkey, sum(l_linenumber), sum(l_discount) from tmp group by
// l_orderkey"
VP_BENCHMARK_ALL(groupby,
                 true,
                 "",
                 {},
                 {"l_orderkey"},
                 {"sum(l_linenumber)", "sum(l_discount)"});
BENCHMARK_DRAW_LINE();

// select max(l_orderkey) from tmp
VP_BENCHMARK_ALL(max_on_col, false, "", {"l_orderkey"}, {}, {"max(l_orderkey)"});
BENCHMARK_DRAW_LINE();

// select min(l_orderkey) from tmp
VP_BENCHMARK_ALL(min_on_col, false, "", {"l_orderkey"}, {}, {"min(l_orderkey)"});
BENCHMARK_DRAW_LINE();

// select sum(l_quantity) from tmp
VP_BENCHMARK_ALL(sum_on_col, false, "", {"l_orderkey"}, {}, {"sum(l_orderkey)"});
BENCHMARK_DRAW_LINE();

// select sum(cast(l_linenumber as bigint) as linenumber) from tmp
VP_BENCHMARK_ALL(sum_cast_on_col,
                 false,
                 "",
                 {"cast(l_linenumber as bigint) as linenumber"},
                 {},
                 {"sum(linenumber)"});
BENCHMARK_DRAW_LINE();

// Q6-like query
VP_BENCHMARK_ALL(
    q6,
    false,
    "l_shipdate >= 0.67 AND l_shipdate < 0.89 AND l_discount between 0.05 AND "
    "0.07 AND l_quantity < 0.24",
    {"l_extendedprice * l_discount as revenue"},
    {},
    {"sum(revenue)"});
BENCHMARK_DRAW_LINE();

// Comparison Ops
VP_BENCHMARK_ALL(comparison,
                 false,
                 "",
                 {"3 < 2", "3 > 2", "3 = 2", "3 = 2", "3 <= 2", "3 >= 2", "3 <> 2"});
BENCHMARK_DRAW_LINE();

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::init(&argc, &argv, false);
  folly::runBenchmarks();
  return 0;
}
