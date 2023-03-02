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
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <filesystem>

#include "CiderVeloxPluginCtx.h"
#include "TpchInMemBuilder.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::connector::tpch;
using namespace facebook::velox::plugin;

DEFINE_double(scale_factor, 1, "tpch scale factor, delete data_path if it was changed");
DEFINE_int32(num_drivers, 1, "Number of drivers");

DEFINE_int32(run_query_verbose, -1, "Run a given query and print execution statistics");
DEFINE_bool(use_cider, true, "use cider plugin in verbose");
DEFINE_bool(include_stats, true, "Include execution statistics");
DEFINE_bool(include_custom_stats, false, "Include custom statistics");
DEFINE_bool(include_results, false, "Include results in the output");

namespace {
class TpchInMemBenchmark {
 public:
  void initialize() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();

    auto tpchConnector = connector::getConnectorFactory(
                             connector::tpch::TpchConnectorFactory::kTpchConnectorName)
                             ->newConnector(kTpchConnectorId_, nullptr);
    connector::registerConnector(tpchConnector);

    CiderVeloxPluginCtx::init();
  }

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(
      const TpchPlan& tpchPlan) {
    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        for (const auto& nodeId : tpchPlan.scanNodes) {
          for (size_t i = 0; i < FLAGS_num_drivers; ++i) {
            task->addSplit(
                nodeId,
                exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
                    kTpchConnectorId_, FLAGS_num_drivers, i)));
          }

          task->noMoreSplits(nodeId);
        }
      }
      noMoreSplits = true;
    };

    CursorParameters params;
    params.maxDrivers = FLAGS_num_drivers;
    params.planNode = tpchPlan.plan;
    return readCursor(params, addSplits);
  }

 private:
  const std::string kTpchConnectorId_{"test-tpch"};
};

void printResults(const std::vector<RowVectorPtr>& results) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for (const auto& vector : results) {
    // Print RowType only once.
    if (printType) {
      std::cout << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for (vector_size_t i = 0; i < vector->size(); ++i) {
      std::cout << vector->toString(i) << std::endl;
    }
  }
}

void benchmark_verbose(TpchInMemBenchmark& benchmark,
                       TpchInMemBuilder& queryBuilder,
                       int queryId,
                       bool useCider) {
  auto queryPlan = queryBuilder.getQueryPlan(queryId, FLAGS_scale_factor);
  if (useCider) {
    queryPlan.plan = CiderVeloxPluginCtx::transformVeloxPlan(queryPlan.plan);
  }
  const auto [cursor, actualResults] = benchmark.run(queryPlan);

  if (FLAGS_include_stats) {
    auto task = cursor->task();
    const auto stats = task->taskStats();
    std::cout << fmt::format("Execution time: {}",
                             succinctMillis(stats.executionEndTimeMs -
                                            stats.executionStartTimeMs))
              << std::endl;
    std::cout << fmt::format("Splits total: {}, finished: {}",
                             stats.numTotalSplits,
                             stats.numFinishedSplits)
              << std::endl;
    std::cout << printPlanWithStats(*queryPlan.plan, stats, FLAGS_include_custom_stats)
              << std::endl;
  }

  if (FLAGS_include_results) {
    printResults(actualResults);
    std::cout << std::endl;
  }
}
}  // namespace

TpchInMemBenchmark benchmark;
TpchInMemBuilder queryBuilder;

#define BENCHMARK_GROUP(n)                                                        \
  BENCHMARK(q##n##Velox) {                                                        \
    const auto planContext = queryBuilder.getQueryPlan(n, FLAGS_scale_factor);    \
    benchmark.run(planContext);                                                   \
  }                                                                               \
  BENCHMARK_RELATIVE(q##n##NextGen) {                                             \
    auto planContext = queryBuilder.getQueryPlan(n, FLAGS_scale_factor);          \
    planContext.plan = CiderVeloxPluginCtx::transformVeloxPlan(planContext.plan); \
    benchmark.run(planContext);                                                   \
  }                                                                               \
  BENCHMARK_DRAW_LINE()

BENCHMARK_GROUP(1);
BENCHMARK_GROUP(3);
BENCHMARK_GROUP(5);
// BENCHMARK_GROUP(6);
BENCHMARK_GROUP(7);
BENCHMARK_GROUP(8);
// BENCHMARK_GROUP(9);
BENCHMARK_GROUP(10);
BENCHMARK_GROUP(12);
BENCHMARK_GROUP(13);
BENCHMARK_GROUP(14);
BENCHMARK_GROUP(15);
// BENCHMARK_GROUP(16);
BENCHMARK_GROUP(18);
BENCHMARK_GROUP(19);
// BENCHMARK_GROUP(22);

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  benchmark.initialize();

  {
    // warmup `static folly::Singleton<DBGenBackend> DBGenBackendSingleton;`
    const auto planContext = queryBuilder.getWarmupPlan(FLAGS_scale_factor);
    benchmark.run(planContext);
  }

  if (FLAGS_run_query_verbose == -1) {
    folly::runBenchmarks();
  } else {
    benchmark_verbose(benchmark, queryBuilder, FLAGS_run_query_verbose, FLAGS_use_cider);
  }
}
