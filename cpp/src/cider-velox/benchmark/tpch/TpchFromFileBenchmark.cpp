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
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

namespace fs = std::filesystem;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::connector::tpch;
using namespace facebook::velox::plugin;

DEFINE_string(data_path_prefix, "./tpch_data", "path prefix of TPC-H data");
DEFINE_double(scale_factor, 1, "tpch scale factor");

DEFINE_bool(use_native_parquet_reader,
            true,
            "Use Native Parquet Reader, only support GZIP parquet files");
DEFINE_int32(num_drivers, 1, "Number of drivers");
DEFINE_int32(num_splits_per_file, 1, "Number of splits per file");

DEFINE_int32(run_query_verbose, -1, "Run a given query and print execution statistics");
DEFINE_bool(include_results, false, "Include results in the output");
DEFINE_bool(include_stats, false, "Include execution statistics");
DEFINE_bool(include_custom_stats,
            false,
            "Include custom statistics along with execution statistics");

namespace {

class TpchFromFileBenchmark {
 public:
  void initialize() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();

    filesystems::registerLocalFileSystem();
    if (FLAGS_use_native_parquet_reader) {
      parquet::registerParquetReaderFactory(parquet::ParquetReaderType::NATIVE);
    } else {
      parquet::registerParquetReaderFactory(parquet::ParquetReaderType::DUCKDB);
    }
    auto hiveConnector = connector::getConnectorFactory(
                             connector::hive::HiveConnectorFactory::kHiveConnectorName)
                             ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);

    CiderVeloxPluginCtx::init();
  }

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(
      const TpchPlan& tpchPlan) {
    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        for (const auto& entry : tpchPlan.dataFiles) {
          for (const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, FLAGS_num_splits_per_file, tpchPlan.dataFileFormat);
            for (const auto& split : splits) {
              task->addSplit(entry.first, exec::Split(split));
            }
          }
          task->noMoreSplits(entry.first);
        }
      }
      noMoreSplits = true;
    };

    CursorParameters params;
    params.maxDrivers = FLAGS_num_drivers;
    params.planNode = tpchPlan.plan;
    return readCursor(params, addSplits);
  }

  void saveTpchTablesAsParquet(const std::string& tpchDataDir, double tpchScaleFactor) {
    if (fs::is_directory(tpchDataDir)) {
      return;
    }

    const std::unordered_map<std::string, std::string> duckDbParquetWriteSQL = {
        std::make_pair("lineitem",
                       R"(COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
         l_quantity::DOUBLE as quantity, l_extendedprice::DOUBLE as extendedprice, l_discount::DOUBLE as discount,
         l_tax::DOUBLE as tax, l_returnflag, l_linestatus, l_shipdate AS shipdate, l_commitdate, l_receiptdate,
         l_shipinstruct, l_shipmode, l_comment FROM {})
         TO '{}'(FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("orders",
                       R"(COPY (SELECT o_orderkey, o_custkey, o_orderstatus,
         o_totalprice::DOUBLE as o_totalprice,
         o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("customer",
                       R"(COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone,
         c_acctbal::DOUBLE as c_acctbal, c_mktsegment, c_comment FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("nation",
                       R"(COPY (SELECT * FROM {})
          TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("region",
                       R"(COPY (SELECT * FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("part",
                       R"(COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size,
         p_container, p_retailprice::DOUBLE, p_comment FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("supplier",
                       R"(COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone,
         s_acctbal::DOUBLE, s_comment FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))"),
        std::make_pair("partsupp",
                       R"(COPY (SELECT ps_partkey, ps_suppkey, ps_availqty,
         ps_supplycost::DOUBLE as supplycost, ps_comment FROM {})
         TO '{}' (FORMAT 'parquet', CODEC 'GZIP', ROW_GROUP_SIZE {}))")};

    // generate data
    auto duckDb = std::make_shared<DuckDbQueryRunner>();
    duckDb->initializeTpch(tpchScaleFactor);

    // write to parquet files
    fs::create_directory(tpchDataDir);
    for (const auto& [tableName, SQLTemplate] : duckDbParquetWriteSQL) {
      auto tableDirectory = fmt::format("{}/{}", tpchDataDir, tableName);
      fs::create_directory(tableDirectory);

      auto filePath = fmt::format("{}/file.parquet", tableDirectory);
      constexpr int kRowGroupSize = 10'000;
      auto query =
          fmt::format(fmt::runtime(SQLTemplate), tableName, filePath, kRowGroupSize);
      duckDb->execute(query);
    }
  }

 private:
  const std::string kTpchConnectorId_{"test-tpch"};
};

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}

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

uint64_t benchmark_verbose(TpchFromFileBenchmark& benchmark,
                           std::shared_ptr<TpchQueryBuilder>& queryBuilder,
                           int queryId,
                           bool isCider) {
  auto queryPlan = queryBuilder->getQueryPlan(queryId);
  if (isCider) {
    queryPlan.plan = CiderVeloxPluginCtx::transformVeloxPlan(queryPlan.plan);
  }
  const auto [cursor, actualResults] = benchmark.run(queryPlan);
  auto task = cursor->task();
  ensureTaskCompletion(task.get());

  if (FLAGS_include_results) {
    printResults(actualResults);
    std::cout << std::endl;
  }

  const auto stats = task->taskStats();
  if (FLAGS_include_stats) {
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
  return stats.executionEndTimeMs - stats.executionStartTimeMs;
}

}  // namespace

TpchFromFileBenchmark benchmark;
std::shared_ptr<TpchQueryBuilder> queryBuilder;

#define BENCHMARK_GROUP(n)                                                        \
  BENCHMARK(q##n##Velox) {                                                        \
    const auto planContext = queryBuilder->getQueryPlan(n);                       \
    benchmark.run(planContext);                                                   \
  }                                                                               \
  BENCHMARK_RELATIVE(q##n##NextGen) {                                             \
    auto planContext = queryBuilder->getQueryPlan(n);                             \
    planContext.plan = CiderVeloxPluginCtx::transformVeloxPlan(planContext.plan); \
    benchmark.run(planContext);                                                   \
  }                                                                               \
  BENCHMARK_DRAW_LINE()

BENCHMARK_GROUP(1);
BENCHMARK_GROUP(3);
// BENCHMARK_GROUP(5);
// BENCHMARK_GROUP(6);
// BENCHMARK_GROUP(7);
// BENCHMARK_GROUP(8);
// BENCHMARK_GROUP(9);
// BENCHMARK_GROUP(10);
// BENCHMARK_GROUP(12);
// BENCHMARK_GROUP(13);
// BENCHMARK_GROUP(14);
// BENCHMARK_GROUP(15);
// BENCHMARK_GROUP(16);
// BENCHMARK_GROUP(18);
// BENCHMARK_GROUP(19);
// BENCHMARK_GROUP(22);

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  std::string tpchDir =
      fmt::format("{}_sf{}", FLAGS_data_path_prefix, FLAGS_scale_factor);

  benchmark.initialize();
  benchmark.saveTpchTablesAsParquet(tpchDir, FLAGS_scale_factor);

  queryBuilder = std::make_shared<TpchQueryBuilder>(FileFormat::PARQUET);
  queryBuilder->initialize(tpchDir);

  if (FLAGS_run_query_verbose == -1) {
    folly::runBenchmarks();
  } else {
    auto elasped_velox =
        benchmark_verbose(benchmark, queryBuilder, FLAGS_run_query_verbose, false);
    auto elasped_cider =
        benchmark_verbose(benchmark, queryBuilder, FLAGS_run_query_verbose, true);
    std::cout << "velox time: " << elasped_velox << "ms, cider time: " << elasped_cider
              << "ms, diff: " << double(elasped_velox) / double(elasped_cider)
              << std::endl;
  }
}
