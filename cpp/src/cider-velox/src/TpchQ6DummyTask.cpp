/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "TpchQ6DummyTask.h"

// #include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

#include "CiderVeloxPluginCtx.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/duckdb_reader/ParquetReader.h"

#include <gflags/gflags.h>
#include "velox/common/base/tests/Fs.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox::aggregate;

using namespace facebook::velox;

using namespace facebook::velox::plugin;
using facebook::velox::tpch::Table;
namespace {
std::string getFormatDateFilter(const std::string& stringDate,
                                const RowTypePtr& rowType,
                                const std::string& lowerBound,
                                const std::string& upperBound) {
  bool isDwrf = rowType->findChild(stringDate)->isVarchar();
  auto suffix = isDwrf ? "" : "::DATE";

  if (!lowerBound.empty() && !upperBound.empty()) {
    return fmt::format(
        "{} between {}{} and {}{}", stringDate, lowerBound, suffix, upperBound, suffix);
  } else if (!lowerBound.empty()) {
    return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
  } else if (!upperBound.empty()) {
    return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
  }

  VELOX_FAIL("Date range check expression must have either a lower or an upper bound");
}

void printResult(const RowVectorPtr result) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  // Print RowType only once.
  if (printType) {
    std::cout << result->type()->asRow().toString() << std::endl;
    printType = false;
  }
  for (int32_t i = 0; i < result->size(); ++i) {
    std::cout << result->toString(i) << std::endl;
  }
}

};  // namespace

namespace trino::velox {

TpchQ6DummyTask::TpchQ6DummyTask() {
  CiderVeloxPluginCtx::init();
  functions::prestosql::registerAllScalarFunctions();
  registerSumAggregate<SumAggregate>(kSum);
  parse::registerTypeResolver();
  filesystems::registerLocalFileSystem();
  parquet::registerParquetReaderFactory(parquet::ParquetReaderType::NATIVE);
  const std::string kHiveConnectorId = "test-hive";
  auto hiveConnector = connector::getConnectorFactory(
                           connector::hive::HiveConnectorFactory::kHiveConnectorName)
                           ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};
  auto path = "/WorkSpace/nextgen/BDTK/lineitem-10/lineitem";
  std::string singleFilePath;
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_regular_file()) {
      continue;
    }
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    singleFilePath = dirEntry.path();
  }
  dwio::common::ReaderOptions readerOptions;
  readerOptions.setFileFormat(dwio::common::FileFormat::PARQUET);
  std::unique_ptr<dwio::common::Reader> reader =
      dwio::common::getReaderFactory(readerOptions.getFileFormat())
          ->createReader(std::make_unique<dwio::common::FileInputStream>(singleFilePath),
                         readerOptions);
  const auto fileType = reader->rowType();
  const auto fileColumnNames = fileType->names();
  auto columns = tpch::getTableSchema(tpch::Table::TBL_LINEITEM)->names();
  // There can be extra columns in the file towards the end.
  VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
  std::unordered_map<std::string, std::string> fileColumnNamesMap(columns.size());
  std::transform(columns.begin(),
                 columns.end(),
                 fileColumnNames.begin(),
                 std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
                 [](std::string a, std::string b) { return std::make_pair(a, b); });
  auto columnNames = columns;
  auto types = fileType->children();
  types.resize(columnNames.size());
  auto tableTypes = std::make_shared<RowType>(std::move(columnNames), std::move(types));
  auto columnSelector =
      std::make_shared<dwio::common::ColumnSelector>(tableTypes, selectedColumns);
  auto selectedRowType = columnSelector->buildSelectedReordered();
  const auto shipDate = "l_shipdate";
  auto shipDateFilter =
      getFormatDateFilter(shipDate, selectedRowType, "'1994-01-01'", "'1994-12-31'");

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan("lineitem", selectedRowType, fileColumnNamesMap)
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .filter(
                      "l_shipdate <= '1994-12-31' and l_shipdate >= '1994-01-01' and "
                      "l_quantity < 24.0 and l_discount >= 0.05 and l_discount <= 0.07")
                  // .filter(shipDateFilter + // velox btw_varchar_varchar
                  //         " and l_quantity < 24.0 and l_discount >= 0.05 and l_discount
                  //         <= 0.07")
                  // .filter("l_discount between 0.05 and 0.07")
                  // .filter("l_quantity < 24.0")
                  .project({"l_extendedprice * l_discount"})
                  .partialAggregation({}, {"sum(p0)"})
                  .planFragment();
  auto rootNode = plan.planNode;
  plan.planNode = CiderVeloxPluginCtx::transformVeloxPlan(rootNode);
  task_ = std::make_shared<exec::Task>(
      "single.execution.task.0", plan, 0, std::make_shared<core::QueryCtx>());
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_regular_file()) {
      continue;
    }
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    // 1 spilt in the file
    auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
        dirEntry.path(), 1, dwio::common::FileFormat::PARQUET);
    for (const auto& split : splits) {
      task_->addSplit(lineitemPlanNodeId, exec::Split(split));
    }
  }

  task_->noMoreSplits(lineitemPlanNodeId);
  VELOX_CHECK(task_->supportsSingleThreadedExecution());
  is_finished_ = false;
}

void TpchQ6DummyTask::nextBatch(ArrowSchema* c_schema, ArrowArray* c_array) {
  auto result = task_->next();
  if (result) {
    for (auto& child : result->children()) {
      child->loadedVector();
    }
    // printResult(result);
    // for (size_t i = 0; i < result->childrenSize(); i++) {
    //   result->childAt(i)->mutableRawNulls();
    // }
    // exportToArrow(result, *c_array);
    // exportToArrow(result, *c_schema);
  } else {
    is_finished_ = true;
  }
}

bool TpchQ6DummyTask::isFinished() {
  return is_finished_;
}

void TpchQ6DummyTask::close() {}

std::shared_ptr<TpchQ6Task> TpchQ6Task::Make() {
  return std::make_shared<TpchQ6DummyTask>();
}
}  // namespace trino::velox