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

#include <folly/init/Init.h>
#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "RangedBatchGenerator.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/plan/parser/TypeUtils.h"
#include "substrait/VeloxPlanFragmentToSubstraitPlan.h"
#include "substrait/plan.pb.h"
#include "util/ArrowArrayBuilder.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::substrait;

using facebook::velox::test::BatchMaker;

class AggWithRandomDataTest : public OperatorTestBase {
  void SetUp() override {
    for (int32_t i = 0; i < 100; ++i) {
      auto vector =
          std::dynamic_pointer_cast<RowVector>(RangedBatchGenerator::createRangedBatch(
              rowType_, 1000, *pool_, colRange_, std::rand()));
      vectors.push_back(vector);
    }
    createDuckDbTable(vectors);
    CiderVeloxPluginCtx::init();
  }

  void TearDown() override { OperatorTestBase::TearDown(); }

 protected:
  void assertVeloxPlan(const std::string& ident,
                       const std::shared_ptr<const core::PlanNode>& plan) {
    auto startTime = std::chrono::system_clock::now();

    CursorParameters params;
    params.planNode = plan;
    auto result = readCursor(params, [](Task*) {});

    auto endTime = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);

    std::cout << ident << " execution takes " << duration.count() << " us" << std::endl;
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"l_orderkey", "l_linenumber", "l_discount", "l_extendedprice", "l_quantity"},
          {BIGINT(), INTEGER(), DOUBLE(), DOUBLE(), DOUBLE()})};
  std::vector<RowVectorPtr> vectors;
  RangedBatchGenerator::MinMaxRangeVec colRange_{{LONG_MIN, INT_MAX},
                                                 {0, 5000},
                                                 {0, 10},
                                                 {0, 1000},
                                                 {0, 10}};
  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanFragmentConvertor_ =
      std::make_shared<VeloxPlanFragmentToSubstraitPlan>();
};

TEST_F(AggWithRandomDataTest, SUM_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_linenumber", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_linenumber)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_linenumber), sum(l_discount) from tmp group by "
      "l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(AggWithRandomDataTest, SUM_COUNT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_linenumber", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"count(l_linenumber)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, count(l_linenumber), sum(l_discount) from tmp group "
      "by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(AggWithRandomDataTest, MIN_MAX_Test) {
  auto veloxPlan =
      PlanBuilder()
          .values(vectors)
          .project({"l_orderkey", "l_linenumber", "l_discount"})
          .aggregation({"l_orderkey"},
                       {"min(l_linenumber)", "max(l_linenumber)", "sum(l_discount)"},
                       {},
                       core::AggregationNode::Step::kPartial,
                       false)
          .planNode();

  std::string duckDbSql =
      "select l_orderkey, min(l_linenumber), max(l_linenumber), "
      "sum(l_discount) from tmp group by l_orderkey ";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

// TBD
// TEST_F(AggWithRandomDataTest, AVG_Single_Test) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .aggregation(
//                            {0},
//                            {"avg(l_discount)"},
//                            {},
//                            core::AggregationNode::Step::kSingle,
//                            false)
//                        .planNode();

//   auto ciderPlanNode = std::make_shared<CiderPlanNode>("100", veloxPlan);

//   std::string duckDbSql =
//       "select l_orderkey, avg(l_discount) from tmp group by l_orderkey ";
//   assertQuery(veloxPlan, duckDbSql);
//   assertQuery(ciderPlanNode, duckDbSql);
// }

// Data convertor doesn't support.
TEST_F(AggWithRandomDataTest, AVG_Partial_Test) {
  auto veloxPlan =
      PlanBuilder()
          .values(vectors)
          .project({"l_linenumber"})
          .aggregation(
              {}, {"avg(l_linenumber)"}, {}, core::AggregationNode::Step::kPartial, false)
          .planNode();

  auto substraitPlan =
      v2SPlanFragmentConvertor_->toSubstraitPlan(veloxPlan, veloxPlan->sources()[0]);

  int rows = 10;
  std::vector<int64_t> colId0(rows);
  std::vector<int32_t> colId1(rows);
  std::vector<double> colId2(rows);
  std::vector<double> colId3(rows);
  std::vector<double> colId4(rows);
  for (int i = 0; i < rows; i++) {
    colId0[i] = i;
    colId1[i] = i;
    colId2[i] = i + 0.1;
    colId3[i] = i + 0.1;
    colId4[i] = i + 0.1;
  }

  auto schema_and_array =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int64_t>("l_orderkey", CREATE_SUBSTRAIT_TYPE(I64), colId0)
          .addColumn<int32_t>("l_linenumber", CREATE_SUBSTRAIT_TYPE(I32), colId1)
          .addColumn<double>("l_discount", CREATE_SUBSTRAIT_TYPE(Fp64), colId2)
          .addColumn<double>("l_extendedprice", CREATE_SUBSTRAIT_TYPE(Fp64), colId3)
          .addColumn<double>("l_quantity", CREATE_SUBSTRAIT_TYPE(Fp64), colId4)
          .build();

  ArrowArray output_array;
  ArrowSchema output_schema;
  auto allocator_ = std::make_shared<CiderDefaultAllocator>();
  auto context =
      std::make_shared<cider::exec::processor::BatchProcessorContext>(allocator_);
  auto processor = makeBatchProcessor(substraitPlan, context);
  processor->processNextBatch(std::get<1>(schema_and_array),
                              std::get<0>(schema_and_array));
  processor->finish();
  processor->getResult(output_array, output_schema);

  double resSum = *(double*)(output_array.children[0]->children[0]->buffers[1]);
  int64_t resCount = *(int64_t*)(output_array.children[0]->children[1]->buffers[1]);
  CHECK_EQ(resSum, 45);
  CHECK_EQ(resCount, 10);
}

TEST_F(AggWithRandomDataTest, Filter_LT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("(l_quantity < 5.0)")
                       .project({"l_orderkey", "l_extendedprice", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_extendedprice)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice), sum(l_discount) from tmp where "
      "l_quantity < 5.0 group by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(AggWithRandomDataTest, Filter_GT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("(l_linenumber > 1000)")
                       .project({"l_orderkey", "l_extendedprice", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_extendedprice)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice), sum(l_discount) from tmp where "
      "l_linenumber > 1000 group by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(AggWithRandomDataTest, Filter_Proj_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_extendedprice * l_discount as revenue"})
                       .aggregation({"l_orderkey"},
                                    {"sum(revenue)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice * l_discount) from tmp group by "
      "l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

class IncrementalDataTest : public OperatorTestBase {
  void SetUp() override {
    for (int32_t i = 0; i < 100; ++i) {
      RangedBatchGenerator::MinMaxRangeVec colRange_{
          {0, 100.0 * i}, {0, 50.0 * i}, {0, 10}, {0, 10.0 * i}, {0, 10}};
      auto vector =
          std::dynamic_pointer_cast<RowVector>(RangedBatchGenerator::createRangedBatch(
              rowType_, 1000, *pool_, colRange_, std::rand()));
      vectors.push_back(vector);
    }
    createDuckDbTable(vectors);
    CiderVeloxPluginCtx::init();
  }

  void TearDown() override { OperatorTestBase::TearDown(); }

 protected:
  void assertVeloxPlan(const std::string& ident,
                       const std::shared_ptr<const core::PlanNode>& plan) {
    auto startTime = std::chrono::system_clock::now();

    CursorParameters params;
    params.planNode = plan;
    auto result = readCursor(params, [](Task*) {});

    auto endTime = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);

    std::cout << ident << " execution takes " << duration.count() << " us" << std::endl;
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"l_orderkey", "l_linenumber", "l_discount", "l_extendedprice", "l_quantity"},
          {BIGINT(), INTEGER(), DOUBLE(), DOUBLE(), DOUBLE()})};
  std::vector<RowVectorPtr> vectors;
};

TEST_F(IncrementalDataTest, SUM_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_linenumber", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_linenumber)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_linenumber), sum(l_discount) from tmp group by "
      "l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(IncrementalDataTest, SUM_COUNT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_linenumber", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"count(l_linenumber)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, count(l_linenumber), sum(l_discount) from tmp group "
      "by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(IncrementalDataTest, MIN_MAX_Test) {
  auto veloxPlan =
      PlanBuilder()
          .values(vectors)
          .project({"l_orderkey", "l_linenumber", "l_discount"})
          .aggregation({"l_orderkey"},
                       {"min(l_linenumber)", "max(l_linenumber)", "sum(l_discount)"},
                       {},
                       core::AggregationNode::Step::kPartial,
                       false)
          .planNode();

  std::string duckDbSql =
      "select l_orderkey, min(l_linenumber), max(l_linenumber), "
      "sum(l_discount) from tmp group by l_orderkey ";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(IncrementalDataTest, Filter_LT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("(l_quantity < 5.0)")
                       .project({"l_orderkey", "l_extendedprice", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_extendedprice)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice), sum(l_discount) from tmp where "
      "l_quantity < 5.0 group by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(IncrementalDataTest, Filter_GT_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("(l_linenumber > 1000)")
                       .project({"l_orderkey", "l_extendedprice", "l_discount"})
                       .aggregation({"l_orderkey"},
                                    {"sum(l_extendedprice)", "sum(l_discount)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice), sum(l_discount) from tmp where "
      "l_linenumber > 1000 group by l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

TEST_F(IncrementalDataTest, Filter_Proj_Test) {
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_orderkey", "l_extendedprice * l_discount as revenue"})
                       .aggregation({"l_orderkey"},
                                    {"sum(revenue)"},
                                    {},
                                    core::AggregationNode::Step::kPartial,
                                    false)
                       .planNode();

  std::string duckDbSql =
      "select l_orderkey, sum(l_extendedprice * l_discount) from tmp group by "
      "l_orderkey";
  assertQuery(veloxPlan, duckDbSql);
  auto ciderPlanNode = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  assertQuery(ciderPlanNode, duckDbSql);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
