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

#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include <memory>
#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "ciderTransformer/CiderPlanTransformerFactory.h"
#include "planTransformerTest/utils/PlanTansformerTestUtil.h"
#include "substrait/VeloxPlanFragmentToSubstraitPlan.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::substrait;
using namespace facebook::velox::plugin::plantransformer;
using namespace facebook::velox::plugin::plantransformer::test;

using facebook::velox::test::BatchMaker;

class CiderOperatorTest : public OperatorTestBase {
  void SetUp() override {
    for (int32_t i = 0; i < 10; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType_, 100, *pool_));
      vectors.push_back(vector);
    }
    createDuckDbTable(vectors);
    CiderVeloxPluginCtx::init();
    v2SPlanConvertor = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();
    plan = std::make_shared<::substrait::Plan>();
  }

  void TearDown() override { OperatorTestBase::TearDown(); }

 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"l_orderkey",
           "l_linenumber",
           "l_discount",
           "l_extendedprice",
           "l_quantity",
           "l_shipdate"},
          {BIGINT(), INTEGER(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()})};

  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanConvertor;
  std::shared_ptr<::substrait::Plan> plan;
  std::vector<RowVectorPtr> vectors;
};

// TEST_F(CiderOperatorTest, filter) {
//   const std::string& filter = "l_quantity  > 24.0";
//   auto veloxPlan = PlanBuilder().values(vectors).filter(filter).planNode();
//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   assertQuery(resultPtr, "SELECT * FROM tmp WHERE " + filter);
// }

// TEST_F(CiderOperatorTest, project) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .project({"l_extendedprice * l_discount as revenue"})
//                        .planNode();
//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   assertQuery(resultPtr, "SELECT l_extendedprice * l_discount as revenue FROM tmp");
// }

// TEST_F(CiderOperatorTest, Q6) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter(
//               "l_shipdate >= 8765.666666666667 and l_shipdate < "
//               "9130.666666666667 and l_discount between 0.05 and "
//               "0.07 and l_quantity < 24.0")
//           .project({"l_extendedprice * l_discount as revenue"})
//           .aggregation(
//               {}, {"sum(revenue)"}, {}, core::AggregationNode::Step::kPartial, false)
//           .planNode();
//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   std::string duckDbSql =
//       "select sum(l_extendedprice * l_discount) as revenue from tmp where "
//       "l_shipdate >= 8765.666666666667 and l_shipdate < 9130.666666666667 and "
//       "l_discount between 0.05 and 0.07 and l_quantity < 24.0";
//    assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, filter_only) {
//   const std::string& filter = "l_quantity < 0.5";
//   auto veloxPlan = PlanBuilder().values(vectors).filter(filter).planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql = "SELECT * FROM tmp WHERE " + filter;

//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, project_only) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .project({"l_extendedprice * l_discount as revenue"})
//                        .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql = "SELECT l_extendedprice * l_discount as revenue FROM tmp";

//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, fil_proj_transformer) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .filter("l_quantity < 0.5")
//                        .project({"l_extendedprice * l_discount as revenue"})
//                        .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql =
//       "select l_extendedprice * l_discount as revenue from tmp where "
//       "l_quantity < 0.5";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, fil_proj_filter_transformer) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .filter("l_quantity < 0.5")
//                        .project({"l_extendedprice * l_discount as revenue"})
//                        .filter("revenue > 0.1")
//                        .project({"revenue"})
//                        .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql =
//       "select l_extendedprice * l_discount as revenue from tmp where "
//       "l_quantity < 0.5 and revenue > 0.1";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, agg) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .filter("l_quantity < 0.5")
//                        .project({"l_extendedprice * l_discount as revenue"})
//                        .partialAggregation({}, {"sum(revenue)"})
//                        .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql =
//       "select sum(l_extendedprice * l_discount) as revenue from tmp where "
//       "l_quantity < 0.5";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, multi_agg) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("l_quantity < 0.5")
//           .project({"l_extendedprice * l_discount as revenue", "l_quantity as
//           sum_quan"}) .partialAggregation({}, {"sum(revenue)", "sum(sum_quan)"})
//           .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql =
//       "select sum(l_extendedprice * l_discount) as revenue, sum(l_quantity) as "
//       "sum_quan from tmp where l_quantity < 0.5";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, min_max) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("l_quantity < 0.5")
//           .project({"l_extendedprice as min_extendprice", "l_discount as
//           max_discount"}) .partialAggregation({}, {"min(min_extendprice)",
//           "max(max_discount)"}) .planNode();

//   auto transformer = CiderPlanTransformerFactory().getTransformer(veloxPlan);
//   auto resultPtr = transformer->transform();

//   std::string duckDbSql =
//       "select min(l_extendedprice) as  min_extendprice, max(l_discount) as max_discount
//       " "from tmp where " "l_quantity < 0.5";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, sumOnExpr_withoutCond) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .project({"l_extendedprice * l_discount as revenue",
//                                  "l_discount * 0.5 as max_discount"})
//                        .partialAggregation({}, {"sum(revenue)", "max(max_discount)"})
//                        .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);

//   std::string duckDbSql =
//       "select sum(l_extendedprice * l_discount) as revenue, max(l_discount * 0.5) as "
//       "max_discount from tmp";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, maxMinExpr_withoutCond) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .project({"l_discount * 0.5 as discount"})
//                        .partialAggregation({}, {"max(discount)", "min(discount)"})
//                        .planNode();
//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   std::string duckDbSql =
//       "select "
//       " max(l_discount * 0.5) as max_discount,"
//       " min(l_discount * 0.5) as min_discount "
//       "from tmp";
//   assertQuery(resultPtr, duckDbSql);
// }

// TEST_F(CiderOperatorTest, aggOnExpr_withoutCond) {
//   auto veloxPlan = PlanBuilder()
//                        .values(vectors)
//                        .project({"l_extendedprice * l_discount as revenue",
//                                  "(l_extendedprice + 2.5) * l_discount as revenue1",
//                                  "l_discount * 0.5 as discount",
//                                  "l_linenumber",
//                                  "cast(l_linenumber as bigint) as linenumber"})
//                        .partialAggregation({},
//                                            {"sum(revenue)",
//                                             "sum(revenue1)",
//                                             "max(discount)",
//                                             "min(discount)",
//                                             "sum(linenumber)",
//                                             "count(l_linenumber)"})
//                        .planNode();
//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   std::string duckDbSql =
//       "select sum(l_extendedprice * l_discount) as revenue, "
//       "sum((l_extendedprice + 2.5) * l_discount) as revenue1, "
//       " max(l_discount * 0.5) as max_discount,"
//       " min(l_discount* 0.5) as min_discount,"
//       " sum(cast(l_linenumber as bigint)) as sum_linenumber,"
//       " count(l_linenumber) as cnt "
//       " from tmp";
//   assertQuery(resultPtr, duckDbSql);
// }

// // Filter and Project using Cider.
// TEST_F(CiderOperatorTest, avg_on_col_final) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("l_shipdate < 24.0")
//           .partialAggregation(
//               {"l_orderkey", "l_linenumber"}, {"avg(l_quantity) as avg_price"}, {})
//           .finalAggregation()
//           .project({"avg_price"})
//           .planNode();
//   const ::substrait::Plan substraitPlan = ::substrait::Plan();
//   auto expectedPlan =
//       PlanBuilder()
//           .values(vectors)
//           .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
//             return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
//                 CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
//           })
//           .partialAggregation(
//               {"l_orderkey", "l_linenumber"}, {"avg(l_quantity) as avg_price"}, {})
//           .finalAggregation()
//           .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
//             return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
//                 CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
//           })
//           .planNode();

//   auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   auto duckdbSql =
//       "SELECT avg(l_quantity) as avg_price FROM tmp WHERE l_shipdate < 24.0 GROUP BY "
//       "l_orderkey, l_linenumber";
//   assertQuery(resultPtr, duckdbSql);

//   EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
// }

// TEST_F(CiderOperatorTest, avg_on_col_cider) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("l_shipdate < 24.0")
//           .project({"l_orderkey", "l_linenumber", "l_quantity"})
//           .partialAggregation(
//               {"l_orderkey", "l_linenumber"}, {"avg(l_quantity) as avg_price"}, {})
//           .finalAggregation()
//           .planNode();
//   // TODO: enable this after rowType convertor support.
//   // const ::substrait::Plan substraitPlan = ::substrait::Plan();
//   // auto expectedPlan =
//   //     PlanBuilder()
//   //         .values(vectors)
//   //         .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input)
//   {
//   //           return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
//   //              CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
//   //         })
//   //         .finalAggregation()
//   //         .planNode();
//   // std::cout << expectedPlan->toString(true, true) << std::endl;

//   // auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   auto duckdbSql =
//       "SELECT l_orderkey, l_linenumber, avg(l_quantity) as avg_price FROM tmp WHERE "
//       "l_shipdate < 24.0 GROUP BY l_orderkey, l_linenumber";
//   // assertQuery(resultPtr, duckdbSql);

//   assertQuery(veloxPlan, duckdbSql);
//   // EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
// }

// TEST_F(CiderOperatorTest, avg_on_col_debug) {
//   auto veloxPlan =
//       PlanBuilder()
//           .values(vectors)
//           .filter("l_shipdate < 24.0")
//           .project({"l_orderkey", "l_linenumber", "l_quantity"})
//           .partialAggregation(
//               {"l_orderkey", "l_linenumber"}, {"avg(l_quantity) as avg_price"}, {})
//           .finalAggregation()
//           .planNode();
//   // TODO: enable this after rowType convertor support.
//   // const ::substrait::Plan substraitPlan = ::substrait::Plan();
//   // auto expectedPlan =
//   //     PlanBuilder()
//   //         .values(vectors)
//   //         .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input)
//   //{
//   //           return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
//   //              CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
//   //         })
//   //         .finalAggregation()
//   //         .planNode();
//   // std::cout << expectedPlan->toString(true, true) << std::endl;

//   // auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
//   auto duckdbSql =
//       "SELECT l_orderkey, l_linenumber, avg(l_quantity) as avg_price FROM tmp WHERE "
//       "l_shipdate < 24.0 GROUP BY l_orderkey, l_linenumber";
//   // assertQuery(resultPtr, duckdbSql);

//   assertQuery(veloxPlan, duckdbSql);
//   // EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
// }

TEST_F(CiderOperatorTest, debug_avg) {
  auto data = makeRowVector({makeFlatVector<int64_t>(10, [](auto row) { return row; })});
  createDuckDbTable({data});
  auto veloxPlan = PlanBuilder()
                       .values({data})
                       .project({"c0"})
                       .partialAggregation({}, {"avg(c0) as avg_ccccc"}, {})
                       .planNode();

  auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  // auto duckdbSql = "SELECT  avg(l_quantity) FROM tmp WHERE l_shipdate < 24.0 ";
  // assertQuery(resultPtr, duckdbSql);
  auto duckdbSql = "SELECT  row(45, 10)";
  // auto duckdbSql = "SELECT c0, avg(c0) as avg_ccccc from tmp group by c0";
  assertQuery(veloxPlan, duckdbSql);
  std::cout << "Cider Results=========" << std::endl;
  assertQuery(resultPtr, duckdbSql);
  // EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
