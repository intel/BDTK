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

#include "substrait/VeloxPlanFragmentToSubstraitPlan.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"

#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class VeloxPlanFragmentSubstraitConverterTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(), INTEGER(), INTEGER(), BIGINT(), DOUBLE(), DOUBLE(), INTEGER()})};

  /*!
   * @param size  the number of RowVectorPtr.
   * @param childSize the size of rowType_, the number of columns
   * @param batchSize batch size.
   * @return std::vector<RowVectorPtr>
   */
  std::vector<RowVectorPtr> makeVector(int64_t size,
                                       int64_t childSize,
                                       int64_t batchSize) {
    std::vector<RowVectorPtr> vectors;
    std::mt19937 gen(std::mt19937::default_seed);
    for (int i = 0; i < size; i++) {
      std::vector<VectorPtr> children;
      for (int j = 0; j < childSize; j++) {
        children.emplace_back(makeFlatVector<int32_t>(batchSize, [&](auto /*row*/) {
          return folly::Random::rand32(INT32_MAX / 4, INT32_MAX / 2, gen);
        }));
      }

      vectors.push_back(makeRowVector({children}));
    }

    return vectors;
  }

  // Please don't use this function if the sourceNode is ValuesNode.
  void assertPlanConversion(const core::PlanNodePtr& targetNode,
                            const core::PlanNodePtr& sourceNode,
                            const std::string& duckDbSql) {
    // Construct new velox plan according targetNode and sourceNode.
    v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();
    auto constructVeloxPlan =
        v2SPlanFragmentConvertor_->constructVeloxPlan(targetNode, sourceNode);

    // Set new values for the new construct velox plan.
    auto vectors =
        v2SPlanFragmentConvertor_->makeVectors(sourceNode->sources()[0]->outputType());
    createDuckDbTable(vectors);

    assertQuery(constructVeloxPlan, duckDbSql);
    assertQueryReturnsEmptyResult(constructVeloxPlan);

    // Convert Velox Plan to Substrait Plan.
    std::shared_ptr<::substrait::Plan> sPlan = std::make_shared<::substrait::Plan>(
        v2SPlanFragmentConvertor_->toSubstraitPlan(targetNode, sourceNode));
    // Convert Substrait Plan to the same Velox Plan fragment.
    auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);
    // sPlan->PrintDebugString();
    // samePlan->toString(true,true);

    // Assert velox plan again.
    assertQuery(samePlan, duckDbSql);
    assertQueryReturnsEmptyResult(samePlan);
  }

  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanFragmentConvertor_;
  std::shared_ptr<SubstraitVeloxPlanConverter> substraitConverter_ =
      std::make_shared<SubstraitVeloxPlanConverter>(pool_.get());
};

TEST_F(VeloxPlanFragmentSubstraitConverterTest, orderBySingleKey) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan =
      PlanBuilder().values(vectors).orderBy({"c0 DESC NULLS LAST"}, false).planNode();
  assertQuery(plan, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST");

  assertPlanConversion(plan, plan, "SELECT * FROM tmp ORDER BY c0 DESC NULLS LAST");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, orderBy) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 ASC NULLS FIRST", "c1 ASC NULLS LAST"}, false)
                  .planNode();

  assertQuery(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST");
  assertPlanConversion(
      plan, plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, orderByPartial) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder()
                  .values(vectors)
                  .orderBy({"c0 ASC NULLS FIRST", "c1 ASC NULLS LAST"}, true)
                  .planNode();
  assertQuery(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST");
  assertPlanConversion(
      plan, plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST, c1 NULLS LAST");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, Limit) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).limit(0, 10, false).planNode();

  assertQuery(plan, "SELECT * FROM tmp LIMIT 10");
  assertPlanConversion(plan, plan, "SELECT * FROM tmp LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, LimitPartial) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).limit(0, 10, true).planNode();

  assertQuery(plan, "SELECT * FROM tmp LIMIT 10");

  assertPlanConversion(plan, plan, "SELECT * FROM tmp LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, LimitOffest) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).limit(5, 10, false).planNode();

  assertQuery(plan, "SELECT * FROM tmp OFFSET 5 LIMIT 10");
  assertPlanConversion(plan, plan, "SELECT * FROM tmp OFFSET 5 LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, topN) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan =
      PlanBuilder().values(vectors).topN({"c0 NULLS FIRST"}, 10, false).planNode();
  assertPlanConversion(plan, plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, topNPartial) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder().values(vectors).topN({"c0 NULLS FIRST"}, 10, true).planNode();

  assertQuery(plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST LIMIT 10");

  assertPlanConversion(plan, plan, "SELECT * FROM tmp ORDER BY c0 NULLS FIRST LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, topNFilter) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 15")
                  .topN({"c0 NULLS FIRST"}, 10, true)
                  .planNode();
  assertQuery(plan, "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 NULLS FIRST LIMIT 10");

  assertPlanConversion(
      plan,
      plan->sources()[0],
      "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 NULLS FIRST LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, topNTwoKeys) {
  auto vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 15")
                  .topN({"c0 NULLS FIRST", "c1 DESC NULLS LAST"}, 10, true)
                  .planNode();

  assertQuery(plan,
              "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 NULLS FIRST, c1 DESC "
              "NULLS LAST LIMIT 10");

  assertPlanConversion(plan,
                       plan->sources()[0],
                       "SELECT * FROM tmp WHERE c0 > 15 ORDER BY c0 NULLS FIRST, c1 DESC "
                       "NULLS LAST LIMIT 10");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, avg_on_col_single) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("c5 < 24.0")
                       .singleAggregation({"c0", "c1"}, {"avg(c4) as avg_price"})
                       .project({"avg_price"})
                       .planNode();
  auto duckdbSql = "SELECT avg(c4) as avg_price FROM tmp WHERE c5 < 24.0 GROUP BY c0, c1";

  assertQuery(veloxPlan, duckdbSql);

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  // Convert Velox Plan to Substrait Plan.
  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          veloxPlan, veloxPlan->sources()[0]->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(samePlan, duckdbSql);
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, avg_on_col_fragment) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("c5 < 24.0")
                       .singleAggregation({"c0", "c1"}, {"avg(c4) as avg_price"})
                       .project({"avg_price"})
                       .planNode();

  assertQuery(veloxPlan,
              "SELECT avg(c4) as avg_price FROM tmp WHERE c5 < 24.0 GROUP BY c0, c1");

  // Process the plan fragment project->agg.
  assertPlanConversion(veloxPlan,
                       veloxPlan->sources()[0],
                       "SELECT avg(c4) as avg_price FROM tmp GROUP BY c0, c1");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, avg_on_col_final) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter("c5 < 24.0")
                       .partialAggregation({"c0", "c1"}, {"avg(c4) as avg_price"}, {})
                       .finalAggregation()
                       .project({"avg_price"})
                       .planNode();

  auto duckdbSql = "SELECT avg(c4) as avg_price FROM tmp WHERE c5 < 24.0 GROUP BY c0, c1";

  assertQuery(veloxPlan, duckdbSql);

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          veloxPlan, veloxPlan->sources()[0]->sources()[0]->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(samePlan, duckdbSql);
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, filterProject) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 / c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");

  // Process plan fragment, project->filter.
  assertPlanConversion(
      vPlan, vPlan->sources()[0], "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, ProjectNodeOnly) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 / c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");

  // Process plan fragment that only has one project node.
  assertPlanConversion(vPlan, vPlan, "SELECT c0, c1, c0 / c1 FROM tmp");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, ProjectFullPlan) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 / c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          vPlan, vPlan->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(samePlan, "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, filter) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 / c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 / c1 FROM tmp WHERE c1 > 0");

  // Process plan fragment that not start from root node: filter->values.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          vPlan->sources()[0], vPlan->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(samePlan, "SELECT * FROM tmp WHERE c1 > 0");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, InvalidSourceValuesNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 > 0");

  // Invalid sourceNode and it's valuesNode.
  auto invalidSource = PlanBuilder().values(vectors).planNode();

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  EXPECT_ANY_THROW(
      std::shared_ptr<::substrait::Plan> sPlan = std::make_shared<::substrait::Plan>(
          v2SPlanFragmentConvertor_->toSubstraitPlan(vPlan, invalidSource)));
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, InvalidSourceNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 > 0");

  // Invalid sourceNode and it's not valuesNode.
  auto invalidSource = PlanBuilder().values(vectors).filter("c2 > 1").planNode();

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();
  EXPECT_ANY_THROW(
      std::shared_ptr<::substrait::Plan> sPlan = std::make_shared<::substrait::Plan>(
          v2SPlanFragmentConvertor_->toSubstraitPlan(vPlan, invalidSource)));
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, InvalidTarget) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 > 0");

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  // Invalid target node that from the same plan fragment.
  EXPECT_ANY_THROW(
      std::shared_ptr<::substrait::Plan> sPlan = std::make_shared<::substrait::Plan>(
          v2SPlanFragmentConvertor_->toSubstraitPlan(vPlan->sources()[0], vPlan)));
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, InvalidTargetFromOtherPlan) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c1 > 0")
                   .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                   .planNode();

  assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 > 0");

  // Invalid targetNode that from other plan.
  auto invalidTarget = PlanBuilder().values(vectors).filter("c2 > 1").planNode();

  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  EXPECT_ANY_THROW(
      std::shared_ptr<::substrait::Plan> sPlan = std::make_shared<::substrait::Plan>(
          v2SPlanFragmentConvertor_->toSubstraitPlan(invalidTarget, vPlan)));
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, sourcefilter) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .aggregation({"c0", "c1"},
                               {"sum(c4) as num_price"},
                               {},
                               core::AggregationNode::Step::kPartial,
                               false)
                  .project({"num_price"})
                  .planNode();

  assertQuery(plan, "SELECT sum(c4) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");

  // Process plan fragment Project->Aggregation and filterNode Output is
  // the input of source node.
  assertPlanConversion(
      plan, plan->sources()[0], "SELECT sum(c4) as num_price FROM tmp GROUP BY c0, c1");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, sourceAggregate) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .aggregation({"c0", "c1"},
                               {"sum(c4) as num_price"},
                               {},
                               core::AggregationNode::Step::kPartial,
                               false)
                  .project({"num_price"})
                  .planNode();
  assertQuery(plan, "SELECT sum(c4) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
  // Process plan fragment only contains ProjectNode and aggregationNode output  is the
  // input of the source node.
  assertPlanConversion(plan, plan, "SELECT num_price FROM tmp");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, sourceProject) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .project({"c0", "c1", "c4 "})
                  .aggregation({"c0", "c1"},
                               {"sum(c4) as num_price"},
                               {},
                               core::AggregationNode::Step::kPartial,
                               false)
                  .planNode();
  assertQuery(
      plan, "SELECT c0, c1, sum(c4) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
  // Process plan fragment only contains aggregationNode, and Project output is  the input
  // of the source node.
  assertPlanConversion(
      plan, plan, "SELECT sum(c4) as num_price FROM tmp GROUP BY c0, c1");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, having_simple) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c0 > 2")
                   .project({"c0"})
                   .partialAggregation({"c0"}, {"sum(c0) as sum_a"})
                   .finalAggregation()
                   .planNode();

  assertQuery(vPlan, "SELECT c0, SUM(c0) AS sum_a FROM tmp GROUP BY c0 HAVING c0 > 2");
  // Process plan fragment : Agg->project->filter.

  assertPlanConversion(vPlan,
                       vPlan->sources()[0]->sources()[0]->sources()[0],
                       "SELECT c0, SUM(c0) AS sum_a FROM tmp GROUP BY c0 HAVING c0 > 2");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, having_simple_FullPlan) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c0 > 2 and c0 < 10")
                   .project({"c0"})
                   .partialAggregation({"c0"}, {"sum(c0) as sum_a"})
                   .finalAggregation()
                   .planNode();

  assertQuery(
      vPlan,
      "SELECT c0, SUM(c0) AS sum_a FROM tmp WHERE c0 < 10 GROUP BY c0 HAVING c0 > 2");
  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          vPlan, vPlan->sources()[0]->sources()[0]->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(
      samePlan,
      "SELECT c0, SUM(c0) AS sum_a FROM tmp WHERE c0 < 10 GROUP BY c0 HAVING c0 > 2");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, having_agg_FullPlan) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .filter("c0 < 10")
                   .project({"c0"})
                   .partialAggregation({"c0"}, {"sum(c0) as sum_a"})
                   .filter("sum_a > 2")
                   .project({"c0", "sum_a"})
                   .planNode();

  assertQuery(vPlan,
              "SELECT c0, SUM(c0) AS sum_a FROM tmp WHERE c0 < 10 GROUP BY c0 HAVING "
              "SUM(c0) > 2");
  // Process the whole plan.
  v2SPlanFragmentConvertor_ = std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          vPlan,
          vPlan->sources()[0]->sources()[0]->sources()[0]->sources()[0]->sources()[0]));
  // Convert Substrait Plan to the same Velox Plan fragment.
  auto samePlan = substraitConverter_->toVeloxPlan(*sPlan);

  // Assert velox again.
  assertQuery(samePlan,
              "SELECT c0, SUM(c0) AS sum_a FROM tmp WHERE c0 < 10 GROUP BY c0 HAVING "
              "SUM(c0) > 2");
}

TEST_F(VeloxPlanFragmentSubstraitConverterTest, having_agg) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto vPlan = PlanBuilder()
                   .values(vectors)
                   .project({"c0"})
                   .partialAggregation({"c0"}, {"sum(c0) as sum_a"})
                   .finalAggregation()
                   .filter("sum_a > 2")
                   .project({"c0", "sum_a"})
                   .planNode();

  assertQuery(vPlan,
              "SELECT c0, SUM(c0) AS sum_a FROM tmp GROUP BY c0 HAVING SUM(c0) > 2");

  // Process plan fragment（exclude values node）
  assertPlanConversion(
      vPlan,
      vPlan->sources()[0]->sources()[0]->sources()[0]->sources()[0],
      "SELECT c0, SUM(c0) AS sum_a FROM tmp GROUP BY c0 HAVING SUM(c0) > 2");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
