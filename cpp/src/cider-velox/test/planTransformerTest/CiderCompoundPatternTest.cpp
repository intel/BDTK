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
#include "CiderPlanTransformerIncludes.h"

using namespace facebook::velox::plugin;

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class CiderCompoundPatternTest : public PlanTransformerTestBase {
 public:
  CiderCompoundPatternTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<plantransformer::CompoundPattern>(),
        std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }

 protected:
  std::vector<std::string> projections_ = {"c0", "c1", "c0 + 2"};
  std::string filter_ = "c0 > 2 ";
  std::vector<std::string> aggs_ = {"SUM(c1)"};
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
};

TEST_F(CiderCompoundPatternTest, filterProjectAgg) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .project(projections_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();

  VeloxPlanNodePtr expectedPtr = getCiderExpectedPtr(rowType_);

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, projectAgg) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .project(projections_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();

  VeloxPlanNodePtr expectedPtr = getCiderExpectedPtr(rowType_);

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, projectTopN) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .project(projections_)
                                 .topN({"c0"}, 10, true)
                                 .planNode();

  VeloxPlanNodePtr expectedPtr = getCiderExpectedPtr(rowType_);
  // TODO: (Jie) enable this after TopN Node supported by cider-velox and cider.
  GTEST_SKIP();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, projectNoPartialTopN) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .project(projections_)
                                 .topN({"c0"}, 10, false)
                                 .planNode();

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .topN({"c0"}, 10, false)
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, filterProject) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .project(projections_)
                                 .planNode();
  VeloxPlanNodePtr expectedPtr = getCiderExpectedPtr(rowType_);

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, projectFilter) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .project(projections_)
                                 .filter(filter_)
                                 .planNode();
  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .filter(filter_)
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, FilterAgg) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(CiderCompoundPatternTest, singleProject) {
  VeloxPlanNodePtr planPtr = getSingleProjectNode(rowType_, projections_);

  VeloxPlanNodePtr expectedPtr = getCiderExpectedPtr(rowType_);

  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, SingleBranchMultiCompoundNodes) {
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter(filter_)
                                 .project(projections_)
                                 .filter(filter_)
                                 .project(projections_)
                                 .partialAggregation({}, aggs_)
                                 .planNode();
  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, MultiBranchesMultiCompoundNodes) {
  RowTypePtr rowTypeLeft{ROW({"c2", "c3"}, {BIGINT(), INTEGER()})};

  VeloxPlanNodePtr planRightPtr = PlanBuilder()
                                      .values(generateTestBatch(rowType_, false))
                                      .project({"c0 as u_c0", "c1 as u_c1"})
                                      .filter("u_c0 > 1")
                                      .project({"u_c0", "u_c1"})
                                      .planNode();

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr, "", {"c2", "c3", "u_c1"})
          .filter("u_c1 > 2")
          .project({"c2", "c3", "u_c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodePtr expectedRightPtr =
      PlanBuilder()
          .values(generateTestBatch(rowType_, false))
          .project({"c0 as u_c0", "c1 as u_c1"})
          // .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
          //   return std::make_shared<TestCiderPlanNode>(id, input);
          // })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft, false))
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .hashJoin({"c2"}, {"u_c0"}, expectedRightPtr, "", {"c2", "c3", "u_c1"})
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();

  EXPECT_TRUE(compareWithExpected(resultPtr, expectedLeftPtr));
}

}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
