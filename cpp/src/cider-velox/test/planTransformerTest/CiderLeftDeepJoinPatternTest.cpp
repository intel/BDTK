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

namespace facebook::velox::plugin::plantransformer::test {

using facebook::velox::exec::test::PlanBuilder;
using namespace facebook::velox::core;

class CiderLeftDeepJoinPatternTest : public PlanTransformerTestBase {
 public:
  CiderLeftDeepJoinPatternTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<plantransformer::LeftDeepJoinPattern>(),
        std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }

 protected:
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
  RowTypePtr rowTypeLeft_{ROW({"c2", "c3"}, {BIGINT(), INTEGER()})};
  VeloxPlanNodePtr planRightPtr_ = PlanBuilder()
                                       .values(generateTestBatch(rowType_, false))
                                       .project({"c0 as u_c0", "c1 as u_c1"})
                                       .filter("u_c0 > 1")
                                       .project({"u_c0", "u_c1"})
                                       .planNode();

  VeloxPlanNodePtr planRight1Ptr_ =
      getSingleProjectNode(rowType_, {"c0 as u_c0", "c1 as u_c1"});

  VeloxPlanNodePtr planRight2Ptr_ = getSingleFilterNode(rowType_, "c0 > 1");

  VeloxPlanNodePtr expectedLeftPtr_ = PlanBuilder()
                                          .values(generateTestBatch(rowTypeLeft_, false))
                                          .filter("c2 > 3")
                                          .project({"c2", "c3"})
                                          .planNode();
};

TEST_F(CiderLeftDeepJoinPatternTest, JoinCompoundNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr_, "", {"c2", "c3", "u_c1"})
          .filter("u_c1 > 2")
          .project({"c2", "c3"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr_, planRightPtr_};

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .project({"c2", "c3"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinWithoutCompoundNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr_, "", {"c2", "c3", "u_c1"})
          .filter("c3 > 2")
          .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr_, planRightPtr_};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinWithAggNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr_, "", {"c2", "c3", "u_c1"})
          .partialAggregation({}, {"sum(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr_, planRightPtr_};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinWithFilterAggNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr_, "", {"c2", "c3", "u_c1"})
          .filter("c3 > 5")
          .partialAggregation({}, {"sum(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr_, planRightPtr_};

  VeloxPlanNodePtr expectedJoinPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .partialAggregation({}, {"sum(c2)"})
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .filter("c1 > 2")
          .project({"c2", "c3", "c1"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{
      getSingleFilterNode(rowTypeLeft_, "c2 > 3"), planRight1Ptr_, planRight2Ptr_};

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(
      compareWithExpected(resultPtr, getCiderExpectedPtr(rowTypeLeft_, joinSrcVec)));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinNodeAsRoot) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .project({"c2", "c3"})
          .hashJoin({"c2"}, {"u_c0"}, planRightPtr_, "", {"c2", "c3", "u_c1"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr_, planRightPtr_};

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(
      compareWithExpected(resultPtr, getCiderExpectedPtr(rowTypeLeft_, joinSrcVec)));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodesAsRoot) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec{
      getSingleFilterNode(rowTypeLeft_, "c2 > 3"), planRight1Ptr_, planRight2Ptr_};

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(
      compareWithExpected(resultPtr, getCiderExpectedPtr(rowTypeLeft_, joinSrcVec)));
}

TEST_F(CiderLeftDeepJoinPatternTest, MultiJoinNodes) {
  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"u_c0"}, planRight1Ptr_, "", {"c2", "c3", "u_c1"})
          .hashJoin({"c2"}, {"c0"}, planRight2Ptr_, "", {"c2", "c3", "c1"})
          .filter("c3 > 2")
          .project({"c2", "c3"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodeVec joinSrcVec1{getSingleFilterNode(rowTypeLeft_, "c2 > 3"),
                               planRight1Ptr_};

  VeloxPlanNodeVec joinSrcVec2{getCiderExpectedPtr(rowTypeLeft_, joinSrcVec1),
                               planRight2Ptr_};

  VeloxPlanNodePtr expectedPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .addNode([&joinSrcVec2](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec2);
          })
          .project({"c2", "c3"})
          .partialAggregation({}, {"SUM(c2)"})
          .planNode();

  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}
}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
