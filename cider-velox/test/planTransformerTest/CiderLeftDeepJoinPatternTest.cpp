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

#include "CiderPlanTransformerIncludes.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class CiderLeftDeepJoinPatternTest : public PlanTransformerTestBase {
 public:
  CiderLeftDeepJoinPatternTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<plantransformer::LeftDeepJoinPattern>(),
        std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }
};
TEST_F(CiderLeftDeepJoinPatternTest, JoinCompoundNodes) {
  VeloxPlanBuilder planRightBranchBuilder;
  VeloxPlanNodePtr planRightPtr =
      planRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.filter()
                                     .proj()
                                     .hashjoin(planRightPtr)
                                     .filter()
                                     .proj()
                                     .partialAgg()
                                     .planNode();
  VeloxPlanBuilder expectedRightBranchBuilder;
  VeloxPlanNodePtr expectedRightPtr =
      expectedRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().proj().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, expectedRightPtr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinWithoutCompoundNodes) {
  VeloxPlanBuilder planRightBranchBuilder;
  VeloxPlanNodePtr planRightPtr =
      planRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr =
      planLeftBranchBuilder.filter().proj().hashjoin(planRightPtr).filter().planNode();
  VeloxPlanBuilder expectedRightBranchBuilder;
  VeloxPlanNodePtr expectedRightPtr =
      expectedRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().proj().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, expectedRightPtr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .filter()
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodes) {
  VeloxPlanBuilder planRight1BranchBuilder;
  VeloxPlanNodePtr planRight1Ptr = planRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder planRight2BranchBuilder;
  VeloxPlanNodePtr planRight2Ptr = planRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.filter()
                                     .hashjoin(planRight1Ptr)
                                     .hashjoin(planRight2Ptr)
                                     .filter()
                                     .proj()
                                     .partialAgg()
                                     .planNode();
  VeloxPlanBuilder expectedRight1BranchBuilder;
  VeloxPlanNodePtr expectedRight1Ptr = expectedRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr = expectedRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, expectedRight1Ptr, expectedRight2Ptr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, JoinNodeAsRoot) {
  VeloxPlanBuilder planRightBranchBuilder;
  VeloxPlanNodePtr planRightPtr =
      planRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr =
      planLeftBranchBuilder.filter().proj().hashjoin(planRightPtr).planNode();
  VeloxPlanBuilder expectedRightBranchBuilder;
  VeloxPlanNodePtr expectedRightPtr =
      expectedRightBranchBuilder.proj().filter().proj().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().proj().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, expectedRightPtr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, NotAcceptMultiJoinNodesAsRoot) {
  VeloxPlanBuilder planRight1BranchBuilder;
  VeloxPlanNodePtr planRight1Ptr = planRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder planRight2BranchBuilder;
  VeloxPlanNodePtr planRight2Ptr = planRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.filter()
                                     .hashjoin(planRight1Ptr)
                                     .hashjoin(planRight2Ptr)
                                     .planNode();
  VeloxPlanBuilder expectedRight1BranchBuilder;
  VeloxPlanNodePtr expectedRight1Ptr = expectedRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr = expectedRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, expectedRight1Ptr, expectedRight2Ptr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_FALSE(compareWithExpected(resultPtr, expectedJoinPtr));
}

TEST_F(CiderLeftDeepJoinPatternTest, MultiJoinNodes) {
  VeloxPlanBuilder planRight1BranchBuilder;
  VeloxPlanNodePtr planRight1Ptr = planRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder planRight2BranchBuilder;
  VeloxPlanNodePtr planRight2Ptr = planRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.filter()
                                     .hashjoin(planRight1Ptr)
                                     .hashjoin(planRight2Ptr)
                                     .filter()
                                     .proj()
                                     .partialAgg()
                                     .planNode();
  VeloxPlanBuilder expectedRight1BranchBuilder;
  VeloxPlanNodePtr expectedRight1Ptr = expectedRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr = expectedRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().planNode();
  VeloxPlanNodeVec joinSrcVec1{expectedLeftPtr, expectedRight1Ptr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode([&joinSrcVec1](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec1);
          })
          .planNode();
  VeloxPlanNodeVec joinSrcVec2{expectedJoinPtr, expectedRight2Ptr};
  expectedJoinPtr =
      expectedJoinBuilder
          .addNode([&joinSrcVec2](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, joinSrcVec2);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoinPtr));
}
}  // namespace facebook::velox::plugin::plantransformer::test
