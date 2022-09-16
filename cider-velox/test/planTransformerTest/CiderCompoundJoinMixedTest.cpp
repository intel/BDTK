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
class CiderCompoundJoinMixedTest : public PlanTransformerTestBase {
 public:
  CiderCompoundJoinMixedTest() {
    auto transformerFactory =
        PlanTransformerFactory()
            .registerPattern(std::make_shared<plantransformer::CompoundPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>())
            .registerPattern(std::make_shared<plantransformer::LeftDeepJoinPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>())
            .registerPattern(std::make_shared<plantransformer::FilterPattern>(),
                             std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }
};

TEST_F(CiderCompoundJoinMixedTest, filterOnly) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, filterAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().partialAgg().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .partialAgg()
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, filterProjectAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundJoinMixedTest, SingleJoinMultiCompoundNodes) {
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
      expectedRightBranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr =
      expectedLeftBranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
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

TEST_F(CiderCompoundJoinMixedTest, DontMergeMultiJoinNodes) {
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
  VeloxPlanNodePtr expectedRight1Ptr =
      expectedRight1BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr =
      expectedRight2BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr =
      expectedLeftBranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
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

TEST_F(CiderCompoundJoinMixedTest, MultiJoinNodes) {
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
  VeloxPlanNodePtr expectedRight1Ptr =
      expectedRight1BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr =
      expectedRight2BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr =
      expectedLeftBranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
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

TEST_F(CiderCompoundJoinMixedTest, MultiSeperatedJoinNodes) {
  VeloxPlanBuilder planRight1BranchBuilder;
  VeloxPlanNodePtr planRight1Ptr = planRight1BranchBuilder.proj().planNode();
  VeloxPlanBuilder planRight2BranchBuilder;
  VeloxPlanNodePtr planRight2Ptr = planRight2BranchBuilder.filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.filter()
                                     .hashjoin(planRight1Ptr)
                                     .filter()
                                     .proj()
                                     .hashjoin(planRight2Ptr)
                                     .filter()
                                     .proj()
                                     .partialAgg()
                                     .planNode();
  VeloxPlanBuilder expectedRight1BranchBuilder;
  VeloxPlanNodePtr expectedRight1Ptr =
      expectedRight1BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedRight2BranchBuilder;
  VeloxPlanNodePtr expectedRight2Ptr =
      expectedRight2BranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr =
      expectedLeftBranchBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanNodeVec join1SrcVec{expectedLeftPtr, expectedRight1Ptr};
  VeloxPlanBuilder expectedJoin1Builder;
  VeloxPlanNodePtr expectedJoin1Ptr =
      expectedJoin1Builder
          .addNode([&join1SrcVec](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, join1SrcVec);
          })
          .planNode();
  VeloxPlanNodeVec join2SrcVec{expectedJoin1Ptr, expectedRight2Ptr};
  VeloxPlanBuilder expectedJoin2Builder;
  VeloxPlanNodePtr expectedJoin2Ptr =
      expectedJoin2Builder
          .addNode([&join2SrcVec](std::string id,
                                  std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, join2SrcVec);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedJoin2Ptr));
}
}  // namespace facebook::velox::plugin::plantransformer::test
