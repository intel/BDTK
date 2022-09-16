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
class CiderCompoundPatternTest : public PlanTransformerTestBase {
 public:
  CiderCompoundPatternTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<plantransformer::CompoundPattern>(),
        std::make_shared<CiderPatternTestNodeRewriter>());
    setTransformerFactory(transformerFactory);
  }
};

TEST_F(CiderCompoundPatternTest, filterProjectAgg) {
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

TEST_F(CiderCompoundPatternTest, projectAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.proj().partialAgg().planNode();
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

TEST_F(CiderCompoundPatternTest, filterProject) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().planNode();
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

TEST_F(CiderCompoundPatternTest, projectFilter) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.proj().filter().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .filter()
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(CiderCompoundPatternTest, FilterAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(CiderCompoundPatternTest, singleProject) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.proj().planNode();
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

TEST_F(CiderCompoundPatternTest, SingleBranchMultiCompoundNodes) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr =
      transformPlanBuilder.filter().proj().filter().proj().partialAgg().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder
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
          .hashjoin(expectedRightPtr)
          .addNode([](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<TestCiderPlanNode>(id, input);
          })
          .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedLeftPtr));
}

}  // namespace facebook::velox::plugin::plantransformer::test
