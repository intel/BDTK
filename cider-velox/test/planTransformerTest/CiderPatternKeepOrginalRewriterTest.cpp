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
class CiderPatternKeepOrginalRewriterTest : public PlanTransformerTestBase {
 public:
  CiderPatternKeepOrginalRewriterTest() {
    auto transformerFactory =
        PlanTransformerFactory()
            .registerPattern(std::make_shared<plantransformer::CompoundPattern>(),
                             std::make_shared<KeepOrginalRewriter>())
            .registerPattern(std::make_shared<plantransformer::LeftDeepJoinPattern>(),
                             std::make_shared<KeepOrginalRewriter>());
    setTransformerFactory(transformerFactory);
  }
}

TEST_F(CiderPatternKeepOrginalRewriterTest, filterProjectAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(CiderPatternKeepOrginalRewriterTest, SingleJoinMultiCompoundNodes) {
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
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planLeftPtr));
}

TEST_F(CiderPatternKeepOrginalRewriterTest, MultiJoinNodes) {
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
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planLeftPtr));
}

TEST_F(CiderPatternKeepOrginalRewriterTest, MultiSeperatedJoinNodes) {
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
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planLeftPtr));
}

TEST_F(CiderPatternKeepOrginalRewriterTest, EndWithSingleJoinNode) {
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
                                     .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planLeftPtr));
}
}  // namespace facebook::velox::plugin::plantransformer::test
