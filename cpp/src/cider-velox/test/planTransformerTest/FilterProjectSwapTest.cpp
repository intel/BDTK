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

#include "PlanTranformerIncludes.h"
#include "utils/FilterProjectSwapTransformer.h"

namespace facebook::velox::plugin::plantransformer::test {
class FilterProjectSwapTest : public PlanTransformerTestBase {
 public:
  FilterProjectSwapTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<ProjectFilterPattern>(),
        std::make_shared<ProjcetFilterSwapRewriter>());
    setTransformerFactory(transformerFactory);
  }
};

TEST_F(FilterProjectSwapTest, noNeedToSwap) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(FilterProjectSwapTest, singelBranchSwap) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr =
      transformPlanBuilder.proj().filter().proj().filter().partialAgg().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder.filter().proj().filter().proj().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(FilterProjectSwapTest, singelBranchSwap2) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr =
      transformPlanBuilder.proj().proj().filter().proj().filter().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr =
      expectedPlanBuilder.proj().filter().proj().filter().proj().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(FilterProjectSwapTest, MultiBranchesSwap) {
  VeloxPlanBuilder planRightBranchBuilder;
  VeloxPlanNodePtr planRightPtr =
      planRightBranchBuilder.proj().proj().filter().proj().filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr = planLeftBranchBuilder.proj()
                                     .filter()
                                     .hashjoin(planRightPtr)
                                     .proj()
                                     .filter()
                                     .partialAgg()
                                     .planNode();
  VeloxPlanBuilder expectedRightBranchBuilder;
  VeloxPlanNodePtr expectedRightPtr =
      expectedRightBranchBuilder.proj().filter().proj().filter().proj().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter()
                                         .proj()
                                         .hashjoin(expectedRightPtr)
                                         .filter()
                                         .proj()
                                         .partialAgg()
                                         .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedLeftPtr));
}

}  // namespace facebook::velox::plugin::plantransformer::test
