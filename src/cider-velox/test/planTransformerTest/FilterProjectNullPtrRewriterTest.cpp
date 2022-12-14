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
using namespace facebook::velox::core;
class FilterProjectNullptrRewriterTest : public PlanTransformerTestBase {
 public:
  FilterProjectNullptrRewriterTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<ProjectFilterPattern>(), std::make_shared<NullptrRewriter>());
    setTransformerFactory(transformerFactory);
  }
}

TEST_F(FilterProjectNullptrRewriterTest, noNeedToChange) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, planPtr));
}

TEST_F(FilterProjectNullptrRewriterTest, singelBranch) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr =
      transformPlanBuilder.proj().filter().proj().filter().partialAgg().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr = expectedPlanBuilder.partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(FilterProjectNullptrRewriterTest, singelBranch2) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr =
      transformPlanBuilder.proj().proj().filter().proj().filter().planNode();
  VeloxPlanBuilder expectedPlanBuilder;
  VeloxPlanNodePtr expectedPtr = expectedPlanBuilder.proj().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedPtr));
}

TEST_F(FilterProjectNullptrRewriterTest, MultiBranches) {
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
  VeloxPlanNodePtr expectedRightPtr = expectedRightBranchBuilder.proj().planNode();
  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr =
      expectedLeftBranchBuilder.hashjoin(expectedRightPtr).partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planLeftPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, expectedLeftPtr));
}

}  // namespace facebook::velox::plugin::plantransformer::test
