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
class CiderPatternNullptrRewriterTest : public PlanTransformerTestBase {
 public:
  CiderPatternNullptrRewriterTest() {
    auto transformerFactory =
        PlanTransformerFactory()
            .registerPattern(std::make_shared<plantransformer::CompoundPattern>(),
                             std::make_shared<NullptrRewriter>())
            .registerPattern(std::make_shared<plantransformer::LeftDeepJoinPattern>(),
                             std::make_shared<NullptrRewriter>());
    setTransformerFactory(transformerFactory);
  }
};

TEST_F(CiderPatternNullptrRewriterTest, filterProjectAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(compareWithExpected(resultPtr, nullptr));
}

TEST_F(CiderPatternNullptrRewriterTest, SingleJoinMultiCompoundNodes) {
  VeloxPlanBuilder planRightBranchBuilder;
  VeloxPlanNodePtr planRightPtr = planRightBranchBuilder.filter().planNode();
  VeloxPlanBuilder planLeftBranchBuilder;
  VeloxPlanNodePtr planLeftPtr =
      planLeftBranchBuilder.filter().hashjoin(planRightPtr).planNode();

  VeloxPlanBuilder expectedLeftBranchBuilder;
  VeloxPlanNodePtr expectedLeftPtr = expectedLeftBranchBuilder.filter().planNode();
  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRightPtr};
  VeloxPlanBuilder expectedJoinBuilder;
  VeloxPlanNodePtr expectedJoinPtr =
      expectedJoinBuilder
          .addNode(
              [&joinSrcVec](std::string id, std::shared_ptr<const core::PlanNode> input) {
                return std::make_shared<TestCiderPlanNode>(id, joinSrcVec);
              })
          .planNode();
  EXPECT_THROW(getTransformer(planLeftPtr)->transform(), std::runtime_error);
}
}  // namespace facebook::velox::plugin::plantransformer::test
