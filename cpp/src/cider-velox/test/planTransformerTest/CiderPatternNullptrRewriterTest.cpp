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

 protected:
  RowTypePtr rowType_{ROW({"c0", "c1"}, {BIGINT(), INTEGER()})};
  RowTypePtr rowTypeLeft_{ROW({"c2", "c3"}, {BIGINT(), INTEGER()})};
};

TEST_F(CiderPatternNullptrRewriterTest, filterProjectAgg) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = PlanBuilder()
                                 .values(generateTestBatch(rowType_, false))
                                 .filter("c0 > 2 ")
                                 .project({"c0", "c1", "c0 + 2"})
                                 .partialAggregation({}, {"SUM(c1)"})
                                 .planNode();
  VeloxPlanNodePtr resultPtr = getTransformer(planPtr)->transform();
  EXPECT_TRUE(
      compareWithExpected(resultPtr, planPtr->sources()[0]->sources()[0]->sources()[0]));
}

TEST_F(CiderPatternNullptrRewriterTest, SingleJoinMultiCompoundNodes) {
  VeloxPlanNodePtr planRightPtr = getSingleFilterNode(rowType_, "c0 > 1");

  VeloxPlanNodePtr planLeftPtr =
      PlanBuilder()
          .values(generateTestBatch(rowTypeLeft_, false))
          .filter("c2 > 3")
          .hashJoin({"c2"}, {"c0"}, planRightPtr, "", {"c2", "c3", "c1"})
          .planNode();

  VeloxPlanNodePtr expectedLeftPtr = getSingleFilterNode(rowTypeLeft_, "c2 > 3");

  VeloxPlanNodeVec joinSrcVec{expectedLeftPtr, planRightPtr};

  VeloxPlanNodePtr expectedJoinPtr = getCiderExpectedPtr(rowTypeLeft_, joinSrcVec);

  EXPECT_THROW(getTransformer(planLeftPtr)->transform(),
               facebook::velox::VeloxRuntimeError);
}

}  // namespace facebook::velox::plugin::plantransformer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
