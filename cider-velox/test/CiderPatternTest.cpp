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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "ciderTransformer/CiderPlanTransformerFactory.h"
#include "planTransformerTest/utils/PlanTansformerTestUtil.h"

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;

using namespace facebook::velox::plugin::plantransformer;
using namespace facebook::velox::plugin::plantransformer::test;

class CiderPatternTest : public OperatorTestBase {
  void SetUp() override {
    FLAGS_PartialAggPattern = true;
    CiderVeloxPluginCtx::init();
  }
};

TEST_F(CiderPatternTest, partialAggPattern) {
  auto data = makeRowVector({makeFlatVector<int64_t>(10, [](auto row) { return row; })});
  createDuckDbTable({data});
  auto veloxPlan =
      PlanBuilder().values({data}).partialAggregation({}, {"sum(c0)"}, {}).planNode();

  auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  auto duckdbSql = "SELECT sum(c0) from tmp ";

  assertQuery(veloxPlan, duckdbSql);
  assertQuery(resultPtr, duckdbSql);

  const ::substrait::Plan substraitPlan = ::substrait::Plan();
  auto expectedPlan =
      PlanBuilder()
          .values({data})
          .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
                CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
          })
          .planNode();
  EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
}

TEST_F(CiderPatternTest, FilterPattern) {
  auto data = makeRowVector({makeFlatVector<int64_t>(10, [](auto row) { return row; })});
  createDuckDbTable({data});
  auto veloxPlan = PlanBuilder().values({data}).filter("c0 > 5").planNode();

  auto resultPtr = CiderVeloxPluginCtx::transformVeloxPlan(veloxPlan);
  auto duckdbSql = "SELECT * FROM tmp WHERE c0 > 5";

  assertQuery(veloxPlan, duckdbSql);
  assertQuery(resultPtr, duckdbSql);

  const ::substrait::Plan substraitPlan = ::substrait::Plan();
  auto expectedPlan =
      PlanBuilder()
          .values({data})
          .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<facebook::velox::plugin::CiderPlanNode>(
                CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
          })
          .planNode();
  EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(resultPtr, expectedPlan));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
