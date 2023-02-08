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

#include "PlanTranformerIncludes.h"
#include "utils/InvalidPlanPatterns.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class NotStartFromMatchPointTest : public PlanTransformerTestBase {
 public:
  NotStartFromMatchPointTest() {
    auto transformerFactory = PlanTransformerFactory().registerPattern(
        std::make_shared<NotStartFromMatchPointPlanPattern>(),
        std::make_shared<KeepOrginalRewriter>());
    setTransformerFactory(transformerFactory);
  }
}

TEST_F(NotStartFromMatchPointTest, notStartFromMatchPoint) {
  VeloxPlanBuilder transformPlanBuilder;
  VeloxPlanNodePtr planPtr = transformPlanBuilder.filter().proj().partialAgg().planNode();
  EXPECT_THROW(getTransformer(planPtr)->transform(), std::runtime_error);
}

}  // namespace facebook::velox::plugin::plantransformer::test
