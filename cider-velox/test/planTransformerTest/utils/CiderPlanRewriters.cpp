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

#include "CiderPlanRewriters.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;

const std::vector<std::shared_ptr<const PlanNode>>& TestCiderPlanNode::sources() const {
  return sources_;
}

std::string_view TestCiderPlanNode::name() const {
  return "TestCider";
}

const RowTypePtr& TestCiderPlanNode::outputType() const {
  return sources_[0]->outputType();
}
void TestCiderPlanNode::addDetails(std::stringstream& stream) const {}

std::pair<bool, VeloxPlanNodePtr>
CiderPatternTestNodeRewriter::rewritePlanSectionWithMultiSources(
    facebook::velox::plugin::plantransformer::VeloxNodeAddrPlanSection& planSection,
    facebook::velox::plugin::plantransformer::VeloxPlanNodeAddrList& srcList) const {
  VeloxPlanNodeVec srcPtrList;
  for (VeloxPlanNodeAddr addr : srcList) {
    srcPtrList.emplace_back(addr.nodePtr);
  }
  auto resultPtr = std::make_shared<TestCiderPlanNode>("CiderJoin", srcPtrList);
  return std::pair<bool, VeloxPlanNodePtr>(true, resultPtr);
}

std::pair<bool, VeloxPlanNodePtr>
CiderPatternTestNodeRewriter::rewritePlanSectionWithSingleSource(
    facebook::velox::plugin::plantransformer::VeloxNodeAddrPlanSection& planSection,
    facebook::velox::plugin::plantransformer::VeloxPlanNodeAddr& source) const {
  VeloxPlanNodePtr testNodePtr =
      std::make_shared<TestCiderPlanNode>("CiderTest", source.nodePtr);
  return std::pair<bool, VeloxPlanNodePtr>(true, testNodePtr);
}

}  // namespace facebook::velox::plugin::plantransformer::test
