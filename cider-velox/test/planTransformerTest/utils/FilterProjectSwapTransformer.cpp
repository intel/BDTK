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

#include "FilterProjectSwapTransformer.h"

#include "planTransformer/PlanUtil.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;

StatePtr ProjectFilterStateMachine::Initial::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto projectNode = std::dynamic_pointer_cast<const ProjectNode>(nodePtr)) {
    return std::make_shared<ProjectFilterStateMachine::Project>();
  } else {
    return std::make_shared<ProjectFilterStateMachine::NotAccept>();
  }
}

StatePtr ProjectFilterStateMachine::Project::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(nodePtr)) {
    return std::make_shared<ProjectFilterStateMachine::Filter>();
  } else {
    return std::make_shared<ProjectFilterStateMachine::NotAccept>();
  }
}

bool ProjectFilterStateMachine::accept(VeloxPlanNodeAddr nodeAddr) {
  StatePtr curState = getCurState();
  if (curState != nullptr) {
    curState = curState->accept(nodeAddr);
    setCurState(curState);
    if (auto notAcceptState = std::dynamic_pointer_cast<NotAccept>(curState)) {
      return false;
    } else {
      addToMatchResult(nodeAddr);
      return true;
    }
  } else {
    return false;
  }
}

std::pair<bool, VeloxPlanNodePtr>
ProjcetFilterSwapRewriter::rewritePlanSectionWithSingleSource(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddr& source) const {
  VeloxPlanNodeAddrList nodeList = PlanUtil::getPlanNodeListForPlanSection(planSection);
  if (nodeList.empty() || nodeList.size() != 2) {
    // don't change the plan
    return std::pair<bool, VeloxPlanNodePtr>(true, planSection.target.nodePtr);
  } else {
    VeloxPlanNodeAddr filterNode = planSection.target;
    VeloxPlanNodeAddr projectNode = planSection.source;
    VeloxPlanNodePtr newFilterPtr =
        PlanUtil::cloneNodeWithNewSource(filterNode.nodePtr, source.nodePtr);
    VeloxPlanNodePtr newProjectPtr =
        PlanUtil::cloneNodeWithNewSource(projectNode.nodePtr, newFilterPtr);
    return std::pair<bool, VeloxPlanNodePtr>(true, newProjectPtr);
  }
}

std::pair<bool, VeloxPlanNodePtr>
ProjcetFilterSwapRewriter::rewritePlanSectionWithMultiSources(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddrList& srcList) const {
  return std::pair<bool, VeloxPlanNodePtr>(false, nullptr);
}

std::pair<bool, VeloxPlanNodePtr>
ProjcetFilterDeleteRewriter::rewritePlanSectionWithSingleSource(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddr& source) const {
  return std::pair<bool, VeloxPlanNodePtr>(true, source.nodePtr);
}

std::pair<bool, VeloxPlanNodePtr>
ProjcetFilterDeleteRewriter::rewritePlanSectionWithMultiSources(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddrList& srcList) const {
  return std::pair<bool, VeloxPlanNodePtr>(false, nullptr);
}

}  // namespace facebook::velox::plugin::plantransformer::test
