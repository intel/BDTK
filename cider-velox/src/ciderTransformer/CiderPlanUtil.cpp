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

#include "CiderPlanUtil.h"
#include <memory>
#include "planTransformer/PlanBranches.h"
#include "planTransformer/PlanUtil.h"
#include "substrait/plan.pb.h"

namespace facebook::velox::plugin::plantransformer {
std::shared_ptr<CiderPlanNode> CiderPlanUtil::toCiderPlanNode(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddr& source) {
  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanFragmentConvertor_ =
      std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          planSection.target.nodePtr, planSection.source.nodePtr));
  return std::make_shared<CiderPlanNode>(planSection.target.nodePtr->id(),
                                         source.nodePtr,
                                         planSection.target.nodePtr->outputType(),
                                         *sPlan);
}

std::shared_ptr<CiderPlanNode> CiderPlanUtil::toCiderPlanNode(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddrList& srcList) {
  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanFragmentConvertor_ =
      std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          planSection.target.nodePtr, planSection.source.nodePtr));

  int32_t srcBranchId = planSection.source.branchId;
  int32_t leftSrcBranchId = PlanBranches::getLeftSrcBranchId(srcBranchId);
  int32_t rightSrcBranchId = PlanBranches::getRightSrcBranchId(srcBranchId);
  NodeAddrMapPtr newSrcMap = PlanUtil::toNodeAddrMap(srcList);
  std::pair<bool, VeloxPlanNodePtr> newLeftSrc = PlanUtil::findInNodeAddrMap(newSrcMap, leftSrcBranchId, 0);
  std::pair<bool, VeloxPlanNodePtr> newRightSrc = PlanUtil::findInNodeAddrMap(newSrcMap, rightSrcBranchId, 0);
  if (newLeftSrc.first && newRightSrc.first) {
    std::shared_ptr<CiderPlanNode> ciderPlanNode =
        std::make_shared<CiderPlanNode>(planSection.target.nodePtr->id(),
                                        newLeftSrc.second,
                                        newRightSrc.second,
                                        planSection.target.nodePtr->outputType(),
                                        *sPlan);
    return ciderPlanNode;
  } else {
    VELOX_FAIL(
        "Can't find source nodes in the source node list of multi source planSection.");
  }
}

}  // namespace facebook::velox::plugin::plantransformer
