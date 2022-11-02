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

#include "PlanUtil.h"

#include "PlanBranches.h"

namespace facebook::velox::plugin::plantransformer {
using namespace facebook::velox::core;
bool PlanUtil::isJoin(VeloxPlanNodePtr node) {
  if (auto hashJoinNode = std::dynamic_pointer_cast<const AbstractJoinNode>(node)) {
    return true;
  } else if (auto crossJoinNode = std::dynamic_pointer_cast<const CrossJoinNode>(node)) {
    return true;
  }
  return false;
}

void PlanUtil::changeNodeSource(VeloxPlanNodePtr node, VeloxPlanNodePtr source) {
  std::vector<std::shared_ptr<const PlanNode>>& nodeSources =
      const_cast<std::vector<std::shared_ptr<const PlanNode>>&>(node->sources());
  nodeSources = {source};
}

void PlanUtil::changeJoinNodeSource(VeloxPlanNodePtr node,
                                    VeloxPlanNodePtr left,
                                    VeloxPlanNodePtr right) {
  std::vector<std::shared_ptr<const PlanNode>>& nodeSources =
      const_cast<std::vector<std::shared_ptr<const PlanNode>>&>(node->sources());
  nodeSources = {left, right};
}

void PlanUtil::changeJoinNodeLeftSource(VeloxPlanNodePtr node, VeloxPlanNodePtr left) {
  std::vector<std::shared_ptr<const PlanNode>>& nodeSources =
      const_cast<std::vector<std::shared_ptr<const PlanNode>>&>(node->sources());
  nodeSources = {left, nodeSources[1]};
}

void PlanUtil::changeJoinNodeRightSource(VeloxPlanNodePtr node, VeloxPlanNodePtr right) {
  std::vector<std::shared_ptr<const PlanNode>>& nodeSources =
      const_cast<std::vector<std::shared_ptr<const PlanNode>>&>(node->sources());
  nodeSources = {nodeSources[0], right};
}

void PlanUtil::changeSingleSourcePlanSectionSource(
    const VeloxNodeAddrPlanSection& planSection,
    const VeloxPlanNodeAddr& source) {
  PlanUtil::changeNodeSource(planSection.source.nodePtr, source.nodePtr);
}

void PlanUtil::changeMultiSourcePlanSectionSources(
    const VeloxNodeAddrPlanSection& planSection,
    const VeloxPlanNodeAddrList& sourceList) {
  NodeAddrMapPtr newSrcPtrMap = toNodeAddrMap(sourceList);
  PlanBranches planBranches{planSection.target.root};
  VeloxPlanNodeAddrList sources = planBranches.getAllSourcesOf(planSection);
  for (VeloxPlanNodeAddr oldSrc : sources) {
    VeloxPlanNodeAddr target = planBranches.moveToTarget(oldSrc);
    const auto& [found, sourceNode] =
        findInNodeAddrMap(newSrcPtrMap, oldSrc.branchId, oldSrc.nodeId);
    if (found) {
      if (target.branchId == oldSrc.branchId) {  // target is not a join
        changeNodeSource(target.nodePtr, sourceNode);
      } else {
        if (PlanBranches::getLeftSrcBranchId(target.branchId) == oldSrc.branchId) {
          changeJoinNodeLeftSource(target.nodePtr, sourceNode);
        } else {
          changeJoinNodeRightSource(target.nodePtr, sourceNode);
        }
      }
    }
  }
}

VeloxPlanNodeAddrList PlanUtil::getPlanNodeListForPlanSection(
    const VeloxNodeAddrPlanSection& planSection) {
  PlanBranches planBranches{planSection.target.root};
  BranchSrcToTargetIterator nodeIte =
      planBranches.getPlanSectionSrcToTargetIterator(planSection);
  VeloxPlanNodeAddrList planSectonList;
  while (nodeIte.hasNext()) {
    planSectonList.emplace_back(nodeIte.next());
  }
  return planSectonList;
}

NodeAddrMapPtr PlanUtil::toNodeAddrMap(const VeloxPlanNodeAddrList& nodeAddrList) {
  NodeAddrMap sourcePtrMap;
  for (const auto& sourceAddr : nodeAddrList) {
    std::pair<int32_t, int32_t> nodeAddr{};
    nodeAddr.first = sourceAddr.branchId;
    nodeAddr.second = sourceAddr.nodeId;
    sourcePtrMap[nodeAddr] = sourceAddr.nodePtr;
  }
  return std::make_shared<std::map<std::pair<int32_t, int32_t>, VeloxPlanNodePtr>>(
      sourcePtrMap);
}

std::pair<bool, VeloxPlanNodePtr> PlanUtil::findInNodeAddrMap(NodeAddrMapPtr map,
                                                              int32_t branchId,
                                                              int32_t nodeId) {
  std::pair<int32_t, int32_t> nodeAddr{};
  nodeAddr.first = branchId;
  nodeAddr.second = nodeId;
  auto foundPos = map->find(nodeAddr);
  VeloxPlanNodePtr foundSrcPtr = nullptr;
  bool found = false;
  if (foundPos != map->end()) {
    found = true;
    foundSrcPtr = foundPos->second;
  }
  return std::pair<bool, VeloxPlanNodePtr>(found, foundSrcPtr);
}

}  // namespace facebook::velox::plugin::plantransformer
