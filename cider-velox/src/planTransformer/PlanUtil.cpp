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

VeloxPlanNodePtr PlanUtil::cloneJoinNodeWithNewSources(VeloxPlanNodePtr node,
                                                       VeloxPlanNodePtr left,
                                                       VeloxPlanNodePtr right) {
  if (auto hashJoinNode = std::dynamic_pointer_cast<const HashJoinNode>(node)) {
    return std::make_shared<const HashJoinNode>(hashJoinNode->id(),
                                                hashJoinNode->joinType(),
                                                hashJoinNode->leftKeys(),
                                                hashJoinNode->rightKeys(),
                                                hashJoinNode->filter(),
                                                left,
                                                right,
                                                hashJoinNode->outputType());
  } else if (auto mergeJoinNode = std::dynamic_pointer_cast<const MergeJoinNode>(node)) {
    return std::make_shared<const MergeJoinNode>(mergeJoinNode->id(),
                                                 mergeJoinNode->joinType(),
                                                 mergeJoinNode->leftKeys(),
                                                 mergeJoinNode->rightKeys(),
                                                 mergeJoinNode->filter(),
                                                 left,
                                                 right,
                                                 mergeJoinNode->outputType());
  } else if (auto crossJoinNode = std::dynamic_pointer_cast<const CrossJoinNode>(node)) {
    return std::make_shared<const CrossJoinNode>(
        crossJoinNode->id(), left, right, crossJoinNode->outputType());
  } else {
    throw std::runtime_error(
        "Clone velox plannode:Not supported velox Join plannode type.");
  }
  return nullptr;
}

VeloxPlanNodePtr PlanUtil::cloneNodeWithNewSource(VeloxPlanNodePtr node,
                                                  VeloxPlanNodePtr source) {
  if (auto partitionOutputNode =
          std::dynamic_pointer_cast<const PartitionedOutputNode>(node)) {
    return std::make_shared<const PartitionedOutputNode>(
        partitionOutputNode->id(),
        partitionOutputNode->keys(),
        partitionOutputNode->numPartitions(),
        partitionOutputNode->isBroadcast(),
        partitionOutputNode->isReplicateNullsAndAny(),
        partitionOutputNode->partitionFunctionFactory(),
        partitionOutputNode->outputType(),
        source);
  } else if (auto lPartitionNode =
                 std::dynamic_pointer_cast<const LocalPartitionNode>(node)) {
    std::vector<PlanNodePtr> sources = {source};
    return std::make_shared<const LocalPartitionNode>(
        lPartitionNode->id(),
        lPartitionNode->type(),
        lPartitionNode->partitionFunctionFactory(),
        lPartitionNode->outputType(),
        sources,
        lPartitionNode->inputTypeFromSource());
  } else if (auto aggNode = std::dynamic_pointer_cast<const AggregationNode>(node)) {
    return std::make_shared<const AggregationNode>(aggNode->id(),
                                                   aggNode->step(),
                                                   aggNode->groupingKeys(),
                                                   aggNode->preGroupedKeys(),
                                                   aggNode->aggregateNames(),
                                                   aggNode->aggregates(),
                                                   aggNode->aggregateMasks(),
                                                   aggNode->ignoreNullKeys(),
                                                   source);
  } else if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(node)) {
    return std::make_shared<const FilterNode>(
        filterNode->id(), filterNode->filter(), source);
  } else if (auto projectNode = std::dynamic_pointer_cast<const ProjectNode>(node)) {
    return std::make_shared<const ProjectNode>(
        projectNode->id(), projectNode->names(), projectNode->projections(), source);
  } else if (auto orderByNode = std::dynamic_pointer_cast<const OrderByNode>(node)) {
    return std::make_shared<const OrderByNode>(orderByNode->id(),
                                               orderByNode->sortingKeys(),
                                               orderByNode->sortingOrders(),
                                               orderByNode->isPartial(),
                                               source);
  } else if (auto topNNode = std::dynamic_pointer_cast<const TopNNode>(node)) {
    return std::make_shared<const TopNNode>(topNNode->id(),
                                            topNNode->sortingKeys(),
                                            topNNode->sortingOrders(),
                                            topNNode->count(),
                                            topNNode->isPartial(),
                                            source);
  } else if (auto limitNode = std::dynamic_pointer_cast<const LimitNode>(node)) {
    return std::make_shared<const LimitNode>(limitNode->id(),
                                             limitNode->offset(),
                                             limitNode->count(),
                                             limitNode->isPartial(),
                                             source);
  } else if (auto valuesNode = std::dynamic_pointer_cast<const ValuesNode>(node)) {
    return std::make_shared<ValuesNode>(valuesNode->id(), valuesNode->values());
  } else if (auto mergeExceNode =
                 std::dynamic_pointer_cast<const MergeExchangeNode>(node)) {
    return std::make_shared<MergeExchangeNode>(mergeExceNode->id(),
                                               mergeExceNode->outputType(),
                                               mergeExceNode->sortingKeys(),
                                               mergeExceNode->sortingOrders());
  } else if (auto exchangeNode = std::dynamic_pointer_cast<const ExchangeNode>(node)) {
    return std::make_shared<ExchangeNode>(exchangeNode->id(), exchangeNode->outputType());
  } else if (auto tableScanNode = std::dynamic_pointer_cast<const TableScanNode>(node)) {
    return std::make_shared<TableScanNode>(tableScanNode->id(),
                                           tableScanNode->outputType(),
                                           tableScanNode->tableHandle(),
                                           tableScanNode->assignments());
  } else if (auto assignUniqueIdNode =
                 std::dynamic_pointer_cast<const AssignUniqueIdNode>(node)) {
    return std::make_shared<AssignUniqueIdNode>(assignUniqueIdNode->id(),
                                                std::string{assignUniqueIdNode->name()},
                                                assignUniqueIdNode->taskUniqueId(),
                                                source);
  } else if (auto enforceSingleRowNode =
                 std::dynamic_pointer_cast<const EnforceSingleRowNode>(node)) {
    return std::make_shared<EnforceSingleRowNode>(enforceSingleRowNode->id(), source);
  } else {
    throw std::runtime_error("Clone velox plannode: Not supported velox plannode type[" +
                             std::string(typeid(node).name()) + "]");
  }
}

VeloxPlanNodeAddrList PlanUtil::getPlanNodeListForPlanSection(
    VeloxNodeAddrPlanSection& planSection) {
  PlanBranches planBranches{planSection.target.root};
  BranchSrcToTargetIterator nodeIte =
      planBranches.getPlanSectionSrcToTargetIterator(planSection);
  VeloxPlanNodeAddrList planSectonList;
  while (nodeIte.hasNext()) {
    planSectonList.emplace_back(nodeIte.next());
  }
  return planSectonList;
}

VeloxPlanNodePtr PlanUtil::cloneSingleSourcePlanSectionWithNewSource(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddr& source) {
  PlanBranches planBranches{planSection.target.root};
  BranchSrcToTargetIterator nodeIte =
      planBranches.getPlanSectionSrcToTargetIterator(planSection);
  VeloxPlanNodeAddrList planSectonList;
  VeloxPlanNodePtr curTargetPtr = source.nodePtr;
  while (nodeIte.hasNext()) {
    curTargetPtr = cloneNodeWithNewSource(nodeIte.next().nodePtr, curTargetPtr);
  }
  return curTargetPtr;
}

NodeAddrMapPtr PlanUtil::toNodeAddrMap(VeloxPlanNodeAddrList& nodeAddrList) {
  NodeAddrMap sourcePtrMap;
  for (auto sourceAddr : nodeAddrList) {
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

VeloxPlanNodePtr PlanUtil::cloneMultiSourcePlanSectionWithNewSources(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddrList& sourceList) {
  auto sourcePtrMap = toNodeAddrMap(sourceList);
  PlanBranches planBranches{planSection.target.root};
  BranchSrcToTargetIterator planSectionIte =
      planBranches.getPlanSectionSrcToTargetIterator(planSection);
  VeloxPlanNodePtr curTargetPtr = nullptr;
  VeloxPlanNodeAddr prevNode = VeloxPlanNodeAddr::invalid();
  VeloxPlanNodeAddr curNode = VeloxPlanNodeAddr::invalid();
  while (planSectionIte.hasNext()) {
    prevNode = curNode;
    curNode = planSectionIte.next();
    auto nodeSrcPtrs = curNode.nodePtr->sources();
    if (curNode.equal(planSection.source)) {
      if (nodeSrcPtrs.size() == 1 and nodeSrcPtrs[0] != nullptr) {
        auto foundInSrcNodeAddMap =
            findInNodeAddrMap(sourcePtrMap, curNode.branchId, curNode.nodeId + 1);
        if (foundInSrcNodeAddMap.first) {
          curTargetPtr =
              cloneNodeWithNewSource(curNode.nodePtr, foundInSrcNodeAddMap.second);
        } else {
          curTargetPtr = cloneNodeWithNewSource(curNode.nodePtr, nullptr);
        }
      } else if (nodeSrcPtrs.size() == 2) {
        int32_t left = planBranches.getLeftSrcBranchId(curNode.branchId);
        int32_t right = planBranches.getRightSrcBranchId(curNode.branchId);
        auto foundLeftInSrcNodeAddMap = findInNodeAddrMap(sourcePtrMap, left, 0);
        auto foundRightInSrcNodeAddMap = findInNodeAddrMap(sourcePtrMap, right, 0);
        VeloxPlanNodePtr leftSrcPtr =
            foundLeftInSrcNodeAddMap.first ? foundLeftInSrcNodeAddMap.second : nullptr;
        VeloxPlanNodePtr rightSrcPtr =
            foundRightInSrcNodeAddMap.first ? foundRightInSrcNodeAddMap.second : nullptr;
        curTargetPtr =
            cloneJoinNodeWithNewSources(curNode.nodePtr, leftSrcPtr, rightSrcPtr);
      }
    } else {
      if (nodeSrcPtrs.size() == 1) {
        curTargetPtr = cloneNodeWithNewSource(curNode.nodePtr, curTargetPtr);
      } else if (nodeSrcPtrs.size() == 2) {
        for (VeloxPlanNodePtr nodePtr : nodeSrcPtrs) {
          if (nodePtr != prevNode.nodePtr) {
            int32_t branchId = -1;
            if (prevNode.branchId % 2 == 0) {
              // right branch src
              branchId = prevNode.branchId + 1;
              auto foundRightInSrcNodeAddMap =
                  findInNodeAddrMap(sourcePtrMap, branchId, 0);
              VeloxPlanNodePtr rightSrcPtr = foundRightInSrcNodeAddMap.first
                                                 ? foundRightInSrcNodeAddMap.second
                                                 : nullptr;
              curTargetPtr =
                  cloneJoinNodeWithNewSources(curNode.nodePtr, curTargetPtr, rightSrcPtr);
            } else {
              // left branch src
              branchId = prevNode.branchId - 1;
              auto foundLeftInSrcNodeAddMap =
                  findInNodeAddrMap(sourcePtrMap, branchId, 0);
              VeloxPlanNodePtr leftSrcPtr = foundLeftInSrcNodeAddMap.first
                                                ? foundLeftInSrcNodeAddMap.second
                                                : nullptr;
              curTargetPtr =
                  cloneJoinNodeWithNewSources(curNode.nodePtr, leftSrcPtr, curTargetPtr);
            }
          }
        }
      }
    }
  }
  return curTargetPtr;
}

}  // namespace facebook::velox::plugin::plantransformer
