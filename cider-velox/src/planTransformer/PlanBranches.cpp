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

#include "PlanBranches.h"

namespace facebook::velox::plugin::plantransformer {

PlanBranches::PlanBranches(VeloxPlanNodePtr root) {
  root_ = root;
  splitPlanBranches(root);
}

void PlanBranches::splitPlanBranches(VeloxPlanNodePtr rootNode) {
  splitPlanBranches(rootNode, 1);
  std::reverse(srcToRootBranchIds_.begin(), srcToRootBranchIds_.end());
}

void PlanBranches::splitPlanBranches(VeloxPlanNodePtr branchRootNode, int32_t branchIdx) {
  VeloxPlanNodeVec curBranch = makePlanBranch(branchRootNode);
  size_t curBranchSize = curBranch.size();
  if (curBranchSize > 0) {
    srcToRootBranchIds_.emplace_back(branchIdx);
    branchMap_[branchIdx] = curBranch;
    VeloxPlanNodePtr branchSourceNode = curBranch[curBranchSize - 1];
    auto branchSrcs = branchSourceNode->sources();
    if (branchSrcs.empty() || branchSrcs.size() < 2) {
      srcBranchIds_.push_back(branchIdx);
    } else if (branchSrcs.size() == 2) {
      splitPlanBranches(branchSrcs[0], getLeftSrcBranchId(branchIdx));
      splitPlanBranches(branchSrcs[1], getRightSrcBranchId(branchIdx));
    }
  }
}

VeloxPlanBranch PlanBranches::makePlanBranch(VeloxPlanNodePtr root) {
  VeloxPlanBranch branch;
  if (root != nullptr) {
    VeloxPlanNodePtr curNode = root;
    while (curNode != nullptr) {
      branch.push_back(curNode);
      auto curNodeSrcs = curNode->sources();
      if (curNodeSrcs.size() == 1) {
        curNode = curNodeSrcs[0];
      } else {
        curNode = nullptr;
      }
    }
  }
  return branch;
}

VeloxPlanBranch PlanBranches::getBranch(int32_t branchId) {
  return branchMap_[branchId];
}

int32_t PlanBranches::getParentBranchId(int32_t branchId) {
  // invalid branchId
  if (branchId < 1) {
    return -1;
  } else if (branchId == 1) {  // It's the root branch, no parent branch.
    return -1;
  } else {
    return branchId / 2;
  }
}

int32_t PlanBranches::getLeftSrcBranchId(int32_t branchId) {
  // invalid branchId
  if (branchId < 1) {
    return -1;
  } else {
    return branchId * 2;
  }
}

int32_t PlanBranches::getRightSrcBranchId(int32_t branchId) {
  // invalid branchId
  if (branchId < 1) {
    return -1;
  } else {
    return branchId * 2 + 1;
  }
}

VeloxPlanNodeAddr PlanBranches::getPlanNodeAddr(int32_t branchId, int32_t nodeId) {
  VeloxPlanBranch branch = getBranch(branchId);
  return VeloxPlanNodeAddr{root_, branchId, nodeId, branch[nodeId]};
}

VeloxPlanNodeAddr PlanBranches::getBranchSrcNodeAddr(int32_t branchId) {
  VeloxPlanBranch branch = getBranch(branchId);
  int32_t nodeId = branch.size() - 1;
  return VeloxPlanNodeAddr{root_, branchId, nodeId, branch[nodeId]};
}

VeloxPlanNodeAddr PlanBranches::moveToTarget(VeloxPlanNodeAddr& nodeAddr) {
  if (!VeloxPlanNodeAddr::invalid().equal(nodeAddr) && root_ != nodeAddr.nodePtr) {
    VeloxPlanNodeAddr curNodeAddr = nodeAddr;
    VeloxPlanNodePtr root = curNodeAddr.root;
    int32_t branchId = curNodeAddr.branchId;
    VeloxPlanBranch curBranch = getBranch(branchId);
    int32_t prevNodeId = curNodeAddr.nodeId - 1;
    VeloxPlanNodePtr prevNodePtr = nullptr;
    // set nextPlanNodeId to default value which means not valid.
    VeloxPlanNodeAddr prevPlanNodeAddr;
    // Need navigate to parent branch
    if (prevNodeId < 0) {
      int32_t parentBranchId = getParentBranchId(curNodeAddr.branchId);
      // if there is a parent branch
      if (parentBranchId > 0) {
        VeloxPlanBranch parentBranch = getBranch(parentBranchId);
        if (parentBranch.size() > 0) {
          prevNodeId = parentBranch.size() - 1;
          prevNodePtr = parentBranch[prevNodeId];
          prevPlanNodeAddr = {root, parentBranchId, prevNodeId, prevNodePtr};
        } else {
          // there is no parent branch
          return VeloxPlanNodeAddr::invalid();
        }
      }
    } else {
      prevPlanNodeAddr = {root, curNodeAddr.branchId, prevNodeId, curBranch[prevNodeId]};
    }
    return prevPlanNodeAddr;
  } else {
    return VeloxPlanNodeAddr::invalid();
  }
}

VeloxPlanNodeAddrList PlanBranches::getAllSourcesOf(
    VeloxNodeAddrPlanSection& planSection) {
  BranchSrcToTargetIterator planSectionIte =
      getPlanSectionSrcToTargetIterator(planSection);
  VeloxPlanNodeAddrList sourcesList{};
  VeloxPlanNodeAddr prevNode = VeloxPlanNodeAddr::invalid();
  VeloxPlanNodeAddr curNode = VeloxPlanNodeAddr::invalid();
  while (planSectionIte.hasNext()) {
    prevNode = curNode;
    curNode = planSectionIte.next();
    VeloxPlanNodeVec sources = curNode.nodePtr->sources();
    if (curNode.equal(planSection.source)) {
      if (sources.size() == 1 && sources[0] != nullptr) {
        VeloxPlanNodeAddr srcAddr{
            root_, curNode.branchId, curNode.nodeId + 1, sources[0]};
        sourcesList.emplace_back(srcAddr);
      } else if (sources.size() == 2) {
        int32_t left = getLeftSrcBranchId(curNode.branchId);
        int32_t right = getRightSrcBranchId(curNode.branchId);
        VeloxPlanNodeAddr leftAddr{root_, left, 0, sources[0]};
        VeloxPlanNodeAddr rightAddr{root_, right, 0, sources[1]};
        sourcesList.emplace_back(leftAddr);
        sourcesList.emplace_back(rightAddr);
      }
    } else if (sources.size() > 1) {
      for (VeloxPlanNodePtr nodePtr : sources) {
        if (nodePtr != prevNode.nodePtr) {
          int32_t branchId = -1;
          if (prevNode.branchId % 2 == 0) {
            branchId = prevNode.branchId + 1;
          } else {
            branchId = prevNode.branchId - 1;
          }
          sourcesList.emplace_back(VeloxPlanNodeAddr{root_, branchId, 0, nodePtr});
        }
      }
    }
  }
  return sourcesList;
}

BranchSrcToTargetIterator PlanBranches::getBranchSrcToTargetIterator(int32_t branchId) {
  return BranchSrcToTargetIterator(*this, branchId);
}

BranchSrcToTargetIterator PlanBranches::getPlanSectionSrcToTargetIterator(
    VeloxNodeAddrPlanSection& planSection) {
  return BranchSrcToTargetIterator(*this, planSection);
}

BranchSrcToTargetIterator::BranchSrcToTargetIterator(PlanBranches& branches,
                                                     int32_t branchId) {
  planBranches_ = std::make_shared<PlanBranches>(branches);
  branchId_ = branchId;
  curPlanNodeId_ = planBranches_->getBranchSrcNodeAddr(branchId);
  planSection_ = nullptr;
}

BranchSrcToTargetIterator::BranchSrcToTargetIterator(
    PlanBranches& branches,
    VeloxNodeAddrPlanSection& planSection) {
  planBranches_ = std::make_shared<PlanBranches>(branches);
  branchId_ = -1;
  curPlanNodeId_ = planSection.source;
  planSection_ = std::make_shared<VeloxNodeAddrPlanSection>(planSection);
}

bool BranchSrcToTargetIterator::hasNext() {
  if (firstMove_) {
    return true;
  }
  if (planSection_ == nullptr) {
    if (curPlanNodeId_.nodePtr != planBranches_->getRoot()) {
      return true;
    }
  } else {
    VeloxPlanNodeAddr target = planSection_->target;
    if (curPlanNodeId_.nodePtr != target.nodePtr) {
      return true;
    }
  }
  return false;
}

VeloxPlanNodeAddr BranchSrcToTargetIterator::next() {
  if (!firstMove_) {
    if (planSection_ == nullptr) {
      if (curPlanNodeId_.nodePtr != planBranches_->getRoot()) {
        curPlanNodeId_ = planBranches_->moveToTarget(curPlanNodeId_);
      }
    } else {
      VeloxPlanNodeAddr target = planSection_->target;
      if (curPlanNodeId_.nodePtr != target.nodePtr) {
        curPlanNodeId_ = planBranches_->moveToTarget(curPlanNodeId_);
      }
    }
  } else {
    firstMove_ = false;
  }

  return curPlanNodeId_;
}

VeloxPlanNodeAddr BranchSrcToTargetIterator::getCurPos() {
  return curPlanNodeId_;
}

void BranchSrcToTargetIterator::setCurPos(VeloxPlanNodeAddr& curPos) {
  curPlanNodeId_ = curPos;
}

}  // namespace facebook::velox::plugin::plantransformer
