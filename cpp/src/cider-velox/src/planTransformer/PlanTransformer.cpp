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

#include "PlanTransformer.h"

namespace facebook::velox::plugin::plantransformer {

PlanTransformer::PlanTransformer(const PatternRewriterList& rewriterList,
                                 VeloxPlanNodePtr root) {
  rewriterList_ = rewriterList;
  root_ = root;
  PlanBranches branches(root);
  orgBranches_ = std::make_shared<PlanBranches>(branches);
}

VeloxPlanNodePtr PlanTransformer::transform() {
  matchFromSrcToTarget();
  if (hasMatchResult_) {
    return rewriteAllBranches();
  } else {
    return root_;
  }
}

void PlanTransformer::matchFromSrcToTarget() {
  for (int32_t srcBranchId : orgBranches_->getPlanSourceBranchIds()) {
    BranchSrcToTargetIterator srcBranchIte =
        orgBranches_->getBranchSrcToTargetIterator(srcBranchId);
    matchSourceBranch(srcBranchIte);
  }
}

void PlanTransformer::updateMatchResultForBranch(
    int32_t branchId,
    const VeloxNodeAddrPlanSection& matchResult,
    std::shared_ptr<PatternRewriter> rewriterPtr) {
  // update branchIdMatchResultMap_
  std::shared_ptr<MatchResultRewriterList> branchMatchResult;
  if (branchIdMatchResultMap_.find(branchId) != branchIdMatchResultMap_.end()) {
    branchMatchResult = branchIdMatchResultMap_[branchId];
  } else {
    branchIdMatchResultMap_[branchId] = std::make_shared<MatchResultRewriterList>();
    branchMatchResult = branchIdMatchResultMap_[branchId];
  }
  std::shared_ptr<VeloxNodeAddrPlanSection> matchResultPtr =
      std::make_shared<VeloxNodeAddrPlanSection>(matchResult);
  PlanSectionRewriterPair resultRewriterPair = {matchResultPtr, rewriterPtr};
  branchMatchResult->emplace_back(
      std::make_shared<PlanSectionRewriterPair>(resultRewriterPair));
}

bool PlanTransformer::acceptMatchResult(
    const VeloxNodeAddrPlanSection& matchResult) const {
  if (VeloxPlanNodeAddr::invalid().equal(matchResult.source) ||
      VeloxPlanNodeAddr::invalid().equal(matchResult.target)) {
    return false;
  }
  int32_t branchId = matchResult.target.branchId;
  if (branchIdMatchResultMap_.find(branchId) == branchIdMatchResultMap_.end()) {
    return true;
  }
  std::shared_ptr<MatchResultRewriterList> targetBranchMatchResult =
      branchIdMatchResultMap_.at(branchId);
  if (targetBranchMatchResult->empty() || targetBranchMatchResult->size() == 0) {
    return true;
  } else {
    std::shared_ptr<PlanSectionRewriterPair> latestResult =
        targetBranchMatchResult->back();
    std::shared_ptr<VeloxNodeAddrPlanSection> latestPlanSection =
        latestResult->planSection;
    if (latestPlanSection->isBefore(matchResult)) {
      return false;
    } else {
      return true;
    }
  }
}

void PlanTransformer::matchSourceBranch(BranchSrcToTargetIterator& srcBranchIte) {
  bool matchedLastTime = false;
  VeloxNodeAddrPlanSection lastMatchResult;
  do {
    if (!matchedLastTime) {
      // No pattern can match last time, move one step towards target
      if (srcBranchIte.hasNext()) {
        srcBranchIte.next();
      } else {
        break;
      }
    } else {
      VeloxPlanNodeAddr startPos = orgBranches_->moveToTarget(lastMatchResult.target);
      if (VeloxPlanNodeAddr::invalid().equal(startPos)) {
        break;
      } else {
        srcBranchIte.setCurPos(startPos);
      }
    }
    // The point to start match.
    VeloxPlanNodeAddr matchStartPos = srcBranchIte.getCurPos();
    matchedLastTime = false;
    for (std::shared_ptr<PatternRewriter> patternRewriter : rewriterList_) {
      // use different iterator for each pattern
      VeloxNodeAddrPlanSection patternIteratorSection{orgBranches_->getPlanNodeAddr(1, 0),
                                                      matchStartPos};
      BranchSrcToTargetIterator patternIterator =
          orgBranches_->getPlanSectionSrcToTargetIterator(patternIteratorSection);
      auto matched = patternRewriter->matchFromSrc(patternIterator);
      // if the current patternRewriter pattern can match
      if (matched.first) {
        VeloxNodeAddrPlanSection matchResult = matched.second;
        if (!matchResult.source.equal(matchStartPos)) {
          VELOX_UNSUPPORTED(
              "Match result should always start from match start point while currently "
              "match result source nodeId is {} and matchStartPos nodeId is {} .",
              matchResult.source.nodeId,
              matchStartPos.nodeId);
        }
        if (acceptMatchResult(matchResult)) {
          if (!hasMatchResult_) {
            hasMatchResult_ = true;
          }
          matchedLastTime = true;
          lastMatchResult = matchResult;
          if (matchResult.crossBranch()) {
            updateMatchResultForBranch(
                matchResult.source.branchId, matchResult, patternRewriter);
            updateMatchResultForBranch(
                matchResult.target.branchId, matchResult, patternRewriter);
            std::vector<int32_t> coveredBranchIds = matchResult.coveredBranches();
            if (!coveredBranchIds.empty()) {
              matchResultCoveredBranchIds_.insert(matchResultCoveredBranchIds_.end(),
                                                  coveredBranchIds.begin(),
                                                  coveredBranchIds.end());
            }
          } else {
            updateMatchResultForBranch(
                matchResult.target.branchId, matchResult, patternRewriter);
          }
          // if the match result is accepted, break the for loop because
          // there is no need to let other patten try to match start from the
          // position.
          break;
        } else {
          // if matched but not acceptted, simply give up match alone the path.
          return;
        }
      }
    }
  } while (srcBranchIte.hasNext());
}

VeloxPlanNodePtr PlanTransformer::rewriteAllBranches() {
  for (int32_t branchId : orgBranches_->getSrcToRootBranchIds()) {
    if (!coveredByMatchResult(branchId)) {
      rewriteBranch(branchId);
    }
  }
  return lookupRewrittenMap(1, 0);
}

bool PlanTransformer::coveredByMatchResult(int32_t branchId) const {
  auto found = std::find(
      matchResultCoveredBranchIds_.begin(), matchResultCoveredBranchIds_.end(), branchId);
  // the whole branch is not covered by match results
  if (found == matchResultCoveredBranchIds_.end()) {
    return false;
  } else {
    return true;
  }
}

bool PlanTransformer::foundBranchMatchResults(int32_t branchId) const {
  return branchIdMatchResultMap_.count(branchId) > 0;
}

void PlanTransformer::rewriteBranch(int32_t branchId) {
  VeloxPlanBranch curBranch = orgBranches_->getBranch(branchId);
  int32_t branchSize = curBranch.size();
  std::shared_ptr<MatchResultRewriterList> branchMatchResults =
      branchIdMatchResultMap_[branchId];
  if (!branchMatchResults) {
    return;
  }
  size_t matchResultsCount = branchMatchResults->size();
  for (int idx = 0; idx < matchResultsCount; idx++) {
    std::shared_ptr<PlanSectionRewriterPair> curPair = branchMatchResults->at(idx);
    std::shared_ptr<VeloxNodeAddrPlanSection> curMatchResult = curPair->planSection;
    if (curMatchResult->crossBranch()) {
      // do not process the cross branch match result if its target not
      // belongs to this branch.
      if (branchId != curMatchResult->target.branchId) {
        break;
      }
    }
    VeloxPlanNodeAddr matchResultTarget =
        orgBranches_->moveToTarget(curMatchResult->target);
    VeloxPlanNodePtr matchResultPtr = rewriteMatchResult(*curPair);
    if (!VeloxPlanNodeAddr::invalid().equal(matchResultTarget)) {
      int32_t matchResultTargetBranchId = matchResultTarget.branchId;
      if (matchResultTargetBranchId == branchId) {
        PlanUtil::changeNodeSource(matchResultTarget.nodePtr, matchResultPtr);
      } else {  // matchResult target node is a Join
        if (branchId == orgBranches_->getLeftSrcBranchId(matchResultTargetBranchId)) {
          PlanUtil::changeJoinNodeLeftSource(matchResultTarget.nodePtr, matchResultPtr);
        } else {
          PlanUtil::changeJoinNodeRightSource(matchResultTarget.nodePtr, matchResultPtr);
        }
      }
    }
  }
}

VeloxPlanNodePtr PlanTransformer::rewriteMatchResult(
    const PlanSectionRewriterPair& resultPair) {
  std::shared_ptr<VeloxNodeAddrPlanSection> planSectionPtr = resultPair.planSection;
  std::shared_ptr<PatternRewriter> rewriterPtr = resultPair.rewriter;
  VeloxPlanNodeAddrList sourceNodeList = orgBranches_->getAllSourcesOf(*planSectionPtr);
  VeloxPlanNodePtr rewrittenPtr = nullptr;

  if (sourceNodeList.size() == 0) {
    VeloxPlanNodeAddr invalidSrc = VeloxPlanNodeAddr::invalid();
    rewrittenPtr = rewriterPtr->rewrite(*planSectionPtr, invalidSrc);
  } else if (sourceNodeList.size() == 1) {
    VeloxPlanNodeAddr oldSrcAddr = sourceNodeList[0];
    VeloxPlanNodePtr newSrcPtr =
        lookupRewrittenMap(oldSrcAddr.branchId, oldSrcAddr.nodeId);
    VeloxPlanNodeAddr newSrcAddr{
        oldSrcAddr.root, oldSrcAddr.branchId, oldSrcAddr.nodeId, newSrcPtr};
    rewrittenPtr = rewriterPtr->rewrite(*planSectionPtr, newSrcAddr);
  } else if (sourceNodeList.size() > 1) {
    VeloxPlanNodeAddrList srcNodeList;
    for (VeloxPlanNodeAddr srcAddr : sourceNodeList) {
      VeloxPlanNodePtr rewrittenSrcNodePtr =
          lookupRewrittenMap(srcAddr.branchId, srcAddr.nodeId);
      VeloxPlanNodeAddr newSrcAddr{
          srcAddr.root, srcAddr.branchId, srcAddr.nodeId, rewrittenSrcNodePtr};
      srcNodeList.emplace_back(newSrcAddr);
    }
    rewrittenPtr = rewriterPtr->rewriteWithMultiSrc(*planSectionPtr, srcNodeList);
  }
  insertRewrittenMap(
      planSectionPtr->target.branchId, planSectionPtr->target.nodeId, rewrittenPtr);
  return rewrittenPtr;
}

VeloxPlanNodePtr PlanTransformer::lookupRewrittenMap(int32_t branchId, int32_t nodeId) {
  std::pair<int32_t, int32_t> nodeAddr;
  nodeAddr.first = branchId;
  nodeAddr.second = nodeId;

  auto found = rewrittenNodePtrMap_.find(nodeAddr);
  // found the branch
  if (found != rewrittenNodePtrMap_.end()) {
    return found->second;
  }
  // not found nodePtr in rewrittenNodePtrMap_
  // try to find it in orgBranches_
  return orgBranches_->getPlanNodeAddr(branchId, nodeId).nodePtr;
}

void PlanTransformer::insertRewrittenMap(int32_t branchId,
                                         int32_t nodeId,
                                         VeloxPlanNodePtr planNode) {
  std::pair<int32_t, int32_t> nodeAddr{};
  nodeAddr.first = branchId;
  nodeAddr.second = nodeId;
  rewrittenNodePtrMap_[nodeAddr] = planNode;
}

PlanTransformerFactory& PlanTransformerFactory::registerPattern(
    std::shared_ptr<PlanPattern> pattern,
    std::shared_ptr<PlanRewriter> rewriter) {
  patternRewriters_.emplace_back(std::make_shared<PatternRewriter>(pattern, rewriter));
  return *this;
}

std::shared_ptr<PlanTransformer> PlanTransformerFactory::getTransformer(
    VeloxPlanNodePtr root) {
  return std::make_shared<PlanTransformer>(patternRewriters_, root);
}

}  // namespace facebook::velox::plugin::plantransformer
