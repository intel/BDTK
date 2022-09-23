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

#include <fstream>
#include <thread>
#include "PlanTransformer.h"

namespace facebook::velox::plugin::plantransformer {

PlanTransformer::PlanTransformer(PatternRewriterList& rewriterList,
                                 VeloxPlanNodePtr root) {
  rewriterList_ = rewriterList;
  root_ = root;
  PlanBranches branches(root);
  orgBranches_ = std::make_shared<PlanBranches>(branches);
}

VeloxPlanNodePtr PlanTransformer::transform() {
  matchFromSrcToTarget();
  if (hasMatchResult_) {
    VeloxPlanNodePtr rewrittenRoot = rewriteAllBranches();
    return rewrittenRoot;
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
    VeloxNodeAddrPlanSection& matchResult,
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

bool PlanTransformer::acceptMatchResult(VeloxNodeAddrPlanSection& matchResult) {
  if (VeloxPlanNodeAddr::invalid().equal(matchResult.source) ||
      VeloxPlanNodeAddr::invalid().equal(matchResult.target)) {
    return false;
  }
  int32_t branchId = matchResult.target.branchId;
  if (branchIdMatchResultMap_.find(branchId) == branchIdMatchResultMap_.end()) {
    return true;
  }
  std::shared_ptr<MatchResultRewriterList> targetBranchMatchResult =
      branchIdMatchResultMap_[branchId];
  if (targetBranchMatchResult->empty() or targetBranchMatchResult->size() == 0) {
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
    // the whole branch is not covered by match results
    if (!coveredByMatchResult(branchId)) {
      rewriteBranch(branchId);
    }
  }
  return lookupRewrittenMap(1, 0);
}

bool PlanTransformer::coveredByMatchResult(int32_t branchId) {
  auto found = std::find(
      matchResultCoveredBranchIds_.begin(), matchResultCoveredBranchIds_.end(), branchId);
  // the whole branch is not covered by match results
  if (found == matchResultCoveredBranchIds_.end()) {
    return false;
  } else {
    return true;
  }
}

bool PlanTransformer::foundBranchMatchResults(int32_t branchId) {
  auto found = branchIdMatchResultMap_.find(branchId);
  if (found == branchIdMatchResultMap_.end()) {
    return false;
  } else {
    return true;
  }
}

VeloxPlanNodePtr PlanTransformer::cloneBranchWithRewrittenSrc(int32_t branchId,
                                                              int32_t startNodeId) {
  VeloxPlanBranch branch = orgBranches_->getBranch(branchId);
  int32_t branchSize = branch.size();
  VeloxPlanNodePtr curNode;
  VeloxPlanNodePtr curClonedNode = nullptr;
  for (int idx = branchSize - 1; idx >= startNodeId; idx--) {
    curNode = branch[idx];
    if (idx == branchSize - 1 && PlanUtil::isJoin(curNode)) {
      int32_t leftBranchId = orgBranches_->getLeftSrcBranchId(branchId);
      int32_t rightBranchId = orgBranches_->getRightSrcBranchId(branchId);
      auto left = lookupRewrittenMap(leftBranchId, 0);
      auto right = lookupRewrittenMap(rightBranchId, 0);
      curClonedNode = PlanUtil::cloneJoinNodeWithNewSources(curNode, left, right);
    } else {
      curClonedNode = PlanUtil::cloneNodeWithNewSource(curNode, curClonedNode);
    }
  }
  if (curClonedNode != nullptr) {
    insertRewrittenMap(branchId, startNodeId, curClonedNode);
  }
  return curClonedNode;
}

VeloxPlanNodePtr PlanTransformer::rewriteBranchSection(
    VeloxNodeAddrPlanSection branchSection) {
  if (branchSection.crossBranch()) {
    return nullptr;
  }
  VeloxPlanNodeAddr SrcNode = branchSection.source;
  VeloxPlanNodeAddr TarNode = branchSection.target;
  int32_t branchId = SrcNode.branchId;
  VeloxPlanBranch branch = orgBranches_->getBranch(branchId);
  VeloxPlanNodePtr rewrittenSrc = lookupRewrittenMap(SrcNode.branchId, SrcNode.nodeId);
  VeloxPlanNodePtr curSrc = rewrittenSrc;
  for (int idx = SrcNode.nodeId - 1; idx >= TarNode.nodeId; idx--) {
    curSrc = PlanUtil::cloneNodeWithNewSource(branch[idx], curSrc);
  }
  if (curSrc != nullptr) {
    insertRewrittenMap(branchId, TarNode.nodeId, curSrc);
  }
  return curSrc;
}

void PlanTransformer::rewriteBranch(int32_t branchId) {
  if (!foundBranchMatchResults(branchId)) {
    if (!coveredByMatchResult(branchId)) {
      VeloxPlanNodePtr clonedBranchRoot = cloneBranchWithRewrittenSrc(branchId, 0);
    }
  } else {
    VeloxPlanBranch curBranch = orgBranches_->getBranch(branchId);
    int32_t branchSize = curBranch.size();
    std::shared_ptr<MatchResultRewriterList> branchMatchResults =
        branchIdMatchResultMap_[branchId];
    size_t matchResultsCount = branchMatchResults->size();
    VeloxPlanNodePtr clonedStartPtr;
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
      if (idx == 0 && curMatchResult->source.nodeId < branchSize - 1) {
        cloneBranchWithRewrittenSrc(branchId, curMatchResult->source.nodeId + 1);
      }
      VeloxPlanNodePtr matchResultPtr = rewriteMatchResult(*curPair);
      VeloxPlanNodeAddr cloneStartPos = orgBranches_->getPlanNodeAddr(branchId, 0);
      // set clone start pos to the source of next match result in the branch.
      if (idx < matchResultsCount - 1) {
        std::shared_ptr<PlanSectionRewriterPair> nextPair =
            branchMatchResults->at(idx + 1);
        std::shared_ptr<VeloxNodeAddrPlanSection> nextResultSection =
            nextPair->planSection;
        VeloxPlanNodeAddr nextResultSourceAddr = nextResultSection->source;
        cloneStartPos = orgBranches_->getPlanNodeAddr(nextResultSourceAddr.branchId,
                                                      nextResultSourceAddr.nodeId + 1);
      }
      if (!cloneStartPos.equal(curMatchResult->target)) {
        VeloxNodeAddrPlanSection cloneSection =
            VeloxNodeAddrPlanSection{cloneStartPos, curMatchResult->target};
        clonedStartPtr = rewriteBranchSection(cloneSection);
      } else {
        clonedStartPtr = matchResultPtr;
      }
      insertRewrittenMap(branchId, cloneStartPos.nodeId, clonedStartPtr);
    }
  }
}

VeloxPlanNodePtr PlanTransformer::rewriteMatchResult(
    PlanSectionRewriterPair& resultPair) {
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
#if 0
  std::ofstream file_out;
  file_out.open("/tmp/fragment.txt", std::ios_base::app);
  file_out << "thread[" << std::this_thread::get_id() << "], patternRewriters_.size = " << patternRewriters_.size() << '\n';
  file_out.close();
#endif
  return *this;
}

std::shared_ptr<PlanTransformer> PlanTransformerFactory::getTransformer(
    VeloxPlanNodePtr root) {
#if 0
  std::ofstream file_out;
  file_out.open("/tmp/fragment.txt", std::ios_base::app);
  file_out << root->toString(true, true) << '\n';
  file_out.close();
  // LOG(INFO) << " ############################ " << root->toString(true, true);
#endif
  return std::make_shared<PlanTransformer>(patternRewriters_, root);
}

}  // namespace facebook::velox::plugin::plantransformer
