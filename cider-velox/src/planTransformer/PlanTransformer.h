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

#pragma once

#include <map>
#include <memory>

#include "PatternRewriter.h"
#include "PlanBranches.h"
#include "PlanPattern.h"
#include "PlanRewriter.h"

namespace facebook::velox::plugin::plantransformer {
struct PlanSectionRewriterPair {
  std::shared_ptr<VeloxNodeAddrPlanSection> planSection;
  std::shared_ptr<PatternRewriter> rewriter;
};
using PatternRewriterList = std::vector<std::shared_ptr<PatternRewriter>>;
using MatchResultRewriterList = std::vector<std::shared_ptr<PlanSectionRewriterPair>>;

class PlanTransformer {
 public:
  PlanTransformer(PatternRewriterList& rewriterList, VeloxPlanNodePtr root);
  VeloxPlanNodePtr transform();

 private:
  void matchFromSrcToTarget();
  void updateMatchResultForBranch(int32_t branchId,
                                  VeloxNodeAddrPlanSection& matchResult,
                                  std::shared_ptr<PatternRewriter> rewriterPtr);
  bool acceptMatchResult(VeloxNodeAddrPlanSection& matchResult);
  void matchSourceBranch(BranchSrcToTargetIterator& srcBranchIte);
  // rewrite from source branch to parent branch.Cross branch match result will
  // be rewritten during rewriting target branch.
  VeloxPlanNodePtr rewriteAllBranches();
  // rewrite all single branch match results of the branch and the cross branch
  // match result whose target point belongs to the branch.
  void rewriteBranch(int32_t branchId);
  VeloxPlanNodePtr cloneBranchWithRewrittenSrc(int32_t branchId, int32_t startNodeId);
  VeloxPlanNodePtr rewriteBranchSection(VeloxNodeAddrPlanSection branchSection);
  // rewrite a single match result and insert the rewrittern result into the
  // rewritten map
  VeloxPlanNodePtr rewriteMatchResult(PlanSectionRewriterPair& resultPair);
  VeloxPlanNodePtr lookupRewrittenMap(int32_t branchId, int32_t nodeId);
  void insertRewrittenMap(int32_t branchId, int32_t nodeId, VeloxPlanNodePtr planNode);
  bool coveredByMatchResult(int32_t branchId);
  bool foundBranchMatchResults(int32_t branchId);

  PatternRewriterList rewriterList_;
  VeloxPlanNodePtr root_;
  // The read-only branch view of the original plan.
  std::shared_ptr<PlanBranches> orgBranches_;
  // map: branchId -> vector<PlanSectionRewriterPair>
  // for cross branch match result, insert it into both source and target branch
  std::map<int32_t, std::shared_ptr<MatchResultRewriterList>> branchIdMatchResultMap_;
  std::vector<int32_t> matchResultCoveredBranchIds_;
  bool hasMatchResult_ = false;
  // map: (branchId,nodeId)->nodePtr after rewrite
  std::map<std::pair<int32_t, int32_t>, VeloxPlanNodePtr> rewrittenNodePtrMap_;
};
// PlanTransformerFactory is responsible for registering the user implemented
// PlanPattern, PlanRewriter. After (PlanPattern, PlanRewriter) is registered,
// getTransformer can be called to get the transformer.
class PlanTransformerFactory {
 public:
  PlanTransformerFactory() {}
  PlanTransformerFactory& registerPattern(std::shared_ptr<PlanPattern> pattern,
                                          std::shared_ptr<PlanRewriter> rewriter);
  std::shared_ptr<PlanTransformer> getTransformer(VeloxPlanNodePtr root);

 private:
  PatternRewriterList patternRewriters_;
};
}  // namespace facebook::velox::plugin::plantransformer
