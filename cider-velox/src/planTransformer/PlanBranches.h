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

#include "PlanNodeAddr.h"
#include "PlanUtil.h"

namespace facebook::velox::plugin::plantransformer {
using VeloxPlanBranch = std::vector<VeloxPlanNodePtr>;
class BranchSrcToTargetIterator;
class PlanBranches {
  using VeloxPlanBranchMap =
      std::map<int32_t, VeloxPlanBranch>;  // branch id -> brach vector map

 public:
  // Plan Branches is a read-only branch view of the plan start from root.
  PlanBranches(VeloxPlanNodePtr root);  // NOLINT
  VeloxPlanNodePtr getRoot() { return root_; }
  // get all the source branch ids of the plan;
  std::vector<int32_t> getPlanSourceBranchIds() { return srcBranchIds_; }
  std::vector<int32_t> getSrcToRootBranchIds() { return srcToRootBranchIds_; }
  // branch Id start from 1.
  VeloxPlanBranch getBranch(int32_t branchId);
  static int32_t getParentBranchId(int32_t branchId);
  static int32_t getLeftSrcBranchId(int32_t branchId);
  static int32_t getRightSrcBranchId(int32_t branchId);
  VeloxPlanNodeAddr getPlanNodeAddr(int32_t branchId, int32_t nodeId);
  VeloxPlanNodeAddr getBranchSrcNodeAddr(int32_t branchId);
  VeloxPlanNodeAddr moveToTarget(VeloxPlanNodeAddr& curNodeAddr);
  VeloxPlanNodeAddrList getAllSourcesOf(VeloxNodeAddrPlanSection& planSection);
  BranchSrcToTargetIterator getBranchSrcToTargetIterator(int32_t branchId);
  BranchSrcToTargetIterator getPlanSectionSrcToTargetIterator(
      VeloxNodeAddrPlanSection& planSection);

 private:
  // Split the plan into branches
  void splitPlanBranches(VeloxPlanNodePtr rootNode);
  void splitPlanBranches(VeloxPlanNodePtr rootNode, int32_t branchIdx);
  static VeloxPlanBranch makePlanBranch(VeloxPlanNodePtr root);

  VeloxPlanNodePtr root_;
  // Those fields will be filled during split Plan branches.
  std::vector<int32_t> srcBranchIds_;
  std::vector<int32_t> srcToRootBranchIds_;
  // The plannode order in branch vector is from target to source.
  VeloxPlanBranchMap branchMap_;
};

// This class travels the plan from source to target.
class BranchSrcToTargetIterator {
 public:
  BranchSrcToTargetIterator(PlanBranches& branches, int32_t branchId);
  BranchSrcToTargetIterator(PlanBranches& branches,
                            VeloxNodeAddrPlanSection& planSection);
  bool hasNext();
  VeloxPlanNodeAddr next();
  VeloxPlanNodeAddr getCurPos();
  void setCurPos(VeloxPlanNodeAddr& curPos);

 private:
  std::shared_ptr<PlanBranches> planBranches_;
  int32_t branchId_;
  VeloxPlanNodeAddr curPlanNodeId_;
  std::shared_ptr<VeloxNodeAddrPlanSection> planSection_;
  bool firstMove_ = true;
};
}  // namespace facebook::velox::plugin::plantransformer
