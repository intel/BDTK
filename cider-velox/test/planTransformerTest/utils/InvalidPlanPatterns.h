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

#include "planTransformer/PlanPattern.h"

namespace facebook::velox::plugin::plantransformer::test {
// This plan pattern always produce invalid pattern.
class InvalidPlanPattern : public PlanPattern {
 public:
  std::pair<bool, VeloxNodeAddrPlanSection> matchFromSrc(
      BranchSrcToTargetIterator branchIte) const override {
    VeloxPlanNodeAddr curAddr = VeloxPlanNodeAddr::invalid();
    VeloxPlanNodeAddr prevAddr = VeloxPlanNodeAddr::invalid();
    VeloxNodeAddrPlanSection planSection{};
    if (branchIte.hasNext()) {
      curAddr = branchIte.next();
    }
    if (branchIte.hasNext()) {
      prevAddr = curAddr;
      curAddr = branchIte.next();
    }
    planSection.source = curAddr;
    planSection.target = prevAddr;
    return std::pair<bool, VeloxNodeAddrPlanSection>(true, planSection);
  };
};

class NotStartFromMatchPointPlanPattern : public PlanPattern {
 public:
  std::pair<bool, VeloxNodeAddrPlanSection> matchFromSrc(
      BranchSrcToTargetIterator branchIte) const override {
    VeloxPlanNodeAddr secondNode = VeloxPlanNodeAddr::invalid();
    VeloxPlanNodeAddr thirdNode = VeloxPlanNodeAddr::invalid();
    VeloxNodeAddrPlanSection planSection{};
    if (branchIte.hasNext()) {
      branchIte.next();
      if (branchIte.hasNext()) {
        secondNode = branchIte.next();
        if (branchIte.hasNext()) {
          thirdNode = branchIte.next();
        }
      }
    }
    planSection.source = secondNode;
    planSection.target = thirdNode;
    return std::pair<bool, VeloxNodeAddrPlanSection>(true, planSection);
  };
};
}  // namespace facebook::velox::plugin::plantransformer::test
