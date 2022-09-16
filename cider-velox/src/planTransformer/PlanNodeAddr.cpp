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

#include "PlanNodeAddr.h"

namespace facebook::velox::plugin::plantransformer {
bool VeloxPlanSection::multiSectionSource() {
  return (source->sources().size() > 1);
}

bool VeloxPlanNodeAddr::equal(VeloxPlanNodeAddr addr) {
  return (root == addr.root and branchId == addr.branchId and nodeId == addr.nodeId);
}

VeloxPlanNodeAddr VeloxPlanNodeAddr::invalid() {
  return {nullptr, -1, -1, nullptr};
}

bool VeloxNodeAddrPlanSection::isValid() {
  if (VeloxPlanNodeAddr::invalid().equal(target) ||
      VeloxPlanNodeAddr::invalid().equal(source)) {
    return false;
  }
  if (target.root != source.root) {
    return false;
  }
  if (target.branchId > source.branchId) {
    return false;
  } else if (target.branchId == source.branchId) {
    if (target.nodeId > source.nodeId) {
      return false;
    }
  }
  return true;
}

bool VeloxNodeAddrPlanSection::isBefore(VeloxNodeAddrPlanSection section) {
  if (target.equal(section.target)) {
    return true;
  }
  // if root is not the same, the section is not comparable.
  if (target.root != section.target.root) {
    return false;
  }
  if (target.branchId < section.target.branchId) {
    return true;
  } else if (target.branchId == section.target.branchId and
             target.nodeId <= section.target.nodeId) {
    return true;
  }
  return false;
}

bool VeloxNodeAddrPlanSection::crossBranch() {
  if (target.branchId != source.branchId) {
    return true;
  }
  return false;
}

std::vector<int32_t> VeloxNodeAddrPlanSection::coveredBranches() {
  if (crossBranch()) {
    int32_t parentBranchId = source.branchId / 2;
    std::vector<int32_t> coveredBranchIds;
    while (parentBranchId > target.branchId) {
      coveredBranchIds.emplace_back(parentBranchId);
      parentBranchId = parentBranchId / 2;
    }
    return coveredBranchIds;
  } else {
    return std::vector<int32_t>();
  }
}
}  // namespace facebook::velox::plugin::plantransformer
