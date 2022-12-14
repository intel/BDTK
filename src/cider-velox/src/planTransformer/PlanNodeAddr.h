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
#include <vector>

#include "velox/core/PlanNode.h"

namespace facebook::velox::plugin::plantransformer {

using VeloxPlanNodePtr = std::shared_ptr<const facebook::velox::core::PlanNode>;
using VeloxPlanNodeVec = std::vector<VeloxPlanNodePtr>;

struct VeloxPlanSection {
  VeloxPlanNodePtr target;
  VeloxPlanNodePtr source;
  bool multiSectionSource() const;
};

struct VeloxPlanNodeAddr {
  VeloxPlanNodePtr root;
  int32_t branchId = -1;
  int32_t nodeId = -1;
  VeloxPlanNodePtr nodePtr;
  bool equal(const VeloxPlanNodeAddr& addr) const;
  static VeloxPlanNodeAddr& invalid();
};

struct VeloxNodeAddrPlanSection {
  VeloxPlanNodeAddr target;
  VeloxPlanNodeAddr source;
  bool isValid() const;
  bool isBefore(const VeloxNodeAddrPlanSection& section) const;
  bool crossBranch() const;
  std::vector<int32_t> coveredBranches() const;
};
using VeloxPlanNodeAddrList = std::vector<VeloxPlanNodeAddr>;

}  // namespace facebook::velox::plugin::plantransformer
