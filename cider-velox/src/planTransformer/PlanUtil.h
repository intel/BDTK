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

namespace facebook::velox::plugin::plantransformer {
using NodeAddrMap = std::map<std::pair<int32_t, int32_t>, VeloxPlanNodePtr>;
using NodeAddrMapPtr = std::shared_ptr<NodeAddrMap>;
class PlanUtil {
 public:
  static bool isJoin(VeloxPlanNodePtr node);
  static VeloxPlanNodePtr cloneNodeWithNewSource(VeloxPlanNodePtr node,
                                                 VeloxPlanNodePtr source);
  static VeloxPlanNodePtr cloneSingleSourcePlanSectionWithNewSource(
      VeloxNodeAddrPlanSection& planSection,
      VeloxPlanNodeAddr& source);
  static VeloxPlanNodePtr cloneMultiSourcePlanSectionWithNewSources(
      VeloxNodeAddrPlanSection& planSection,
      VeloxPlanNodeAddrList& source);
  static VeloxPlanNodePtr cloneJoinNodeWithNewSources(VeloxPlanNodePtr node,
                                                      VeloxPlanNodePtr left,
                                                      VeloxPlanNodePtr right);
  static VeloxPlanNodeAddrList getPlanNodeListForPlanSection(
      VeloxNodeAddrPlanSection& planSection);

 private:
  static NodeAddrMapPtr toNodeAddrMap(VeloxPlanNodeAddrList& nodeAddrList);
  static std::pair<bool, VeloxPlanNodePtr> findInNodeAddrMap(NodeAddrMapPtr map,
                                                             int32_t branchId,
                                                             int32_t nodeId);
};
}  // namespace facebook::velox::plugin::plantransformer
