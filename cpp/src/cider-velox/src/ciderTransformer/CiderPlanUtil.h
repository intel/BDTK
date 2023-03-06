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
#pragma once

#include "CiderPlanNode.h"
#include "planTransformer/PlanNodeAddr.h"
#include "substrait/VeloxPlanFragmentToSubstraitPlan.h"
#include "substrait/plan.pb.h"

namespace facebook::velox::plugin::plantransformer {

using CiderPlanNode = facebook::velox::plugin::CiderPlanNode;
using VeloxPlanFragmentToSubstraitPlan =
    facebook::velox::substrait::VeloxPlanFragmentToSubstraitPlan;

class CiderPlanUtil {
 public:
  // Construct a CiderPlanNode based on the plan section with given source
  static std::shared_ptr<CiderPlanNode> toCiderPlanNode(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddr& source);
  static std::shared_ptr<CiderPlanNode> toCiderPlanNode(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddrList& srcList);
};

}  // namespace facebook::velox::plugin::plantransformer
