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

#include "CiderPlanUtil.h"
#include <memory>
#include "substrait/plan.pb.h"

namespace facebook::velox::plugin::plantransformer {
std::shared_ptr<CiderPlanNode> CiderPlanUtil::toCiderPlanNode(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddr& source) {
  std::shared_ptr<VeloxPlanFragmentToSubstraitPlan> v2SPlanFragmentConvertor_ =
      std::make_shared<VeloxPlanFragmentToSubstraitPlan>();

  std::shared_ptr<::substrait::Plan> sPlan =
      std::make_shared<::substrait::Plan>(v2SPlanFragmentConvertor_->toSubstraitPlan(
          planSection.target.nodePtr, planSection.source.nodePtr));
  return std::make_shared<CiderPlanNode>(planSection.target.nodePtr->id(),
                                         source.nodePtr,
                                         planSection.target.nodePtr->outputType(),
                                         *sPlan);
}

std::shared_ptr<CiderPlanNode> CiderPlanUtil::toCiderPlanNode(
    VeloxNodeAddrPlanSection& planSection,
    VeloxPlanNodeAddrList& srcList) {
  VELOX_UNSUPPORTED("multi-source PlanSection rewrite is not supported for now.");
}

}  // namespace facebook::velox::plugin::plantransformer
