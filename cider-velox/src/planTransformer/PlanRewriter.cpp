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

#include "PlanRewriter.h"
#include "PlanUtil.h"

namespace facebook::velox::plugin::plantransformer {
VeloxPlanNodePtr PlanRewriter::rewrite(VeloxNodeAddrPlanSection& planSection,
                                       VeloxPlanNodeAddr& source) {
  auto rewrittenResult = rewritePlanSectionWithSingleSource(planSection, source);
  // if this plan sectoin need to be rewritten, just rewite it with the result
  // of rewritePlanSectionWithSingleSource. If
  // rewritePlanSectionWithSingleSource returns a nullptr, the framework treat
  // it as deletion for the whole plan section.
  if (rewrittenResult.first) {
    auto resultPtr = rewrittenResult.second;
    if (resultPtr == nullptr) {
      resultPtr = source.nodePtr;
    }
    return resultPtr;
  } else {
    // if this plan section needn't to be rewitten,simply copy one and link it
    // to the new source.
    return PlanUtil::cloneSingleSourcePlanSectionWithNewSource(planSection, source);
  }
}

VeloxPlanNodePtr PlanRewriter::rewriteWithMultiSrc(VeloxNodeAddrPlanSection& planSection,
                                                   VeloxPlanNodeAddrList& srcList) {
  auto rewrittenResult = rewritePlanSectionWithMultiSources(planSection, srcList);
  if (rewrittenResult.first) {
    auto resultPtr = rewrittenResult.second;
    if (resultPtr == nullptr) {
      // For cross branch plan section, it's not reasonable to simply
      // delete the whole plan section since the target node of the plan section
      // may can not accept multi sources.So for this situation, we
      // throw exception out directly.
      VELOX_FAIL(
          "PlanSection with multi sources nodes should not be rewritten to nullptr");
    }
    return resultPtr;
  } else {
    // if this plan section needn't to be rewitten,simply copy one and link it
    // to the new source.
    return PlanUtil::cloneMultiSourcePlanSectionWithNewSources(planSection, srcList);
  }
}

}  // namespace facebook::velox::plugin::plantransformer
