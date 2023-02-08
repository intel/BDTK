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

#include "PlanNodeAddr.h"

namespace facebook::velox::plugin::plantransformer {
// PlanRewriter is the API of the pattern match - rewrite framework.
// The user of the framework need provide implementation of the interface
// and register it to PlanTransformer.
class PlanRewriter {
 public:
  PlanRewriter() {}
  ~PlanRewriter() {}
  // The methods called by PlanTransformer.
  VeloxPlanNodePtr rewrite(const VeloxNodeAddrPlanSection& planSection,
                           const VeloxPlanNodeAddr& source) const;

  VeloxPlanNodePtr rewriteWithMultiSrc(const VeloxNodeAddrPlanSection& planSection,
                                       const VeloxPlanNodeAddrList& srcList) const;

  // rewrite the plan section.
  // planSection: the plan section which need to be rewrite.
  // source: the source node the planSection need point to after rewrite.
  // return: the first bool indicate whether this planSection need to be
  // rewritten the second nodeptr indicate the rewritten result if rewritten is
  // needed.
  virtual std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithSingleSource(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddr& source) const = 0;
  virtual std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithMultiSources(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddrList& srcList) const = 0;
};
}  // namespace facebook::velox::plugin::plantransformer
