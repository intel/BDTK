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

#include "planTransformer/PlanRewriter.h"

namespace facebook::velox::plugin::plantransformer::test {
using namespace facebook::velox::core;
class TestCiderPlanNode : public PlanNode {
 public:
  TestCiderPlanNode(const PlanNodeId& id, std::shared_ptr<const PlanNode> source)
      : PlanNode(id), sources_{source} {}

  TestCiderPlanNode(const PlanNodeId& id,
                    std::vector<std::shared_ptr<const PlanNode>> source)
      : PlanNode(id), sources_(source) {}

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override;

  std::string_view name() const override;

  const RowTypePtr& outputType() const override;

 private:
  const std::vector<std::shared_ptr<const PlanNode>> sources_;
  void addDetails(std::stringstream& stream) const override;
};

class CiderPatternTestNodeRewriter : public PlanRewriter {
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithSingleSource(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddr& source) const override;

  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithMultiSources(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddrList& srcList) const override;
};

}  // namespace facebook::velox::plugin::plantransformer::test
