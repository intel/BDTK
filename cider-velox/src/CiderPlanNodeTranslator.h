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

#include <memory>
#include <optional>
#include <string>
#include "CiderJoinBuild.h"
#include "CiderOperator.h"
#include "CiderPlanNode.h"

namespace facebook::velox::plugin {

class CiderPlanNodeTranslator : public exec::Operator::PlanNodeTranslator {
 public:
  explicit CiderPlanNodeTranslator(uint32_t maxDrivers = 1) : maxDrivers_{maxDrivers} {}

  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const core::PlanNode>& node) override {
    if (auto ciderPlanNode = std::dynamic_pointer_cast<const CiderPlanNode>(node)) {
      return CiderOperator::Make(id, ctx, ciderPlanNode);
    }
    return nullptr;
  }

  std::unique_ptr<exec::JoinBridge> toJoinBridge(
      const std::shared_ptr<const core::PlanNode>& node) {
    if (auto ciderJoinNode = std::dynamic_pointer_cast<const CiderPlanNode>(node)) {
      return std::make_unique<CiderJoinBridge>();
    }
    return nullptr;
  }

  exec::OperatorSupplier toOperatorSupplier(
      const std::shared_ptr<const core::PlanNode>& node) {
    if (auto ciderJoinNode = std::dynamic_pointer_cast<const CiderPlanNode>(node)) {
      return [ciderJoinNode](int32_t operatorId, exec::DriverCtx* ctx) {
        return std::make_unique<CiderJoinBuild>(operatorId, ctx, ciderJoinNode);
      };
    }
    return nullptr;
  }

  std::optional<uint32_t> maxDrivers(
      const std::shared_ptr<const core::PlanNode>& node) override {
    if (auto ciderPlanNode = std::dynamic_pointer_cast<const CiderPlanNode>(node)) {
      return maxDrivers_;
    }
    return std::nullopt;
  }

 private:
  uint32_t maxDrivers_;
};

}  // namespace facebook::velox::plugin
