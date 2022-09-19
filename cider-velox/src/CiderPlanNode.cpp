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

#include <mutex>

#include "CiderPlanNode.h"
#include "CiderPlanNodeTranslator.h"
#include "cider/CiderException.h"
#include "substrait/plan.pb.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

namespace facebook::velox::plugin {

namespace {

substrait::VeloxToSubstraitPlanConvertor& getSubstraitConvertor() {
  static substrait::VeloxToSubstraitPlanConvertor convertor;
  return convertor;
}

std::shared_ptr<const core::PlanNode> getValueNode(
    const std::shared_ptr<const core::PlanNode>& plan) {
  return plan->sources().empty() ? plan : getValueNode(plan->sources()[0]);
}

}  // namespace

const std::vector<core::PlanNodePtr>& CiderPlanNode::sources() const {
  return sources_;
}

std::string_view CiderPlanNode::name() const {
  return "CiderPlanNode";
}

const RowTypePtr& CiderPlanNode::outputType() const {
  return outputType_;
}

const bool CiderPlanNode::isKindOf(CiderPlanNodeKind kind) const {
  // TODO: we need check from substrait plan after join plan translator is available in
  // velox plugin
  switch (kind) {
    case CiderPlanNodeKind::kJoin: {
      return sources_.size() > 1;
    }
    case CiderPlanNodeKind::kAggregation: {
      for (auto& rel : plan_.relations()) {
        if (rel.has_root() && rel.root().has_input()) {
          return rel.root().input().has_aggregate();
        }
      }
      return false;
    }
    default:
      VELOX_UNSUPPORTED("Unsupported kind " + kindToString(kind));
  }
}

const std::string CiderPlanNode::kindToString(CiderPlanNodeKind kind) const {
  switch (kind) {
    case CiderPlanNodeKind::kJoin: {
      return "Join";
    }
    case CiderPlanNodeKind::kAggregation: {
      return "Aggregation";
    }
    default: {
      return "Unknown";
    }
  }
}

}  // namespace facebook::velox::plugin
