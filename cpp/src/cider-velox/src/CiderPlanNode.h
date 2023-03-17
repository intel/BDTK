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

#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

#include "exec/nextgen/Nextgen.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"

namespace facebook::velox::plugin {

enum class CiderPlanNodeKind { kJoin, kAggregation };

class CiderPlanNode : public core::PlanNode {
 public:
  explicit CiderPlanNode(const core::PlanNodeId& id,
                         const core::PlanNodePtr& source,
                         const RowTypePtr& outputType,
                         const ::substrait::Plan& plan)
      : core::PlanNode(id), sources_({source}), plan_(plan), outputType_(outputType) {
    auto translator = generator::SubstraitToRelAlgExecutionUnit(plan);
    RelAlgExecutionUnit ra_exe_unit = translator.createRelAlgExecutionUnit();
    codegen_context_ = cider::exec::nextgen::compile(ra_exe_unit);
  }

  explicit CiderPlanNode(const core::PlanNodeId& id,
                         const core::PlanNodePtr& left,
                         const core::PlanNodePtr& right,
                         const RowTypePtr& outputType,
                         const ::substrait::Plan& plan)
      : PlanNode(id), sources_({left, right}), plan_(plan), outputType_(outputType) {}

  const cider::exec::nextgen::context::CodegenCtxPtr& codegenCtx() const {
    return codegen_context_;
  }

  const RowTypePtr& outputType() const override;

  const std::vector<core::PlanNodePtr>& sources() const override;

  std::string_view name() const override;

  ::substrait::Plan getSubstraitPlan() const { return plan_; }

  const bool isKindOf(CiderPlanNodeKind kind) const;

  const std::string kindToString(CiderPlanNodeKind kind) const;

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "CiderPlanNode: " << plan_.ShortDebugString();
  }

  // TODO: will support multiple source?
  const std::vector<core::PlanNodePtr> sources_;
  const ::substrait::Plan plan_;
  const RowTypePtr outputType_;

  // just compile once, and save the compiled result here
  cider::exec::nextgen::context::CodegenCtxPtr codegen_context_;
};

}  // namespace facebook::velox::plugin
