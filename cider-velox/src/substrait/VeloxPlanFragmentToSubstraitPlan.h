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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox::core;

namespace facebook::velox::substrait {

class VeloxPlanFragmentToSubstraitPlan {
 public:
  VeloxPlanFragmentToSubstraitPlan();

  /// Converts velox plan fragment to substrait plan.
  ::substrait::Plan& toSubstraitPlan(const core::PlanNodePtr& targetNode,
                                     const core::PlanNodePtr& sourceNode);

  /// Constructs Velox plan from given plan fragment by appending a ValuesNode
  /// as input of the source Node.
  /// This is used when offloading a partial plan from plan fragment to other
  /// engine backend. A new ValuesNode is introduced as data source node. And
  /// related data convertor needs to convert related format to offloading
  /// engine.
  core::PlanNodePtr constructVeloxPlan(const core::PlanNodePtr& targetNode,
                                       const core::PlanNodePtr& sourceNode);

  /// Makes a vector of 'RowTypePtr' types with RowVectorPtr.
  /// And the value in the RowVector is set null since we only need the schema.
  std::vector<RowVectorPtr> makeVectors(const RowTypePtr& rowType);

 private:
  /// Reconstructs Velox plan from sourceNode to targetNode according given
  /// plan fragment.
  void reconstructVeloxPlan(const std::vector<core::PlanNodePtr>& planNodeList);

  /// Given a sourceNode of plan section, test whether we should append a ValuesNode
  /// for it in case of the source node is neither a ValuesNode nor a AbstraitJoinNode.
  bool shouldAppendValuesNode(const PlanNodePtr& sourceNode) const;

  /// Make a ValuesNode as the input of planFragment source node if the
  /// source node is not a valuesNode.
  std::shared_ptr<const ValuesNode> makeValuesNode(const core::PlanNodePtr& sourceNode);

  std::shared_ptr<const VeloxToSubstraitPlanConvertor> v2SPlanConvertor_;
  std::shared_ptr<exec::test::PlanBuilder> planBuilder_;

  memory::MemoryPool* pool() const { return pool_.get(); }

  std::unique_ptr<memory::MemoryPool> pool_{memory::getDefaultScopedMemoryPool()};
  google::protobuf::Arena arena_;
};
}  // namespace facebook::velox::substrait
