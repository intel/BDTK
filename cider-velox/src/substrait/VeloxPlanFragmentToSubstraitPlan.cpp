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

#include "VeloxPlanFragmentToSubstraitPlan.h"
#include <google/protobuf/util/json_util.h>

namespace facebook::velox::substrait {

::substrait::Plan& VeloxPlanFragmentToSubstraitPlan::toSubstraitPlan(
    const core::PlanNodePtr& targetNode,
    const core::PlanNodePtr& sourceNode) {
  ::substrait::Plan* substraitPlan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena_);

  substraitPlan->MergeFrom(
      v2SPlanConvertor_.toSubstrait(arena_, constructVeloxPlan(targetNode, sourceNode)));

  return *substraitPlan;
}

core::PlanNodePtr VeloxPlanFragmentToSubstraitPlan::constructVeloxPlan(
    const core::PlanNodePtr& targetNode,
    const core::PlanNodePtr& sourceNode) {
  planBuilder_ = std::make_shared<exec::test::PlanBuilder>();

  // Here we only consider that all nodes in the plan only have one source.
  core::PlanNodePtr tmpNode = targetNode;
  std::vector<core::PlanNodePtr> planNodeList;

  // The plan fragment has more than one node.
  if (sourceNode != targetNode) {
    do {
      planNodeList.emplace_back(tmpNode);

      // For the invalid source or target node, the tmpNode cannot equal to the
      // sourceNode, so it will iterate till the source of the original plan
      // which starts from targetNode.
      if (tmpNode->sources().size() == 0) {
        VELOX_NYI("The source node or target node is invalid, please check it!");
      } else {
        tmpNode = tmpNode->sources()[0];
      }
    } while (tmpNode != sourceNode);
  }

  // Push source node into planNodeList.
  planNodeList.emplace_back(sourceNode);

  // Append values node for the case that source node is neither valuesNode nor JoinNode.
  if (shouldAppendValuesNode(sourceNode)) {
    auto valueNode = makeValuesNode(sourceNode);
    planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
      return std::make_shared<core::ValuesNode>(valueNode->id(), valueNode->values());
    });
  }

  reconstructVeloxPlan(planNodeList);

  return planBuilder_->planNode();
}

void VeloxPlanFragmentToSubstraitPlan::reconstructVeloxPlan(
    const std::vector<core::PlanNodePtr>& planNodeList) {
  for (auto riter = planNodeList.rbegin(); riter != planNodeList.rend(); riter++) {
    if (auto projNode = std::dynamic_pointer_cast<const ProjectNode>(*riter)) {
      planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
        return std::make_shared<core::ProjectNode>(projNode->id(),
                                                   projNode->names(),
                                                   projNode->projections(),
                                                   planBuilder_->planNode());
      });
    } else if (auto aggNode = std::dynamic_pointer_cast<const AggregationNode>(*riter)) {
      planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
        return std::make_shared<AggregationNode>(aggNode->id(),
                                                 aggNode->step(),
                                                 aggNode->groupingKeys(),
                                                 aggNode->preGroupedKeys(),
                                                 aggNode->aggregateNames(),
                                                 aggNode->aggregates(),
                                                 aggNode->aggregateMasks(),
                                                 aggNode->ignoreNullKeys(),
                                                 planBuilder_->planNode());
      });
    } else if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(*riter)) {
      planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
        return std::make_shared<FilterNode>(
            filterNode->id(), filterNode->filter(), planBuilder_->planNode());
      });
    } else if (auto valuesNode = std::dynamic_pointer_cast<const ValuesNode>(*riter)) {
      planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
        return std::make_shared<ValuesNode>(valuesNode->id(), valuesNode->values());
      });
    } else if (auto joinNode =
                   std::dynamic_pointer_cast<const AbstractJoinNode>(*riter)) {
      const auto& joinLeftSource = joinNode->sources()[0];
      const auto& joinRightSource = joinNode->sources()[1];

      auto leftValuesNode = std::make_shared<ValuesNode>(
          joinLeftSource->id(), makeVectors(joinLeftSource->outputType()));
      auto rigthValuesNode = std::make_shared<ValuesNode>(
          joinRightSource->id(), makeVectors(joinRightSource->outputType()));

      if (std::dynamic_pointer_cast<const HashJoinNode>(*riter)) {
        planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
          return std::make_shared<HashJoinNode>(joinNode->id(),
                                                joinNode->joinType(),
                                                joinNode->leftKeys(),
                                                joinNode->rightKeys(),
                                                joinNode->filter(),
                                                leftValuesNode,
                                                rigthValuesNode,
                                                joinNode->outputType());
        });
      } else {
        planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
          return std::make_shared<MergeJoinNode>(joinNode->id(),
                                                 joinNode->joinType(),
                                                 joinNode->leftKeys(),
                                                 joinNode->rightKeys(),
                                                 joinNode->filter(),
                                                 leftValuesNode,
                                                 rigthValuesNode,
                                                 joinNode->outputType());
        });
      }

    } else {
      VELOX_UNSUPPORTED("Unsupported node '{}'", riter->get()->name());
    }
  }
}

std::shared_ptr<const ValuesNode> VeloxPlanFragmentToSubstraitPlan::makeValuesNode(
    const core::PlanNodePtr& sourceNode) {
  // Here we only consider that all nodes in the plan only have one source.
  auto previousNode = sourceNode->sources()[0];

  return std::make_shared<ValuesNode>(previousNode->id(),
                                      makeVectors(previousNode->outputType()));
}

std::vector<RowVectorPtr> VeloxPlanFragmentToSubstraitPlan::makeVectors(
    const RowTypePtr& rowType) {
  std::vector<RowVectorPtr> vectors;

  std::vector<VectorPtr> children;

  vectors.emplace_back(
      std::make_shared<RowVector>(pool(), rowType, nullptr, 0, children));

  return vectors;
}

bool VeloxPlanFragmentToSubstraitPlan::shouldAppendValuesNode(
    const PlanNodePtr& sourceNode) const {
  return !std::dynamic_pointer_cast<const core::ValuesNode>(sourceNode) &&
         !std::dynamic_pointer_cast<const core::AbstractJoinNode>(sourceNode);
}

}  // namespace facebook::velox::substrait
