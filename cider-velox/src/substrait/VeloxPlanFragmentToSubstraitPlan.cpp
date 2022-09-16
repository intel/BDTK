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

  // Append values node for the case that source node is not valuesNode.
  if (!std::dynamic_pointer_cast<const core::ValuesNode>(sourceNode)) {
    auto valueNode = addValuesNodeAsSource(sourceNode);
    planBuilder_->addNode([&](std::string id, core::PlanNodePtr input) {
      return std::make_shared<core::ValuesNode>(valueNode->id(), valueNode->values());
    });
  }

  // For the case sourceNode == targetNode, we can directly push the single node
  // into planNodeList.
  // Push source node into planNodeList.
  planNodeList.emplace_back(sourceNode);

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
    } else {
      VELOX_UNSUPPORTED("Unsupported node '{}'", riter->get()->name());
    }
  }
}

std::shared_ptr<const ValuesNode> VeloxPlanFragmentToSubstraitPlan::addValuesNodeAsSource(
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

}  // namespace facebook::velox::substrait
