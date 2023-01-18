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

#include "CiderPlanValidator.h"
#include "SingleNodeValidator.h"
#include "cider/CiderException.h"
#include "cider/CiderSupportPlatType.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "util/Logger.h"

namespace validator {

bool CiderPlanValidator::validate(const substrait::Plan& plan,
                                  const PlatformType& from_platform) {
  const substrait::Rel& root = getRootRel(plan);
  // Put plan rels in a vector with pair <substrait::Rel, isSupported>
  std::vector<substrait::Rel> rel_vec;
  putRelNodesInVec(root, rel_vec);
  // Get functions from plan extension uri
  auto function_map = generator::getFunctionMap(plan);
  auto function_lookup_ptr = FunctionLookupEngine::getInstance(from_platform);
  int i = 0;
  while (i < rel_vec.size() &&
         SingleNodeValidator::validate(rel_vec[i], function_map, function_lookup_ptr)) {
    i++;
  }
  return i == rel_vec.size();
}

PlanSlice CiderPlanValidator::constructPlanSlice(const substrait::Plan& plan,
                                                 const PlatformType& from_platform) {
  const substrait::Rel& root_rel = getRootRel(plan);
  std::vector<substrait::Rel> rel_vec;
  putRelNodesInVec(root_rel, rel_vec);
  return PlanSlice{rel_vec, from_platform};
}

bool CiderPlanValidator::isSupportedSlice(const PlanSlice& plan_slice,
                                          const substrait::Plan& plan,
                                          const PlatformType& from_platform) {
  try {
    auto c_plan = constructSubstraitPlan(plan_slice, plan);
    auto translator = std::make_shared<generator::SubstraitToRelAlgExecutionUnit>(c_plan);
    translator->createRelAlgExecutionUnit();
  } catch (const CiderCompileException& e) {
    return false;
  }
  return true;
}

void CiderPlanValidator::putRelNodesInVec(const substrait::Rel& rel_node,
                                          std::vector<substrait::Rel>& rel_vec) {
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kRead: {
      rel_vec.push_back(rel_node);
      break;
    }
    case substrait::Rel::RelTypeCase::kFilter: {
      rel_vec.push_back(rel_node);
      putRelNodesInVec(rel_node.filter().input(), rel_vec);
      break;
    }
    case substrait::Rel::RelTypeCase::kProject: {
      rel_vec.push_back(rel_node);
      putRelNodesInVec(rel_node.project().input(), rel_vec);
      break;
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      rel_vec.push_back(rel_node);
      putRelNodesInVec(rel_node.aggregate().input(), rel_vec);
      break;
    }
    case substrait::Rel::RelTypeCase::kJoin: {
      rel_vec.push_back(rel_node);
      putRelNodesInVec(rel_node.join().left(), rel_vec);
      break;
    }
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported substrait rel type {}", rel_type));
  }
}

substrait::Plan CiderPlanValidator::constructSubstraitPlan(const PlanSlice& plan_slice,
                                                           const substrait::Plan& plan) {
  google::protobuf::Arena arena;
  substrait::Plan* substrait_plan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);
  substrait::RelRoot* root_rel = substrait_plan->add_relations()->mutable_root();
  substrait::Rel* rel = root_rel->mutable_input();
  int output_size =
      SingleNodeValidator::getRelOutputTypes(plan_slice.rel_nodes[0]).size();
  // Faked names added for rel root
  for (int i = 0; i < output_size; i++) {
    root_rel->add_names("output_" + std::to_string(i));
  }
  for (int i = 0; i < plan_slice.rel_nodes.size(); i++) {
    rel->CopyFrom(plan_slice.rel_nodes[i]);
    switch (plan_slice.rel_nodes[i].rel_type_case()) {
      case substrait::Rel::RelTypeCase::kFilter: {
        rel = rel->mutable_filter()->mutable_input();
        if (i == plan_slice.rel_nodes.size() - 1) {
          rel->CopyFrom(constructReadRel(SingleNodeValidator::getRelOutputTypes(
                                             plan_slice.rel_nodes[i].filter().input()),
                                         arena));
        }
        break;
      }
      case substrait::Rel::RelTypeCase::kProject: {
        rel = rel->mutable_project()->mutable_input();
        if (i == plan_slice.rel_nodes.size() - 1) {
          rel->CopyFrom(constructReadRel(SingleNodeValidator::getRelOutputTypes(
                                             plan_slice.rel_nodes[i].project().input()),
                                         arena));
        }
        break;
      }
      case substrait::Rel::RelTypeCase::kAggregate: {
        rel = rel->mutable_aggregate()->mutable_input();
        if (i == plan_slice.rel_nodes.size() - 1) {
          rel->CopyFrom(constructReadRel(SingleNodeValidator::getRelOutputTypes(
                                             plan_slice.rel_nodes[i].aggregate().input()),
                                         arena));
        }
        break;
      }
      case substrait::Rel::RelTypeCase::kJoin: {
        rel = rel->mutable_join()->mutable_left();
        if (i == plan_slice.rel_nodes.size() - 1) {
          // add ReadRel for both right and left of it's the last node in PlanSlice
          rel->CopyFrom(constructReadRel(SingleNodeValidator::getRelOutputTypes(
                                             plan_slice.rel_nodes[i].join().left()),
                                         arena));
        }
        auto rel_right = rel->mutable_join()->mutable_right();
        if (plan_slice.rel_nodes[i].join().right().has_read()) {
          rel_right->CopyFrom(plan_slice.rel_nodes[i].join().right());
        } else {
          rel_right->CopyFrom(
              constructReadRel(SingleNodeValidator::getRelOutputTypes(
                                   plan_slice.rel_nodes[i].join().right()),
                               arena));
        }
        break;
      }
    }
  }
  auto extensions = substrait_plan->mutable_extensions();
  extensions->CopyFrom(plan.extensions());
  return *substrait_plan;
}

substrait::Rel CiderPlanValidator::constructReadRel(
    const std::vector<substrait::Type>& types,
    google::protobuf::Arena& arena) {
  substrait::Rel rel;
  auto read_rel = rel.mutable_read();
  substrait::RelCommon* rel_common = read_rel->mutable_common();
  auto rel_common_direct = rel_common->mutable_direct();
  rel_common_direct =
      google::protobuf::Arena::CreateMessage<::substrait::RelCommon_Direct>(&arena);
  substrait::NamedStruct* named_struct = read_rel->mutable_base_schema();

  auto mutable_struct = named_struct->mutable_struct_();
  for (int i = 0; i < types.size(); i++) {
    named_struct->add_names("input_" + std::to_string(i));
    mutable_struct->add_types()->CopyFrom(types[i]);
  }
  mutable_struct->set_type_variation_reference(0);
  mutable_struct->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  return rel;
}

const substrait::Rel& CiderPlanValidator::getRootRel(const substrait::Plan& plan) {
  if (plan.relations_size() == 0) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  if (!plan.relations(0).has_root()) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  return plan.relations(0).root().input();
}

std::vector<PlanSlice> CiderPlanValidator::getCiderSupportedSlice(
    substrait::Plan& plan,
    const PlatformType& from_platform) {
  const substrait::Rel& root = getRootRel(plan);
  // Put plan rels in a vector with pair <substrait::Rel, isSupported>
  std::vector<substrait::Rel> rel_vec;
  putRelNodesInVec(root, rel_vec);
  // Get functions from plan extension uri
  auto function_map = generator::getFunctionMap(plan);
  auto function_lookup_ptr = FunctionLookupEngine::getInstance(from_platform);
  // Compose plan slices based on SingleNodeValidator validate result
  std::vector<PlanSlice> slice_candidates;
  bool has_join;
  for (int i = 0; i < rel_vec.size();) {
    if (!SingleNodeValidator::validate(rel_vec[i], function_map, function_lookup_ptr)) {
      i++;
      continue;
    }
    PlanSlice plan_slice{{}, from_platform};
    plan_slice.rel_nodes.emplace_back(rel_vec[i]);
    has_join = rel_vec[i].has_join() ? true : false;
    int j = i + 1;
    while (j < rel_vec.size() &&
           SingleNodeValidator::validate(rel_vec[j], function_map, function_lookup_ptr)) {
      if (has_join && rel_vec[j].has_join()) {
        // split here since don't support multi join for now
        break;
      }
      has_join = rel_vec[j].has_join() ? true : has_join;
      plan_slice.rel_nodes.emplace_back(rel_vec[j]);
      j++;
    }
    slice_candidates.emplace_back(plan_slice);
    i = j;
  }
  // TODO: (yma11) validate the pattern on plan slices
  // Call isSupportedSlice() to do further validation on plan slices
  std::vector<PlanSlice> final_candidates;
  final_candidates.reserve(slice_candidates.size());
  for (int i = 0; i < slice_candidates.size(); i++) {
    if (isSupportedSlice(slice_candidates[i], plan, from_platform)) {
      final_candidates.emplace_back(slice_candidates[i]);
    }
  }
  return slice_candidates;
}

}  // namespace validator
