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

#include "CiderPlanValidator.h"
#include "SingleNodeValidator.h"
#include "cider/CiderException.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "util/Logger.h"

namespace validator {

bool CiderPlanValidator::isSupported(const substrait::Plan& plan,
                                     const std::string& from_platform) {
  // Assumed that plan pattern already validated, do function look up first
  // Apply specific rule check on the plan
  return isSupportedSlice(constructPlanSlice(plan, from_platform), from_platform);
}

PlanSlice CiderPlanValidator::constructPlanSlice(const substrait::Plan& plan,
                                                 const std::string& from_platform) {
  const substrait::Rel& root_rel = getRootRel(plan);
  std::vector<substrait::Rel> rel_vec;
  putRelNodesInVec(root_rel, rel_vec);
  return PlanSlice{rel_vec, from_platform};
}

bool CiderPlanValidator::isSupportedSlice(const PlanSlice& plan_slice,
                                          const std::string& from_platform) {
  // TODO: (yma11) add function look up and rule based check
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

substrait::Rel CiderPlanValidator::getRootRel(const substrait::Plan& plan) {
  if (plan.relations_size() == 0) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  if (!plan.relations(0).has_root()) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  return plan.relations(0).root().input();
}

PlanSlice CiderPlanValidator::getCiderSupportedSlice(substrait::Plan& plan,
                                                     std::string& from_platform) {
  substrait::Rel root = getRootRel(plan);
  // Put plan rels in a vector with pair <substrait::Rel, isSupported>
  std::vector<substrait::Rel> rel_vec;
  putRelNodesInVec(root, rel_vec);
  // Compose plan slices based on SingleNodeValidator validate result
  std::vector<PlanSlice> slice_candidates;
  for (int i = 0; i < rel_vec.size(); i++) {
    if (!SingleNodeValidator::validate(rel_vec[i])) {
      continue;
    }
    PlanSlice plan_slice{};
    plan_slice.rel_nodes.emplace_back(rel_vec[i]);
    plan_slice.from_platform = from_platform;
    int j = i + 1;
    while (j < rel_vec.size() && SingleNodeValidator::validate(rel_vec[j])) {
      plan_slice.rel_nodes.emplace_back(rel_vec[j]);
      j++;
    }
    slice_candidates.emplace_back(plan_slice);
    i = j;
    continue;
  }
  // TODO: (yma11) validate the pattern on plan slices
  // Call isSupportedSlice() to do further validation on plan slices
  // return the longest plan slice
  int final_slice_index = 0, max_len = 0;
  for (int i = 0; i < slice_candidates.size(); i++) {
    if (slice_candidates[i].rel_nodes.size() > max_len) {
      max_len = slice_candidates[i].rel_nodes.size();
      final_slice_index = i;
    }
  }
  return slice_candidates[final_slice_index];
}
}  // namespace validator
