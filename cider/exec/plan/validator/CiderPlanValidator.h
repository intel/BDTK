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

#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"

namespace validator {

struct PlanSlice {
  std::vector<substrait::Rel> rel_nodes;
  const generator::FrontendEngine& frontend_engine;
};

class CiderPlanValidator {
 public:
  /**
   * @param plan plan with pattern already validated
   * @param frontend_engine specific frontend framework for future function lookup
   * @return whether the plan can be executed by Cider
   **/
  static bool validate(const substrait::Plan& plan,
                       const generator::FrontendEngine& frontend_engine);

  /**
   * @param plan original plan without any validation
   * @param frontend_engine specific frontend framework for future function lookup
   * @return the longest plan slice that can be executed by Cider
   **/
  static PlanSlice getCiderSupportedSlice(
      substrait::Plan& plan,
      const generator::FrontendEngine& frontend_engine);

 private:
  // Validate plan slice by function lookup and rule based check
  static bool isSupportedSlice(const PlanSlice& plan_slice,
                               const substrait::Plan& plan,
                               const generator::FrontendEngine& frontend_engine);

  static const substrait::Rel& getRootRel(const substrait::Plan& plan);

  static PlanSlice constructPlanSlice(const substrait::Plan& plan,
                                      const generator::FrontendEngine& frontend_engine);

  static void putRelNodesInVec(const substrait::Rel& rel_node,
                               std::vector<substrait::Rel>& rel_vec);

  static substrait::Plan constructSubstraitPlan(const PlanSlice& plan_slice,
                                                const substrait::Plan& plan);

  static substrait::Rel constructReadRel(const std::vector<substrait::Type>& types,
                                         google::protobuf::Arena& arena);
};
}  // namespace validator
