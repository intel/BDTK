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

#ifndef CIDER_SUBSTRAIT_PLAN_H
#define CIDER_SUBSTRAIT_PLAN_H

#include "substrait/plan.pb.h"

namespace cider::plan {

/// A utility class for substrait plan by wrapped the substrait plan and provide some
/// useful methods for batch processor.
class SubstraitPlan {
 public:
  SubstraitPlan(const ::substrait::Plan& plan);

  bool hasAggregateRel() const;

  bool hasJoinRel() const;

  const substrait::Plan& getPlan() const { return plan_; }

  const ::substrait::JoinRel& joinRel();

 private:
  ::substrait::Plan plan_;
};

using SubstraitPlanPtr = std::shared_ptr<const SubstraitPlan>;

}  // namespace cider::plan

#endif  // CIDER_SUBSTRAIT_PLAN_H
