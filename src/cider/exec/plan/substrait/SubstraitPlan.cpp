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

#include "SubstraitPlan.h"

namespace cider::plan {

SubstraitPlan::SubstraitPlan(const substrait::Plan& plan) : plan_(plan) {}

bool SubstraitPlan::hasAggregateRel() const {
  for (auto& rel : plan_.relations()) {
    if (rel.has_root() && rel.root().has_input()) {
      if (rel.root().input().has_aggregate()) {
        return true;
      }
    }
  }
  return false;
}

bool SubstraitPlan::hasJoinRel() const {
  for (auto& rel : plan_.relations()) {
    if (rel.has_root() && rel.root().has_input()) {
      if (rel.root().input().has_join()) {
        return true;
      }
    }
  }
  return false;
}

const std::optional<std::shared_ptr<::substrait::JoinRel>> SubstraitPlan::getJoinRel() {
  if (hasJoinRel()) {
    return std::make_shared<::substrait::JoinRel>(
        plan_.relations(0).root().input().join());
  }
  return std::nullopt;
}

}  // namespace cider::plan
