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

#include "CiderPlanBuilder.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;

namespace facebook::velox::plugin::test {

CiderPlanBuilder& CiderPlanBuilder::values(const std::vector<RowVectorPtr>& batches) {
  planBuilder_.values(batches);
  return *this;
}

CiderPlanBuilder& CiderPlanBuilder::filter(const std::string& filter) {
  planBuilder_.filter(filter);
  return *this;
}

CiderPlanBuilder& CiderPlanBuilder::project(const std::vector<std::string>& projections) {
  planBuilder_.project(projections);
  return *this;
}

CiderPlanBuilder& CiderPlanBuilder::partialAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks) {
  planBuilder_.partialAggregation(groupingKeys, aggregates, masks);
  return *this;
}

const VeloxPlanNodePtr CiderPlanBuilder::planNode() {
  return CiderVeloxPluginCtx::transformVeloxPlan(planBuilder_.planNode());
}

}  // namespace facebook::velox::plugin::test
