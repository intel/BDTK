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

#include <memory>
#include <string>
#include "CiderVeloxPluginCtx.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::plugin::test {

class CiderPlanBuilder {
 public:
  CiderPlanBuilder() : planBuilder_{PlanBuilder()} {};

  CiderPlanBuilder& values(const std::vector<RowVectorPtr>& batches);
  CiderPlanBuilder& filter(const std::string& filter);
  CiderPlanBuilder& project(const std::vector<std::string>& projections);
  CiderPlanBuilder& partialAggregation(const std::vector<std::string>& groupingKeys,
                                       const std::vector<std::string>& aggregates,
                                       const std::vector<std::string>& masks = {});
  CiderPlanBuilder& finalAggregation();
  const VeloxPlanNodePtr planNode();

 private:
  PlanBuilder planBuilder_;
};

}  // namespace facebook::velox::plugin::test
