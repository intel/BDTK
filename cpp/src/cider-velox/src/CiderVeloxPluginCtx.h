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
#include "ciderTransformer/CiderPlanTransformerFactory.h"
#include "planTransformer/PlanTransformer.h"
#include "velox/core/PlanNode.h"

namespace facebook::velox::plugin {

using VeloxPlanNodePtr = std::shared_ptr<const facebook::velox::core::PlanNode>;

class CiderVeloxPluginCtx {
 public:
  static void init();
  static void init(const std::string& conf_path);
  static VeloxPlanNodePtr transformVeloxPlan(VeloxPlanNodePtr originalPlan);

 private:
  static void registerTranslator();
  static void registerVeloxExtensionFunction();
  inline static plantransformer::CiderPlanTransformerFactory ciderTransformerFactory_;
};

}  // namespace facebook::velox::plugin
