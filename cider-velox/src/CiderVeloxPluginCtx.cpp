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

#include "CiderVeloxPluginCtx.h"
#include "CiderPlanNodeTranslator.h"
#include "exec/plan/parser/ConverterHelper.h"

namespace facebook::velox::plugin {
void facebook::velox::plugin::CiderVeloxPluginCtx::init() {
  registerTranslator();
  registerVeloxExtensionFunction();
  ciderTransformerFactory_.registerCiderPattern();
}

VeloxPlanNodePtr CiderVeloxPluginCtx::transformVeloxPlan(VeloxPlanNodePtr originalPlan) {
  auto transformer =
      CiderVeloxPluginCtx::ciderTransformerFactory_.getTransformer(originalPlan);
  return transformer->transform();
}

void CiderVeloxPluginCtx::registerTranslator() {
  exec::Operator::registerOperator(std::make_unique<CiderPlanNodeTranslator>());
}

void CiderVeloxPluginCtx::registerVeloxExtensionFunction() {
  generator::registerExtensionFunctions();
}
}  // namespace facebook::velox::plugin
