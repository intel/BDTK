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

#include "CiderPlanTransformerFactory.h"

#include "CiderPlanPatterns.h"
#include "CiderPlanRewriter.h"
#include "planTransformer/PlanTransformer.h"

namespace facebook::velox::plugin::plantransformer {
CiderPlanTransformerFactory::CiderPlanTransformerFactory() {
  ciderTransformerFactory_ = PlanTransformerFactory()
                                 .registerPattern(std::make_shared<CompoundPattern>(),
                                                  std::make_shared<CiderPlanRewriter>())
                                 .registerPattern(std::make_shared<LeftDeepJoinPattern>(),
                                                  std::make_shared<CiderPlanRewriter>())
                                 .registerPattern(std::make_shared<FilterPattern>(),
                                                  std::make_shared<CiderPlanRewriter>());
}

std::shared_ptr<PlanTransformer> CiderPlanTransformerFactory::getTransformer(
    VeloxPlanNodePtr root) {
  return ciderTransformerFactory_.getTransformer(root);
}
}  // namespace facebook::velox::plugin::plantransformer
