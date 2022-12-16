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

#include <gtest/gtest.h>

#include "../../CiderOperatorTestBase.h"
#include "planTransformer/PlanNodeAddr.h"
#include "planTransformer/PlanTransformer.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::plugin::plantransformer::test {

class PlanTransformerTestBase : public CiderOperatorTestBase {
 public:
  bool compareWithExpected(VeloxPlanNodePtr result, VeloxPlanNodePtr expected);
  std::shared_ptr<PlanTransformer> getTransformer(VeloxPlanNodePtr root);
  void setTransformerFactory(PlanTransformerFactory& transformerFactory) {
    transformerFactory_ = transformerFactory;
  }
  std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};

  VeloxPlanNodePtr getCiderExpectedPtr(RowTypePtr rowType,
                                       VeloxPlanNodeVec joinSrcVec = {});

  VeloxPlanNodePtr getSingleFilterNode(RowTypePtr rowType, const std::string filter);

  VeloxPlanNodePtr getSingleProjectNode(RowTypePtr rowType,
                                        const std::vector<std::string> projections);

 private:
  PlanTransformerFactory transformerFactory_;
  RowTypePtr rowType_;
};
}  // namespace facebook::velox::plugin::plantransformer::test
