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

#include "PlanTansformerTestUtil.h"

#include <gtest/gtest.h>

namespace facebook::velox::plugin::plantransformer::test {
bool PlanTansformerTestUtil::comparePlanSequence(VeloxPlanNodePtr plan1,
                                                 VeloxPlanNodePtr plan2) {
  if (plan1 == nullptr && plan2 == nullptr) {
    return true;
  } else if (plan1 == nullptr || plan2 == nullptr) {
    return false;
  }
  if (typeid(plan1.get()) != typeid(plan2.get())) {
    return false;
  }
  auto size1 = plan1->sources().size();
  auto size2 = plan2->sources().size();
  if (size1 != size2) {
    return false;
  }
  for (size_t idx = 0; idx < size1; ++idx) {
    if (!comparePlanSequence(plan1->sources().at(idx), plan2->sources().at(idx))) {
      return false;
    }
  }
  return true;
}
}  // namespace facebook::velox::plugin::plantransformer::test
