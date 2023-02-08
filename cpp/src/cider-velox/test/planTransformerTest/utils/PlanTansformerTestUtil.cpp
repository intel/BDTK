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
bool PlanTansformerTestUtil::comparePlanSequence(VeloxPlanNodePtr first,
                                                 VeloxPlanNodePtr second) {
  if (first == nullptr && second == nullptr) {
    return true;
  } else if (!(first != nullptr && second != nullptr)) {
    return false;
  }
  bool isRootEqual = typeid(*first) == typeid(*second);
  if (isRootEqual) {
    auto firstSrc = first->sources();
    auto secondSrc = second->sources();
    if (!firstSrc.empty() && !secondSrc.empty()) {
      auto firstSrcCount = firstSrc.size();
      auto secondSrcCount = secondSrc.size();
      if (firstSrcCount == secondSrcCount) {
        for (int idx = 0; idx < firstSrcCount; idx++) {
          if (firstSrc[idx] != nullptr && secondSrc[idx] != nullptr) {
            bool isSrcEqual = comparePlanSequence(firstSrc[idx], secondSrc[idx]);
            if (!isSrcEqual) {
              return false;
            }
          } else if (!(firstSrc[idx] == nullptr && secondSrc[idx] == nullptr)) {
            return false;
          }
        }
        return true;
      }
    } else if (firstSrc.empty() && secondSrc.empty()) {
      return true;
    }
  }
  return false;
}
}  // namespace facebook::velox::plugin::plantransformer::test
