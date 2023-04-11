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

#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

struct TpchPlan {
  core::PlanNodePtr plan;
  std::unordered_set<core::PlanNodeId> scanNodes;
};

class TpchInMemBuilder {
 public:
  /// Get the query plan for a given TPC-H query number.
  /// @param queryId TPC-H query number
  /// @param scaleFactor TPC-H data scale factor
  TpchPlan getQueryPlan(int queryId, double scaleFactor) const;

  TpchPlan getWarmupPlan(double scaleFactor) const;

 private:
  TpchPlan getQ1Plan(double scaleFactor) const;
  TpchPlan getQ3Plan(double scaleFactor) const;
  TpchPlan getQ5Plan(double scaleFactor) const;
  TpchPlan getQ6Plan(double scaleFactor) const;
  TpchPlan getQ7Plan(double scaleFactor) const;
  TpchPlan getQ8Plan(double scaleFactor) const;
  TpchPlan getQ9Plan(double scaleFactor) const;
  TpchPlan getQ10Plan(double scaleFactor) const;
  TpchPlan getQ12Plan(double scaleFactor) const;
  TpchPlan getQ13Plan(double scaleFactor) const;
  TpchPlan getQ14Plan(double scaleFactor) const;
  TpchPlan getQ15Plan(double scaleFactor) const;
  TpchPlan getQ16Plan(double scaleFactor) const;
  TpchPlan getQ18Plan(double scaleFactor) const;
  TpchPlan getQ19Plan(double scaleFactor) const;
  TpchPlan getQ22Plan(double scaleFactor) const;
  TpchPlan getQ23Plan(double scaleFactor) const;

  std::unique_ptr<memory::ScopedMemoryPool> pool_ = memory::getDefaultScopedMemoryPool();
};

}  // namespace facebook::velox::exec::test
