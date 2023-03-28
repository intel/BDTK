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

#include "BatchDataGenerator.h"
#include "CiderVeloxPluginCtx.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/type/Type.h"

class CiderOperatorTestBase : public facebook::velox::exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    FLAGS_left_deep_join_pattern = true;
    // FLAGS_partial_agg_pattern = true;
    FLAGS_compound_pattern = true;
    // TODO: Enable this after feature fully supported. So that we could enable all
    // supported patterns in our tests.
    // FLAGS_top_n_pattern = true;
    // FLAGS_order_by_pattern = true;
    facebook::velox::plugin::CiderVeloxPluginCtx::init();
  }

 protected:
  auto generateTestBatch(facebook::velox::RowTypePtr& rowType, bool withNull) {
    auto batches = generator_.generate(rowType, 10, 100, withNull);
    createDuckDbTable(batches);
    return batches;
  }

  void verify(const facebook::velox::plugin::VeloxPlanNodePtr& ciderPlan,
              const std::string& referenceQuery,
              int numThreads = 1) {
    facebook::velox::exec::test::CursorParameters params;
    params.planNode = ciderPlan;
    params.maxDrivers = numThreads;

    assertQuery(params, referenceQuery);
  }

  cider::transformer::test::util::BatchDataGenerator generator_{pool_.get()};
};
