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

#include "CiderVeloxPluginCtx.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;

using facebook::velox::test::BatchMaker;

class CiderOperatorTestBase : public OperatorTestBase {
 protected:
  void SetUp() override {
    FLAGS_left_deep_join_pattern = true;
    FLAGS_partial_agg_pattern = true;
    // TODO: Enable this after feature fully supported. So that we could enable all
    // supported patterns in our tests.
    // FLAGS_top_n_pattern = true;
    // FLAGS_order_by_pattern = true;
    CiderVeloxPluginCtx::init();
  }

 protected:
  const std::vector<RowVectorPtr> generateTestBatch(RowTypePtr& rowType,
                                                    bool withNull = false) {
    std::vector<RowVectorPtr> batches;
    for (int32_t i = 0; i < 10; ++i) {
      auto batch = std::dynamic_pointer_cast<RowVector>(BatchMaker::createBatch(
          rowType, 100, *pool_, withNull ? randomNulls(7) : nullptr));
      batches.push_back(batch);
    }
    createDuckDbTable(batches);
    return batches;
  }

  void verify(const VeloxPlanNodePtr& ciderPlan,
              const std::string& referenceQuery,
              int numThreads = 1) {
    CursorParameters params;
    params.planNode = ciderPlan;
    params.maxDrivers = numThreads;

    assertQuery(params, referenceQuery);
  }

 private:
  std::function<bool(vector_size_t /*index*/)> randomNulls(int32_t n) {
    return [n](vector_size_t /*index*/) { return folly::Random::rand32() % n == 0; };
  }
};
