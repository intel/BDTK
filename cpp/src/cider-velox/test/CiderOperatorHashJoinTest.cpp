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

#include <folly/init/Init.h>
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "CiderOperatorTestBase.h"
#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/parse/PlanNodeIdGenerator.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class CiderOperatorHashJoinTest : public CiderOperatorTestBase {
 protected:
  static std::vector<std::string> makeKeyNames(int cnt, const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  static RowTypePtr makeRowType(const std::vector<TypePtr>& keyTypes,
                                const std::string& namePrefix) {
    std::vector<std::string> names = makeKeyNames(keyTypes.size(), namePrefix);
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static std::vector<std::string> concat(const std::vector<std::string>& a,
                                         const std::vector<std::string>& b) {
    std::vector<std::string> result;
    result.insert(result.end(), a.begin(), a.end());
    result.insert(result.end(), b.begin(), b.end());
    return result;
  }

  void testJoin(const std::vector<TypePtr>& keyTypes,
                int32_t leftSize,
                int32_t rightSize,
                const std::string& referenceQuery,
                const std::string& filter = "") {
    auto leftType = makeRowType(keyTypes, "t_");
    auto rightType = makeRowType(keyTypes, "u_");

    auto leftBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, leftSize, *pool_));
    auto rightBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, rightSize, *pool_));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    auto planNode = PlanBuilder(planNodeIdGenerator)
                        .values({leftBatch})
                        .hashJoin(makeKeyNames(keyTypes.size(), "t_"),
                                  makeKeyNames(keyTypes.size(), "u_"),
                                  PlanBuilder(planNodeIdGenerator)
                                      .values({rightBatch})
                                      .project(rightType->names())
                                      .planNode(),
                                  filter,
                                  concat(leftType->names(), rightType->names()))
                        .planNode();

    createDuckDbTable("t", {leftBatch});
    createDuckDbTable("u", {rightBatch});

    assertPlanConversion(planNode, referenceQuery);
  }

  void assertPlanConversion(const std::shared_ptr<const core::PlanNode>& plan,
                            const std::string& duckDbSql) {
    auto ciderPlan = plugin::CiderVeloxPluginCtx::transformVeloxPlan(plan);
    assertQuery(ciderPlan, duckDbSql);
  }

  std::shared_ptr<VeloxToSubstraitPlanConvertor> veloxConvertor_ =
      std::make_shared<VeloxToSubstraitPlanConvertor>();

  std::shared_ptr<SubstraitVeloxPlanConverter> substraitConverter_ =
      std::make_shared<SubstraitVeloxPlanConverter>(pool_.get());
};

TEST_F(CiderOperatorHashJoinTest, innerJoin_allTypes) {
  std::vector<TypePtr> types{BIGINT(), INTEGER()};
  for (const auto& type : types) {
    testJoin({type, type},
             200,
             150,
             "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM"
             " t join u on t_k0 = u_k0 AND t_k1 = u_k1");
  }
}

TEST_F(CiderOperatorHashJoinTest, innerJoin_normalizedKey) {
  testJoin({INTEGER(), INTEGER()},
           16000,
           15000,
           "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM "
           " t join u on t_k0 = u_k0 AND t_k1 = u_k1");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
