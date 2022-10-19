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

#include <folly/init/Init.h>
#include <google/protobuf/util/json_util.h>
#include <boost/filesystem.hpp>

#include "CiderPlanNodeTranslator.h"
#include "CiderVeloxPluginCtx.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using facebook::velox::test::BatchMaker;

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::plugin;
using namespace facebook::velox::exec::test;

std::string getSubstraitJsonFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/SubstraitPlanFiles/";
}

::substrait::Plan generateSubstraitPlan(std::string file_name) {
  std::ifstream sub_json(getSubstraitJsonFilesPath() + file_name);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  std::string sub_data = buffer.str();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(sub_data, &sub_plan);
  return sub_plan;
}

class CiderOperatorJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    CiderVeloxPluginCtx::init();
    HiveConnectorTestBase::SetUp();
  }

  RowVectorPtr makeSimpleRowVector(vector_size_t size) {
    return makeRowVector({makeFlatVector<int64_t>(size, [](auto row) { return row; })});
  }

  void testJoin(int32_t numThreads,
                const std::vector<RowVectorPtr>& leftBatch,
                const std::vector<RowVectorPtr>& rightBatch,
                const std::string& referenceQuery) {
    CursorParameters params;
    auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

    // TODO: replace with velox->substrait plan converter.
    auto substraitPlan = generateSubstraitPlan("hash_join_basic.json");

    auto leftNode = PlanBuilder(planNodeIdGenerator).values({leftBatch}, true).planNode();
    auto rightNode = PlanBuilder(planNodeIdGenerator)
                         .values({rightBatch}, true)
                         .project({"c0 AS u_c0"})
                         .planNode();
    std::shared_ptr<const RowType> outputType{ROW({"c0", "u_c0"}, {BIGINT(), BIGINT()})};

    auto joinNodeFun = [leftNode, rightNode, substraitPlan, outputType](
                           std::string id, const core::PlanNodePtr& /* input */) {
      return std::make_shared<CiderPlanNode>(
          id, std::move(leftNode), std::move(rightNode), outputType, substraitPlan);
    };

    params.planNode = PlanBuilder(planNodeIdGenerator).addNode(joinNodeFun).planNode();
    params.maxDrivers = numThreads;

    createDuckDbTable("t", {leftBatch});
    createDuckDbTable("u", {rightBatch});
    OperatorTestBase::assertQuery(params, referenceQuery);
  }
};

TEST_F(CiderOperatorJoinTest, basic) {
  auto leftBatch = {makeSimpleRowVector(10)};
  auto rightBatch = {makeSimpleRowVector(10)};
  testJoin(1, leftBatch, rightBatch, "SELECT t.c0, u.c0 FROM t JOIN u ON t.c0 = u.c0");
}

TEST_F(CiderOperatorJoinTest, multiBuild) {
  auto leftBatch = {makeSimpleRowVector(10)};
  auto rightBatch = {makeSimpleRowVector(20), makeSimpleRowVector(30)};
  testJoin(1, leftBatch, rightBatch, "SELECT t.c0, u.c0 FROM t JOIN u ON t.c0 = u.c0");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
