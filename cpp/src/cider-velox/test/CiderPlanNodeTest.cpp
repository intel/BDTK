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

#include <memory>

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include "CiderPlanNode.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::plugin;
using namespace facebook::velox::substrait;

using facebook::velox::test::BatchMaker;

class CiderPlanNodeTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"l_quantity", "l_extendedprice", "l_discount", "l_shipdate"},
          {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()})};
};

TEST_F(CiderPlanNodeTest, filter) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  const std::string filter =
      "l_shipdate >= 8765.666666666667 and l_shipdate < 9130.666666666667 and "
      "l_discount between 0.05 and 0.07 and l_quantity < 24.0";
  auto veloxPlan = PlanBuilder().values(vectors).filter(filter).planNode();

  std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
      std::make_shared<VeloxToSubstraitPlanConvertor>();
  google::protobuf::Arena arena;
  auto plan = std::make_shared<::substrait::Plan>(
      v2SPlanConvertor->toSubstrait(arena, veloxPlan));
  auto source = PlanBuilder().values(vectors).planNode();
  auto ciderPlanNode =
      std::make_shared<CiderPlanNode>("100", source, veloxPlan->outputType(), *plan);
}

TEST_F(CiderPlanNodeTest, project) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .project({"l_extendedprice * l_discount as revenue"})
                       .planNode();

  std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
      std::make_shared<VeloxToSubstraitPlanConvertor>();

  google::protobuf::Arena arena;
  auto plan = std::make_shared<::substrait::Plan>(
      v2SPlanConvertor->toSubstrait(arena, veloxPlan));
  auto source = PlanBuilder().values(vectors).planNode();
  auto ciderPlanNode =
      std::make_shared<CiderPlanNode>("100", source, veloxPlan->outputType(), *plan);

  std::cout << ciderPlanNode->toString() << std::endl;
  std::cout << veloxPlan->toString();
}

#if 0
TEST_F(CiderPlanNodeTest, Q6) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType_, 100, *pool_));
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  auto veloxPlan = PlanBuilder()
                       .values(vectors)
                       .filter(
                           "l_shipdate >= 8765.666666666667 and l_shipdate < "
                           "9130.666666666667 and l_discount between 0.05 and "
                           "0.07 and l_quantity < 24.0")
                       .project({"l_extendedprice * l_discount as revenue"})
                       .aggregation(
                           {},
                           {"sum(revenue)"},
                           {},
                           core::AggregationNode::Step::kPartial,
                           false)
                       .planNode();

  std::shared_ptr<VeloxToSubstraitPlanConvertor> v2SPlanConvertor =
      std::make_shared<VeloxToSubstraitPlanConvertor>();
  std::shared_ptr<::substrait::Plan> plan =
      std::make_shared<::substrait::Plan>();
  v2SPlanConvertor->veloxToSubstraitIR(veloxPlan, *plan);

  auto source = PlanBuilder().values(vectors).planNode();

  auto ciderPlanNode = std::make_shared<CiderPlanNode>(
      "100", source, veloxPlan->outputType(), *plan);
  std::cout << ciderPlanNode->toString(true, true);
}
#endif
