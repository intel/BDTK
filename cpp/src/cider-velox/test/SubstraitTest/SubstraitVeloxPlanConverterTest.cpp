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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class SubstraitVeloxPlanConverterTest : public OperatorTestBase {
 protected:
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3"}, {INTEGER(), INTEGER(), INTEGER(), INTEGER()})};

  /*!
   *
   * @param size  the number of RowVectorPtr.
   * @param childSize the size of rowType_, the number of columns
   * @param batchSize batch size.
   * @return std::vector<RowVectorPtr>
   */
  std::vector<RowVectorPtr> makeVector(int64_t size,
                                       int64_t childSize,
                                       int64_t batchSize) {
    std::vector<RowVectorPtr> vectors;
    std::mt19937 gen(std::mt19937::default_seed);
    for (int i = 0; i < size; i++) {
      std::vector<VectorPtr> children;
      for (int j = 0; j < childSize; j++) {
        children.emplace_back(makeFlatVector<int32_t>(
            batchSize,
            [&](auto /*row*/) {
              return folly::Random::rand32(INT32_MAX / 4, INT32_MAX / 2, gen);
            },
            nullEvery(2)));
      }

      vectors.push_back(makeRowVector({children}));
    }

    return vectors;
  }

  void assertFilter(std::vector<RowVectorPtr>&& vectors,
                    const std::string& filter =
                        "(c2 < 1000) and (c1 between 0.6 and 1.6) and (c0 >= 100)") {
    auto vPlan = PlanBuilder().values(vectors).filter(filter).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp WHERE " + filter);
    google::protobuf::Arena arena;
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
  }

  void assertRead(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder().values(vectors).planNode();

    assertQuery(vPlan, "SELECT * FROM tmp");
    google::protobuf::Arena arena;
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
  }

  void assertProject(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .project(std::vector<std::string>{"c0 + c1", "c1-c2"})
                     .planNode();

    assertQuery(vPlan, "SELECT  c0 + c1, c1 - c2 FROM tmp");

    // Convert Velox Plan to Substrait Plan.
    google::protobuf::Arena arena;
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
  }

  void assertFilterProjectFused(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan =
        PlanBuilder()
            .values(vectors)
            .project(std::vector<std::string>{"c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
            .filter("e1 > 13")
            .planNode();

    assertQuery(
        vPlan,
        "SELECT c0, c1 ,c0 % 100 + c1 % 50 AS e1 FROM tmp where c0 % 100 + c1 % 50 >13");

    google::protobuf::Arena arena;
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
  }

  void assertFilterProject(std::vector<RowVectorPtr>&& vectors) {
    auto vPlan = PlanBuilder()
                     .values(vectors)
                     .filter("c1 > 0")
                     .project(std::vector<std::string>{"c0", "c1", "c0 + c1"})
                     .planNode();

    assertQuery(vPlan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 > 0");
    google::protobuf::Arena arena;
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
  }

  void SetUp() override {
    v2SPlanConvertor_ = new VeloxToSubstraitPlanConvertor();
    // sPlan_ = new ::substrait::Plan();
  }

  void TearDown() override {
    delete v2SPlanConvertor_;
    // delete sPlan_;
  }

  VeloxToSubstraitPlanConvertor* v2SPlanConvertor_;
  ::substrait::Plan* sPlan_;
};

TEST_F(SubstraitVeloxPlanConverterTest, projectNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertProject(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, filterNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertFilter(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, valuesNode) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);
  assertRead(std::move(vectors));
}

TEST_F(SubstraitVeloxPlanConverterTest, filterProject) {
  std::vector<RowVectorPtr> vectors;
  vectors = makeVector(3, 4, 2);
  createDuckDbTable(vectors);

  assertFilterProject(std::move(vectors));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
