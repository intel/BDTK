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

#include <folly/Random.h>
#include <folly/init/Init.h>
#include <google/protobuf/util/json_util.h>
#include <fstream>
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using facebook::velox::core::PlanNode;
using facebook::velox::exec::Aggregate;
using namespace facebook::velox::substrait;

using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec::test {
namespace {
class veloxToSubstraitAggregationTest : public OperatorTestBase {
 protected:
  void assertQueryWithSingleKey(std::shared_ptr<const PlanNode>& vPlan,
                                const std::string& keyName,
                                bool ignoreNullKeys,
                                bool distinct) {
    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(vPlan, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(vPlan,
                  "SELECT " + keyName +
                      ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), "
                      "sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), "
                      "min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), "
                      "max(c4), max(c5) " +
                      fromClause + " GROUP BY " + keyName);
    }
  }

  void assertQueryWithMultiKey(std::shared_ptr<const PlanNode>& vPlan,
                               bool ignoreNullKeys,
                               bool distinct) {
    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(vPlan, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(vPlan,
                  "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), "
                  "sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), "
                  "max(0.1), max(c3), max(c4), max(c5) " +
                      fromClause + " GROUP BY c0, c1, c6");
    }
  }

  void messageToJSONFile(const google::protobuf::Message& message,
                         const std::string& file_path) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    options.preserve_proto_field_names = true;
    std::string json_string;
    google::protobuf::util::MessageToJsonString(message, &json_string, options);
    std::ofstream out(file_path);
    out << json_string;
    out.close();
  }

  void SetUp() override { v2SPlanConvertor_ = new VeloxToSubstraitPlanConvertor(); }

  void TearDown() override { delete v2SPlanConvertor_; }

  template <typename T>
  void singleKeyTest(const std::vector<RowVectorPtr>& vectors,
                     const std::string& keyName,
                     bool ignoreNullKeys,
                     bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {"sum(15)", "sum(0.1)", "sum(c1)",  "sum(c2)", "sum(c4)",
                    "sum(c5)", "min(15)",  "min(0.1)", "min(c1)", "min(c2)",
                    "min(c3)", "min(c4)",  "min(c5)",  "max(15)", "max(0.1)",
                    "max(c1)", "max(c2)",  "max(c3)",  "max(c4)", "max(c5)"};
    }

    auto plan = PlanBuilder()
                    .values(vectors)
                    .aggregation({keyName},
                                 aggregates,
                                 {},
                                 core::AggregationNode::Step::kPartial,
                                 ignoreNullKeys)
                    .planNode();
    assertQueryWithSingleKey(plan, keyName, ignoreNullKeys, distinct);
    v2SPlanConvertor_->toSubstrait(arena, plan);
  }

  void multiKeyTest(const std::vector<RowVectorPtr>& vectors,
                    bool ignoreNullKeys,
                    bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {"sum(15)",
                    "sum(0.1)",
                    "sum(c4)",
                    "sum(c5)",
                    "min(15)",
                    "min(0.1)",
                    "min(c3)",
                    "min(c4)",
                    "min(c5)",
                    "max(15)",
                    "max(0.1)",
                    "max(c3)",
                    "max(c4)",
                    "max(c5)"};
    }
    auto plan = PlanBuilder()
                    .values(vectors)
                    .aggregation({"c0", "c1", "c6"},
                                 aggregates,
                                 {},
                                 core::AggregationNode::Step::kPartial,
                                 ignoreNullKeys)
                    .planNode();
    assertQueryWithMultiKey(plan, ignoreNullKeys, distinct);

    // Transform to substrait plan.
    v2SPlanConvertor_->toSubstrait(arena, plan);
  }

  void avgQueryTest(const std::vector<RowVectorPtr>& vectors, bool ignoreNullKeys) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .filter("c6 < 24")
                    .aggregation({"c0", "c1"},
                                 {"avg(c4) as avg_price"},
                                 {},
                                 core::AggregationNode::Step::kSingle,
                                 ignoreNullKeys)
                    .project({"avg_price"})
                    .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE c0 IS NOT NULL AND c1 IS NOT NULL ";
    }

    assertQuery(
        plan,
        "SELECT avg(c4) as avg_price " + fromClause + " WHERE c6 < 24 GROUP BY c0, c1");

    // Transform to substrait plan.
    v2SPlanConvertor_->toSubstrait(arena, plan);
  }

  void countQueryTest(const std::vector<RowVectorPtr>& vectors, bool ignoreNullKeys) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .filter("c6 < 24")
                    .aggregation({"c0", "c1"},
                                 {"count(c4) as num_price"},
                                 {},
                                 core::AggregationNode::Step::kSingle,
                                 ignoreNullKeys)
                    .project({"num_price"})
                    .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE c0 IS NOT NULL AND c1 IS NOT NULL ";
    }

    assertQuery(
        plan,
        "SELECT count(c4) as num_price " + fromClause + " WHERE c6 < 24 GROUP BY c0, c1");

    // Transform to substrait plan.
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, plan);
    messageToJSONFile(sPlan,
                      substraitJsonFilePath_ + "substrait_aggregate_plan_countC4.json");
  }

  void countAllQueryTest(const std::vector<RowVectorPtr>& vectors, bool ignoreNullKeys) {
    auto plan = PlanBuilder()
                    .values(vectors)
                    .filter("c6 < 24")
                    .aggregation({"c0", "c1"},
                                 {"count(1) as num_price"},
                                 {},
                                 core::AggregationNode::Step::kSingle,
                                 ignoreNullKeys)
                    .project({"num_price"})
                    .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE c0 IS NOT NULL AND c1 IS NOT NULL ";
    }

    assertQuery(
        plan,
        "SELECT count(*) as num_price " + fromClause + " WHERE c6 < 24 GROUP BY c0, c1");

    // Transform to substrait plan.
    auto sPlan = v2SPlanConvertor_->toSubstrait(arena, plan);
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(), INTEGER(), INTEGER(), BIGINT(), REAL(), DOUBLE(), INTEGER()})};

  /*!
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

  VeloxToSubstraitPlanConvertor* v2SPlanConvertor_;
  std::string substraitJsonFilePath_ = "/src/substrait/data/";
  google::protobuf::Arena arena;
};

TEST_F(veloxToSubstraitAggregationTest, global) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .aggregation({},
                               {"sum(15)",
                                "sum(c1)",
                                "sum(c2)",
                                "sum(c4)",
                                "sum(c5)",
                                "min(15)",
                                "min(c1)",
                                "min(c2)",
                                "min(c3)",
                                "min(c4)",
                                "min(c5)",
                                "max(15)",
                                "max(c1)",
                                "max(c2)",
                                "max(c3)",
                                "max(c4)",
                                "max(c5)"},
                               {},
                               core::AggregationNode::Step::kPartial,
                               false)
                  .planNode();

  assertQuery(plan,
              "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15), "
              "min(c1),min(c2), min(c3), min(c4), min(c5), max(15), max(c1), max(c2), "
              "max(c3), max(c4),max(c5) FROM tmp");

  auto sPlan = v2SPlanConvertor_->toSubstrait(arena, plan);
}

TEST_F(veloxToSubstraitAggregationTest, SingleBigintKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  singleKeyTest<int64_t>(std::move(vectors), "c0", false, false);
  singleKeyTest<int64_t>(std::move(vectors), "c0", true, false);
}

TEST_F(veloxToSubstraitAggregationTest, SingleBigintKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  singleKeyTest<int64_t>(vectors, "c0", false, true);
  singleKeyTest<int64_t>(vectors, "c0", true, true);
}

TEST_F(veloxToSubstraitAggregationTest, SingleStringKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  singleKeyTest<StringView>(vectors, "c6", false, false);
  singleKeyTest<StringView>(vectors, "c6", true, false);
}

TEST_F(veloxToSubstraitAggregationTest, SingleStringKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  singleKeyTest<StringView>(vectors, "c6", false, true);
  singleKeyTest<StringView>(vectors, "c6", true, true);
}

TEST_F(veloxToSubstraitAggregationTest, MultiKey) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  multiKeyTest(vectors, false, false);
  multiKeyTest(vectors, true, false);
}

TEST_F(veloxToSubstraitAggregationTest, MultiKeyDistinct) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  multiKeyTest(vectors, false, true);
  multiKeyTest(vectors, true, true);
}

TEST_F(veloxToSubstraitAggregationTest, Avg) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  avgQueryTest(vectors, false);
}

TEST_F(veloxToSubstraitAggregationTest, AggregateOfNulls) {
  RowVectorPtr vectors =
      makeRowVector({makeFlatVector<int64_t>(
                         {2499109626526694126, 2342493223442167775, 4077358421272316858}),
                     makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
                     makeFlatVector<double>(
                         {0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
                     makeFlatVector<bool>({true, false, false}),
                     makeFlatVector<int32_t>(3, nullptr, nullEvery(1))});
  createDuckDbTable({vectors});

  auto vPlan = PlanBuilder()
                   .values({vectors})
                   .aggregation({"c0"},
                                {"sum(c4)", "min(c4)", "max(c4)"},
                                {},
                                core::AggregationNode::Step::kPartial,
                                false)
                   .planNode();

  assertQuery(vPlan, "SELECT c0, sum(c4), min(c4), max(c4) FROM tmp GROUP BY c0");
  auto sPlan = v2SPlanConvertor_->toSubstrait(arena, vPlan);
}

TEST_F(veloxToSubstraitAggregationTest, AggregateOfNullsWoGroupBy) {
  RowVectorPtr vectors =
      makeRowVector({makeFlatVector<int64_t>(
                         {2499109626526694126, 2342493223442167775, 4077358421272316858}),
                     makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
                     makeFlatVector<double>(
                         {0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
                     makeFlatVector<bool>({true, false, false}),
                     makeFlatVector<int32_t>(3, nullptr, nullEvery(1))});
  createDuckDbTable({vectors});

  // global aggregation
  auto plan = PlanBuilder()
                  .values({vectors})
                  .aggregation({},
                               {"sum(c4)", "min(c4)", "max(c4)"},
                               {},
                               core::AggregationNode::Step::kPartial,
                               false)
                  .planNode();

  assertQuery(plan, "SELECT sum(c4), min(c4), max(c4) FROM tmp");
  auto sPlan = v2SPlanConvertor_->toSubstrait(arena, plan);
}

TEST_F(veloxToSubstraitAggregationTest, Count) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  countQueryTest(vectors, false);
}

TEST_F(veloxToSubstraitAggregationTest, CountAll) {
  auto vectors = makeVector(2, 7, 3);
  createDuckDbTable(vectors);
  countAllQueryTest(vectors, false);
}

}  // namespace
}  // namespace facebook::velox::exec::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
