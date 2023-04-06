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
#include <velox/type/Type.h>
#include <string>

#include "CiderOperatorTestBase.h"
#include "CiderPlanBuilder.h"
#include "CiderPlanNodeTranslator.h"
#include "planTransformerTest/utils/PlanTansformerTestUtil.h"

using namespace facebook::velox;
using namespace facebook::velox::plugin::test;
using namespace facebook::velox::plugin::plantransformer::test;

using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::plugin::CiderPlanNode;
using facebook::velox::plugin::test::CiderPlanBuilder;

class CiderScalarFunctionMathOpTest : public CiderOperatorTestBase {};

class CiderScalarFunctionLogicalOpTest : public CiderOperatorTestBase {};

TEST_F(CiderScalarFunctionMathOpTest, colAndConstantMathOpForDoubleRealTest) {
  std::string duckDbSql =
      " select c0 + 0.123,c0 - 0.123, c0 * 0.123, c0 / 0.123 from tmp";
  std::vector<std::string> projections = {
      "c0 + 0.123", "c0 - 0.123", "c0 * 0.123", "c0 / 0.123"};
  const std::vector<TypePtr> types_{REAL(), DOUBLE()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0"}, {type})};

    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(projections)
               .planNode(),
           duckDbSql);
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, true))
               .project(projections)
               .planNode(),
           duckDbSql);
  }
}

TEST_F(CiderScalarFunctionMathOpTest, colAndColMathOpForDoubleRealTest) {
  const std::vector<TypePtr> types_{REAL(), DOUBLE()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0", "c1"}, {type, type})};
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project({"c0 + c1", "c0 - c1", "c0 * c1", "c0 / c1"})
               .planNode(),
           " select c0 + c1, c0 - c1, c0 * c1, c0 / c1 from tmp");
  }
}

TEST_F(CiderScalarFunctionMathOpTest, colAndConstantMathOpForIntTest) {
  std::string duckDbSql = " select c0 + 2, c0 - 2, c0 * 1, c0 / 2 from tmp";
  std::vector<std::string> projections = {"c0 + 2", "c0 - 2", "c0 * 1", "c0 / 2"};
  const std::vector<TypePtr> types_{TINYINT(), SMALLINT(), INTEGER(), BIGINT()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0"}, {type})};
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(projections)
               .planNode(),
           duckDbSql);
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, true))
               .project(projections)
               .planNode(),
           duckDbSql);
  }
}

TEST_F(CiderScalarFunctionMathOpTest, colAndColMathOpWithBigIntTest) {
  //  Fixme: multiply op not supported for BIGINT type due to overflow
  //  multiplication/addition of INT64
  const std::vector<TypePtr> types_{TINYINT(), SMALLINT(), INTEGER()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0", "c1"}, {BIGINT(), type})};
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false, false))
               .project({"c0 + c1", "c0 - c1", "c0 / c1"})
               .planNode(),
           " select c0 + c1, c0 - c1, c0 / c1 from tmp");
  }
}

TEST_F(CiderScalarFunctionMathOpTest, modOpTest) {
  std::string duckDbSql = " select c0 % 2 from tmp";
  std::vector<std::string> projections = {"c0 % 2"};
  const std::vector<TypePtr> types_{TINYINT(), SMALLINT(), INTEGER(), BIGINT()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0"}, {type})};

    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(projections)
               .planNode(),
           duckDbSql);
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, true))
               .project(projections)
               .planNode(),
           duckDbSql);
  }
}

TEST_F(CiderScalarFunctionLogicalOpTest, AndNotNullTest) {
  std::string duckDbSql = " select linenumber = 2 and quantity > 10 from tmp";
  std::vector<std::string> projections = {"linenumber = 2 and quantity > 10"};
  std::shared_ptr<const RowType> rowType{
      ROW({"linenumber", "quantity"}, {INTEGER(), INTEGER()})};

  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, false))
             .project(projections)
             .planNode(),
         duckDbSql);
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, true))
             .project(projections)
             .planNode(),
         duckDbSql);
}

TEST_F(CiderScalarFunctionLogicalOpTest, orNotNullTest) {
  std::string duckDbSql = " select linenumber = 2 or quantity > 10 from tmp";
  std::vector<std::string> projections = {"linenumber = 2 or quantity > 10"};
  std::shared_ptr<const RowType> rowType{
      ROW({"linenumber", "quantity"}, {INTEGER(), INTEGER()})};
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, false))
             .project(projections)
             .planNode(),
         duckDbSql);
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, true))
             .project(projections)
             .planNode(),
         duckDbSql);
}

TEST_F(CiderScalarFunctionLogicalOpTest, notNotNullTest) {
  std::string duckDbSql = " select * from tmp where not linenumber = 2";
  std::shared_ptr<const RowType> rowType{ROW({"linenumber"}, {INTEGER()})};
  std::string filter = "not linenumber = 2";
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, false))
             .filter(filter)
             .planNode(),
         duckDbSql);
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType, true))
             .filter(filter)
             .planNode(),
         duckDbSql);
}

TEST_F(CiderScalarFunctionLogicalOpTest, basicCompareTest) {
  std::string duckDbSql = " select 3 < 2, 3 > 2, 3 = 2, 3 <= 2, 3 >= 2, 3 <> 2 from tmp";
  std::vector<std::string> projections = {
      "3 < 2", "3 > 2", "3 = 2", "3 <= 2", "3 >= 2", "3 <> 2"};
  const std::vector<TypePtr> types_{
      BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()};
  for (auto& type : types_) {
    std::shared_ptr<const RowType> rowType{ROW({"c0"}, {type})};
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(projections)
               .planNode(),
           duckDbSql);
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, true))
               .project(projections)
               .planNode(),
           duckDbSql);
  }
}

TEST_F(CiderScalarFunctionLogicalOpTest, likeTest) {
  std::string duckDbSql = " select name from tmp where name like '%29'";
  std::string filter = "name like '%29'";
  std::vector<std::string> projections;
  std::shared_ptr<const RowType> rowType{ROW({"name"}, {VARCHAR()})};
  auto vectors = generateTestBatch(rowType, false);
  auto ciderPlan =
      CiderPlanBuilder().values(vectors).filter(filter).project({"name"}).planNode();
  verify(ciderPlan, duckDbSql);

  const ::substrait::Plan substraitPlan = ::substrait::Plan();
  auto expectedPlan =
      PlanBuilder()
          .values(vectors)
          .addNode([&](std::string id, std::shared_ptr<const core::PlanNode> input) {
            return std::make_shared<CiderPlanNode>(
                CiderPlanNode(id, {input}, input->outputType(), substraitPlan));
          })
          .planNode();
  EXPECT_TRUE(PlanTansformerTestUtil::comparePlanSequence(ciderPlan, expectedPlan));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
