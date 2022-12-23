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
#include <string>
#include <vector>
#include "CiderOperatorTestBase.h"
#include "CiderPlanBuilder.h"

using namespace facebook::velox::plugin::test;

class CiderOperatorAggOpTest : public CiderOperatorTestBase {
 public:
  RowTypePtr rowType_{
      ROW({"l_orderkey",
           "l_linenumber",
           "l_discount",
           "l_extendedprice",
           "l_quantity",
           "l_shipdate"},
          {BIGINT(), INTEGER(), DOUBLE(), REAL(), TINYINT(), SMALLINT()})};

  void verifyProjectAgg(RowTypePtr& rowType,
                        std::vector<std::string> projects,
                        std::vector<std::string> groupingKeys,
                        std::vector<std::string> aggregations,
                        const std::string& referenceQuery) {
    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, true))
               .project(projects)
               .partialAggregation(groupingKeys, aggregations)
               .planNode(),
           referenceQuery);

    verify(CiderPlanBuilder()
               .values(generateTestBatch(rowType, false))
               .project(projects)
               .partialAggregation(groupingKeys, aggregations)
               .planNode(),
           referenceQuery);
  }
};

TEST_F(CiderOperatorAggOpTest, groupby_multi_col) {
  auto duckDbSql =
      "SELECT l_orderkey, l_shipdate, SUM(l_extendedprice * l_discount) AS revenue, "
      "SUM(l_quantity) AS sum_quan FROM tmp GROUP BY l_orderkey, l_shipdate";
  std::vector<std::string> projects = {"l_orderkey",
                                       "l_shipdate",
                                       "l_extendedprice * l_discount as revenue",
                                       "l_quantity as sum_quan"};
  std::vector<std::string> groupbys = {"l_orderkey", "l_shipdate"};
  std::vector<std::string> aggs = {"SUM(revenue)", "SUM(sum_quan)"};
  // Skip this since velox also have the problem about memory pool.
  GTEST_SKIP();
  verifyProjectAgg(rowType_, projects, groupbys, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, groupby_multi_col_count) {
  auto duckDbSql =
      "SELECT l_orderkey, l_shipdate, COUNT(*), SUM(l_extendedprice * l_discount) AS "
      "revenue, SUM(l_quantity) AS sum_quan FROM tmp GROUP BY l_orderkey, l_shipdate";
  std::vector<std::string> projects = {"l_orderkey",
                                       "l_shipdate",
                                       "l_extendedprice * l_discount AS revenue",
                                       "l_quantity AS sum_quan"};
  std::vector<std::string> groupbys = {"l_orderkey", "l_shipdate"};
  std::vector<std::string> aggs = {"COUNT(1)", "SUM(revenue)", "SUM(sum_quan)"};
  // Skip this since velox also have the problem about memory pool.
  GTEST_SKIP();
  verifyProjectAgg(rowType_, projects, groupbys, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, groupby_single_col) {
  std::string duckDbSql =
      "SELECT l_orderkey, SUM(l_extendedprice * l_discount) AS revenue, "
      "SUM(l_quantity) AS sum_quan FROM tmp GROUP BY l_orderkey";

  std::vector<std::string> projects = {
      "l_orderkey", "l_extendedprice * l_discount AS revenue", "l_quantity AS sum_quan"};
  std::vector<std::string> groupbys = {"l_orderkey"};
  std::vector<std::string> aggs = {"SUM(revenue)", "SUM(sum_quan)"};
  // Skip this since velox also have the problem about memory pool.
  GTEST_SKIP();
  verifyProjectAgg(rowType_, projects, groupbys, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, groupby_single_col_count) {
  std::string duckDbSql =
      "SELECT l_orderkey, COUNT(l_discount) FROM tmp GROUP BY l_orderkey ";

  std::vector<std::string> projects = {"l_orderkey", "l_discount"};
  std::vector<std::string> groupbys = {"l_orderkey"};
  std::vector<std::string> aggs = {"COUNT(l_discount)"};
  // Skip this since velox also have the problem about memory pool.
  GTEST_SKIP();
  verifyProjectAgg(rowType_, projects, groupbys, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, having_col) {
  auto duckDbSql =
      "SELECT l_linenumber, SUM(l_linenumber) AS sum_linenum FROM tmp WHERE "
      "l_linenumber < 10 GROUP BY l_linenumber HAVING l_linenumber > 2";

  std::string filters = "l_linenumber > 2 and l_linenumber < 10";
  std::vector<std::string> projects = {"l_linenumber"};
  std::vector<std::string> groupbys = {"l_linenumber"};
  std::vector<std::string> aggs = {"sum(l_linenumber) as sum_linenum"};

  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType_, false))
             .filter(filters)
             .project(projects)
             .partialAggregation(groupbys, aggs)
             .planNode(),
         duckDbSql);
  // Skip this since velox also have the problem about memory pool after WW51.
  GTEST_SKIP();
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType_, true))
             .filter(filters)
             .project(projects)
             .partialAggregation(groupbys, aggs)
             .planNode(),
         duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, having_agg) {
  auto duckDbSql =
      "SELECT l_linenumber, SUM(l_linenumber) AS sum_linenum FROM tmp WHERE  "
      "l_linenumber < 10 GROUP BY l_linenumber HAVING SUM(l_linenumber) > 2";
  std::string filters = "l_linenumber < 10";
  std::vector<std::string> projects = {"l_linenumber"};
  std::vector<std::string> groupbys = {"l_linenumber"};
  std::vector<std::string> aggs = {"sum(l_linenumber) as sum_linenum"};
  std::string postFilters = "sum_linenum > 2";
  std::vector<std::string> postProjects = {"l_linenumber", "sum_linenum"};
  // Skip this since velox also have the problem about memory pool.
  GTEST_SKIP();
  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType_, true))
             .filter(filters)
             .project(projects)
             .partialAggregation(groupbys, aggs)
             .filter(postFilters)
             .project(postProjects)
             .planNode(),
         duckDbSql);

  verify(CiderPlanBuilder()
             .values(generateTestBatch(rowType_, false))
             .filter(filters)
             .project(projects)
             .partialAggregation(groupbys, aggs)
             .filter(postFilters)
             .project(postProjects)
             .planNode(),
         duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, wogroupby_multi_col) {
  auto duckDbSql =
      "SELECT SUM(l_extendedprice * l_discount) AS revenue, SUM(l_quantity) AS sum_quan "
      "FROM tmp";
  std::vector<std::string> projects = {"l_orderkey",
                                       "l_shipdate",
                                       "l_extendedprice * l_discount as revenue",
                                       "l_quantity as sum_quan"};
  std::vector<std::string> aggs = {"SUM(revenue)", "SUM(sum_quan)"};
  verifyProjectAgg(rowType_, projects, {}, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, wogroupby_multi_col_count) {
  auto duckDbSql =
      "SELECT COUNT(*), SUM(l_extendedprice * l_discount) AS revenue, SUM(l_quantity) AS "
      "sum_quan FROM tmp ";
  std::vector<std::string> projects = {"l_orderkey",
                                       "l_shipdate",
                                       "l_extendedprice * l_discount AS revenue",
                                       "l_quantity AS sum_quan"};
  std::vector<std::string> aggs = {"COUNT(1)", "SUM(revenue)", "SUM(sum_quan)"};

  verifyProjectAgg(rowType_, projects, {}, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, wogroupby_single_col) {
  std::string duckDbSql =
      "SELECT SUM(l_extendedprice * l_discount) AS revenue, SUM(l_quantity) AS sum_quan "
      "FROM tmp ";

  std::vector<std::string> projects = {
      "l_orderkey", "l_extendedprice * l_discount AS revenue", "l_quantity AS sum_quan"};
  std::vector<std::string> aggs = {"SUM(revenue)", "SUM(sum_quan)"};

  verifyProjectAgg(rowType_, projects, {}, aggs, duckDbSql);
}

TEST_F(CiderOperatorAggOpTest, wogroupby_single_col_count) {
  std::string duckDbSql = "SELECT COUNT(l_discount) FROM tmp ";

  std::vector<std::string> projects = {"l_orderkey", "l_discount"};
  std::vector<std::string> aggs = {"COUNT(l_discount)"};

  verifyProjectAgg(rowType_, projects, {}, aggs, duckDbSql);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
