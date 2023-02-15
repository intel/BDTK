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

/**
 * @file    Substrait2IRTest.cpp
 * @brief   Test Expression IR generation from Substrait
 **/

#include <gflags/gflags.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include <fstream>
#include <sstream>
#include <string>
#include "cider/CiderTableSchema.h"
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "util/Logger.h"

std::string getDataFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/substrait_plan_files/";
}

void relAlgExecutionUnitCreateAndCompile(std::string file_name) {
  std::ifstream sub_json(getDataFilesPath() + file_name);
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(buffer.str(), &sub_plan);
  generator::SubstraitToRelAlgExecutionUnit eu_translator(sub_plan);
  // TODO : (yma11) switch to use BatchProcessor API
  // auto cider_compile_module =
  //     CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());
  // cider_compile_module->feedBuildTable(std::move(buildCiderBatch()));
  // cider_compile_module->compile(sub_plan);
}

TEST(Substrait2IR, OutputTableSchema) {
  std::ifstream sub_json(getDataFilesPath() + "nullability.json");
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(buffer.str(), &sub_plan);
  generator::SubstraitToRelAlgExecutionUnit eu_translator(sub_plan);
  eu_translator.createRelAlgExecutionUnit();
  auto table_schema = eu_translator.getOutputCiderTableSchema();
  CHECK(table_schema->getColumnTypeById(0).has_bool_() &&
        table_schema->getColumnTypeById(0).bool_().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(1).has_bool_() &&
        table_schema->getColumnTypeById(1).bool_().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
  CHECK(table_schema->getColumnTypeById(2).has_i32() &&
        table_schema->getColumnTypeById(2).i32().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(3).has_i32() &&
        table_schema->getColumnTypeById(3).i32().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
  CHECK(table_schema->getColumnTypeById(4).has_i64() &&
        table_schema->getColumnTypeById(4).i64().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(5).has_i64() &&
        table_schema->getColumnTypeById(5).i64().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
  CHECK(table_schema->getColumnTypeById(6).has_decimal() &&
        table_schema->getColumnTypeById(6).decimal().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(7).has_decimal() &&
        table_schema->getColumnTypeById(7).decimal().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
  CHECK(table_schema->getColumnTypeById(8).has_fp32() &&
        table_schema->getColumnTypeById(8).fp32().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(9).has_fp32() &&
        table_schema->getColumnTypeById(9).fp32().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
  CHECK(table_schema->getColumnTypeById(10).has_fp64() &&
        table_schema->getColumnTypeById(10).fp64().nullability() ==
            substrait::Type::NULLABILITY_NULLABLE);
  CHECK(table_schema->getColumnTypeById(11).has_fp64() &&
        table_schema->getColumnTypeById(11).fp64().nullability() ==
            substrait::Type::NULLABILITY_REQUIRED);
}

TEST(Substrait2IR, ColIndexUpdate_1) {
  // Check final col index should be correct
  // Select sum(l_extendedprice) as sum_ext, sum(l_quantity) as sum_qua,
  // sum(l_discount) as sum_dis from lineitem where l_quantity < 0. 5
  std::ifstream sub_json(getDataFilesPath() + "col_update.json");
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(buffer.str(), &sub_plan);
  generator::SubstraitToRelAlgExecutionUnit eu_translator(sub_plan);
  auto rel_alg_eu = eu_translator.createRelAlgExecutionUnit();
  std::vector<int> target_cols{1, 0, 2};
  std::vector<int> cols;
  for (int i = 0; i < rel_alg_eu.target_exprs.size(); i++) {
    std::shared_ptr<Analyzer::Expr> target_expr(rel_alg_eu.target_exprs[i]);
    if (auto expr = std::dynamic_pointer_cast<Analyzer::AggExpr>(target_expr)) {
      if (auto column =
              std::dynamic_pointer_cast<Analyzer::ColumnVar>(expr->get_own_arg())) {
        cols.emplace_back(column->get_column_id());
      }
    }
  }
  for (int i = 0; i < target_cols.size(); i++) {
    CHECK_EQ(target_cols[i], cols[i]);
  }
}

TEST(Substrait2IR, ColIndexUpdate_2) {
  // Check final targets should be correct
  // select sum(l_extendedprice * l_discount) as revenue,
  // sum(l_quantity) as sum_quantity from lineitem where l_quantity < 0.5
  std::ifstream sub_json(getDataFilesPath() + "agg_with_expr.json");
  std::stringstream buffer;
  buffer << sub_json.rdbuf();
  ::substrait::Plan sub_plan;
  google::protobuf::util::JsonStringToMessage(buffer.str(), &sub_plan);
  generator::SubstraitToRelAlgExecutionUnit eu_translator(sub_plan);
  auto rel_alg_eu = eu_translator.createRelAlgExecutionUnit();
  CHECK(std::strcmp(
      rel_alg_eu.target_exprs[0]->toString().c_str(),
      "(SUM (* (ColumnVar table: 100 column: 1 rte: 0 DOUBLE) (ColumnVar table: 100 "
      "column: 2 rte: 0 DOUBLE) ) )"));
  CHECK(std::strcmp(rel_alg_eu.target_exprs[1]->toString().c_str(),
                    "(SUM (ColumnVar table: 100 column: 0 rte: 0 DOUBLE) ))"));
}

TEST(Substrait2IR, FilterProject) {
  // select * from test where l_quantity > 24
  relAlgExecutionUnitCreateAndCompile("select_all.json");
  // select l_suppkey, l_quantity from lineitem where l_orderkey > 10
  relAlgExecutionUnitCreateAndCompile("simple_project_filter.json");
  // select l_suppkey, l_extendedprice*l_discount from lineitem where l_orderkey > 10
  relAlgExecutionUnitCreateAndCompile("project_filter_with_expression.json");
  // SELECT l_nationkey / 2 ,  l_quantity from lineitem where l_orderkey > 10 or
  // l_suppkey > 4 or l_discount < 0.8
  relAlgExecutionUnitCreateAndCompile("filter_or.json");
  // SELECT l_nationkey * 2,  l_quantity from lineitem where l_orderkey > 10 and
  // l_suppkey > 4 and l_discount < 0.8
  relAlgExecutionUnitCreateAndCompile("filter_and.json");
  // SELECT l_nationkey * 2 from lineitem where
  // (l_orderkey > 10 and l_suppkey > 4) or l_discount < 0.8
  relAlgExecutionUnitCreateAndCompile("filter_and_or.json");
  // select l_suppkey/2 from lineitem where l_orderkey between 10 and 20
  relAlgExecutionUnitCreateAndCompile("between_and.json");
  // SELECT c0 FROM tmp WHERE c0 between 0 and 5
  relAlgExecutionUnitCreateAndCompile("between_and_i64_velox.json");
  // select l_orderkey%2 from lineitem where not l_orderkey > 10
  relAlgExecutionUnitCreateAndCompile("modulus_not.json");
  // select c0, c1, c0 + c1 from tmp where c1 > 1.1 (from velox)
  relAlgExecutionUnitCreateAndCompile("type_real_velox.json");
  // select l_suppkey, l_quantity from lineitem where l_orderkey <> 10
  relAlgExecutionUnitCreateAndCompile("neq.json");
  // select t0 from tmp where t0 and t1 and t2
  relAlgExecutionUnitCreateAndCompile("and_with_three_args.json");
}

TEST(Substrait2IR, AggregateTest) {
  // select sum(l_orderkey+l_partkey) from lineitem where l_orderkey > 10
  relAlgExecutionUnitCreateAndCompile("agg.json");
  // select sum(l_orderkey+l_partkey) from lineitem where l_orderkey >= 100 group by
  // l_orderkey
  relAlgExecutionUnitCreateAndCompile("agg_groupby_1.json");
  // select l_orderkey, sum(l_orderkey+l_partkey) from lineitem where l_orderkey >= 100
  // group by l_orderkey
  relAlgExecutionUnitCreateAndCompile("agg_groupby_2.json");
  // select l_orderkey+l_partkey, sum(l_orderkey) from lineitem where l_orderkey >= 100
  // group by l_orderkey, l_orderkey+l_partkey
  relAlgExecutionUnitCreateAndCompile("agg_groupby_3.json");
  // select l_orderkey, l_orderkey+l_partkey from lineitem where l_orderkey >= 100 group
  // by l_orderkey, l_orderkey+l_partkey
  relAlgExecutionUnitCreateAndCompile("groupby_only.json");
  // select count(*)/count(1) from lineitem
  relAlgExecutionUnitCreateAndCompile("count_asterisk_or_1.json");
  // select count(*)/count(1) from lineitem
  relAlgExecutionUnitCreateAndCompile("count_asterisk_or_2.json");
  // select count(l_linenumber) from lineitem
  // Substarit cannot generate json for count(col)
  relAlgExecutionUnitCreateAndCompile("count_col_fake.json");
  // final avg: select avg(l_orderkey) from lineitem
  relAlgExecutionUnitCreateAndCompile("avg.json");
  // partial avg: select avg(col_i32) from tmp
  relAlgExecutionUnitCreateAndCompile("avg_partial.json");
  // select avg(c4) as avg_price from tmp where c6 < 24 group by c0, c1
  relAlgExecutionUnitCreateAndCompile("avg_groupby_velox.json");
}

TEST(Substrait2IR, JoinTest) {
  // select l_shipmode from orders, lineitem where o_orderkey = l_orderkey
  // group by l_shipmode
  relAlgExecutionUnitCreateAndCompile("join_fake.json");
  // SELECT r_a, r_b from table_left JOIN table_right ON l_a = r_a + 1
  relAlgExecutionUnitCreateAndCompile("join_with_pre_project.json");
  // SELECT r_a, r_b from table_left JOIN table_right ON r_a = l_a + 1
  relAlgExecutionUnitCreateAndCompile("join_with_pre_project_1.json");

  // select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp,supplier,nation
  // where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'

  // select LINEITEM.L_LINENUMBER, ORDERS.O_CUSTKEY from LINEITEM join ORDERS on
  // LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY
  relAlgExecutionUnitCreateAndCompile("basic_join.json");

  // SELECT o_custkey, l_linenumber FROM orders, lineitem WHERE l_orderkey =
  // o_orderkey
  relAlgExecutionUnitCreateAndCompile("join1.json");

  // SELECT count(*) FROM orders, lineitem WHERE l_orderkey = o_orderkey AND o_orderkey >
  // 10000
  relAlgExecutionUnitCreateAndCompile("join2.json");

  // SELECT o_orderstatus, l_linenumber FROM orders, lineitem WHERE l_orderkey =
  // o_orderkey
  // o_orderstatus is string type, cider do not supported yet.

  // SELECT count(*) FROM orders, lineitem WHERE o_orderkey = l_orderkey AND o_orderkey %
  // 2 = 1 isthmus do not support '%' yet, so cannot generate substrait json
}

TEST(Substrait2IR, LeftJoinTest) {
  GTEST_SKIP_("Projection type TEXT not supported for outer joins yet.");
  // SELECT * FROM orders o LEFT JOIN lineitem l ON o_orderkey = l_orderkey AND
  // l_linenumber > 5
  relAlgExecutionUnitCreateAndCompile("left_join.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  generator::registerExtensionFunctions();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
