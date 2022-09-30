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

#include <gtest/gtest.h>
#include "tests/utils/CiderTestBase.h"

class CiderSemiJoinTest : public CiderJoinTestBase {
 public:
  CiderSemiJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e "
        "BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(500,
                                                 {"l_a", "l_b", "l_c", "l_d", "l_e"},
                                                 {CREATE_SUBSTRAIT_TYPE(I64),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Bool)}))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    resetHashTable();
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        100,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderSemiJoinNullableTest : public CiderJoinTestBase {
 public:
  CiderSemiJoinNullableTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e "
        "BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(10,
                                                 {"l_a", "l_b", "l_c", "l_d", "l_e"},
                                                 {CREATE_SUBSTRAIT_TYPE(I64),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Bool)}))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    resetHashTable();
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        3,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {2, 2, 2, 2, 2, 2})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

TEST_F(CiderSemiJoinTest, semi) {
  std::string sql_semi = "SELECT l_a FROM table_probe SEMI JOIN table_hash ON l_a = r_a";
  std::string sql_semi_in =
      "SELECT l_a FROM table_probe WHERE l_a IN (SELECT r_a FROM table_hash)";
  assertJoinQueryAndReset(sql_semi_in, "semi.json");
}

TEST_F(CiderSemiJoinTest, anti) {
  std::string sql_anti = "SELECT l_a FROM table_probe ANTI JOIN table_hash ON l_a = r_a";
  std::string sql_anti_in =
      "SELECT l_a FROM table_probe WHERE l_a NOT IN (SELECT r_a FROM table_hash)";
  assertJoinQueryAndReset(sql_anti_in, "anti.json");
}

TEST_F(CiderSemiJoinNullableTest, semi) {
  std::string sql_semi = "SELECT l_a FROM table_probe SEMI JOIN table_hash ON l_a = r_a";
  std::string sql_semi_in =
      "SELECT l_a FROM table_probe WHERE l_a IN (SELECT r_a FROM table_hash)";
  assertJoinQueryAndReset(sql_semi_in, "semi.json");
}

TEST_F(CiderSemiJoinNullableTest, anti) {
  GTEST_SKIP_("DuckDB not in NULL will filter all rows.");
  std::string sql_anti = "SELECT l_a FROM table_probe ANTI JOIN table_hash ON l_a = r_a";
  std::string sql_anti_in =
      "SELECT l_a FROM table_probe WHERE l_a NOT IN (SELECT r_a FROM table_hash)";
  assertJoinQueryAndReset(sql_anti_in, "anti.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
