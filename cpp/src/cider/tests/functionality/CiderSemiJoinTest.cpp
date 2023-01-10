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

// FOR explicit semi join query like:
// SELECT l_long FROM table_probe SEMI JOIN table_hash ON l_long = r_long,
// it's equal to IN clause:
// SELECT l_long FROM table_probe WHERE l_long IN (SELECT r_long FROM table_hash)
// And same for anti join.

class CiderSemiJoinTest : public CiderJoinTestBase {
 public:
  CiderSemiJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_long BIGINT, l_int INTEGER, l_double DOUBLE, "
        "l_float FLOAT, l_bool BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        500,
        {"l_long", "l_int", "l_double", "l_float", "l_bool"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool)}))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_long BIGINT, r_int INTEGER, r_double DOUBLE, r_float "
        "FLOAT, r_bool BOOLEAN);";
    resetHashTable();
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        100,
        {"r_long", "r_int", "r_double", "r_float", "r_bool"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool)})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderSemiJoinNullableTest : public CiderJoinTestBase {
 public:
  CiderSemiJoinNullableTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_long BIGINT, l_int INTEGER, l_double DOUBLE, "
        "l_float FLOAT, l_bool BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"l_long", "l_int", "l_double", "l_float", "l_bool"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool)}))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_long BIGINT, r_int INTEGER, r_double DOUBLE, r_float "
        "FLOAT, r_bool BOOLEAN);";
    resetHashTable();
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        3,
        {"r_long", "r_int", "r_double", "r_float", "r_bool"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool)},
        {2, 2, 2, 2, 2})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

TEST_F(CiderSemiJoinTest, semi) {
  std::string sql_semi_in =
      "SELECT l_long FROM table_probe WHERE l_long IN (SELECT r_long FROM table_hash)";
  assertJoinQueryAndReset(sql_semi_in, "semi.json");
}

TEST_F(CiderSemiJoinTest, anti) {
  std::string sql_anti_in =
      "SELECT l_long FROM table_probe WHERE l_long NOT IN (SELECT r_long FROM "
      "table_hash)";
  assertJoinQueryAndReset(sql_anti_in, "anti.json");
}

TEST_F(CiderSemiJoinNullableTest, semi) {
  std::string sql_semi_in =
      "SELECT l_long FROM table_probe WHERE l_long IN (SELECT r_long FROM table_hash)";
  assertJoinQueryAndReset(sql_semi_in, "semi.json");
}

TEST_F(CiderSemiJoinNullableTest, anti) {
  GTEST_SKIP_("DuckDB not in NULL will filter all rows.");
  std::string sql_anti_in =
      "SELECT l_long FROM table_probe WHERE l_long NOT IN (SELECT r_long FROM "
      "table_hash)";
  assertJoinQueryAndReset(sql_anti_in, "anti.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
