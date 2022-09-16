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

class CiderNonEqualJoinTest : public CiderJoinTestBase {
 public:
  CiderNonEqualJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e "
        "BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(100,
                                                 {"l_a", "l_b", "l_c", "l_d", "l_e"},
                                                 {CREATE_SUBSTRAIT_TYPE(I64),
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  CREATE_SUBSTRAIT_TYPE(Fp64),
                                                  CREATE_SUBSTRAIT_TYPE(Fp32),
                                                  CREATE_SUBSTRAIT_TYPE(Bool)},
                                                 {2, 2, 2, 2, 2},
                                                 GeneratePattern::Random,
                                                 0,
                                                 20))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER NOT NULL, r_c DOUBLE, r_d FLOAT "
        "NOT NULL, r_e "
        "BOOLEAN, r_f INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        20,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 0, 3, 0, 0, 0}));
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        20,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 0, 3, 0, 0, 0})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderNonEqualJoinNullableSeqTest : public CiderJoinTestBase {
 public:
  CiderNonEqualJoinNullableSeqTest() {
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
                                                  CREATE_SUBSTRAIT_TYPE(Bool)},
                                                 {2, 2, 2, 2, 2},
                                                 GeneratePattern::Sequence))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 0, 3, 0, 0, 0}));
  }

  void resetHashTable() override {
    build_table_.reset(new CiderBatch(QueryDataGenerator::generateBatchByTypes(
        20,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 0, 3, 0, 0, 0})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

#define NON_EQUAL_JOIN_TEST_UNIT(PROJECT, JOIN_COMPARISON_OPERATOR)                  \
  /*INNER JOIN ON BIGINT*/                                                           \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe JOIN table_hash ON l_a " #JOIN_COMPARISON_OPERATOR " r_a"); \
  /*INNER JOIN ON INTEGER*/                                                          \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe JOIN table_hash ON l_b " #JOIN_COMPARISON_OPERATOR " r_b"); \
  /*INNER JOIN ON DOUBLE*/                                                           \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe JOIN table_hash ON l_c " #JOIN_COMPARISON_OPERATOR " r_c"); \
  /*INNER JOIN ON FLOAT*/                                                            \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe JOIN table_hash ON l_d " #JOIN_COMPARISON_OPERATOR " r_d"); \
  /*LEFT JOIN ON BIGINT*/                                                            \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe LEFT JOIN table_hash ON l_a " #JOIN_COMPARISON_OPERATOR     \
      " r_a");                                                                       \
  /*LEFT JOIN ON INTEGER*/                                                           \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe LEFT JOIN table_hash ON l_b " #JOIN_COMPARISON_OPERATOR     \
      " r_b");                                                                       \
  /*LEFT JOIN ON DOUBLE*/                                                            \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe LEFT JOIN table_hash ON l_c " #JOIN_COMPARISON_OPERATOR     \
      " r_c");                                                                       \
  /*LEFT JOIN ON FLOAT*/                                                             \
  assertJoinQueryRowEqualAndReset(                                                   \
      "SELECT " #PROJECT                                                             \
      " from table_probe LEFT JOIN table_hash ON l_d " #JOIN_COMPARISON_OPERATOR     \
      " r_d");

TEST_F(CiderNonEqualJoinTest, JoinCondition_NON_EQUAL) {
  NON_EQUAL_JOIN_TEST_UNIT(*, <);
  NON_EQUAL_JOIN_TEST_UNIT(*, >);
  NON_EQUAL_JOIN_TEST_UNIT(*, <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >=);
  NON_EQUAL_JOIN_TEST_UNIT(*, <>);
}

TEST_F(CiderNonEqualJoinTest, AggWithNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_a), <);
  NON_EQUAL_JOIN_TEST_UNIT(min(l_b), >);
  NON_EQUAL_JOIN_TEST_UNIT(max(r_a), <=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(r_b), >=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_c), <>);
}

TEST_F(CiderNonEqualJoinTest, ComplexNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(*, +1 <);
  NON_EQUAL_JOIN_TEST_UNIT(*, > 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, -1 < 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >= 2 *);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <= 2 *);
}

#define NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(                          \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                                        \
  /*INNER JOIN ON INTEGER AND DOUBLE*/                                                \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b AND l_c " #JOIN_CONDITION2 " r_c ");                                      \
  /*INNER JOIN ON BIGINT AND FLOAT*/                                                  \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a AND l_d " #JOIN_CONDITION2 " r_d ");                                      \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                               \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a AND l_d " #JOIN_CONDITION2 " 10 ");                                       \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                               \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b AND r_c " #JOIN_CONDITION2 " 10 ");

TEST_F(CiderNonEqualJoinTest, multiColInAndRelationshipNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <=, >=);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <=, <>);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <>, >=);
}

#define NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(                           \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                                        \
  /*INNER JOIN ON INTEGER AND DOUBLE*/                                                \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b OR l_c " #JOIN_CONDITION2 " r_c ");                                       \
  /*INNER JOIN ON BIGINT AND FLOAT*/                                                  \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a OR l_d " #JOIN_CONDITION2 " r_d ");                                       \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                               \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a OR l_d " #JOIN_CONDITION2 " 10 ");                                        \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                               \
  assertJoinQueryRowEqualAndReset(                                                    \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b OR r_c " #JOIN_CONDITION2 " 10 ");

// data are generated randomly, using AND to test equal will always return 0 results.
TEST_F(CiderNonEqualJoinTest, multiColInOrRelationshipNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, >, =);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, =, >);
}

TEST_F(CiderNonEqualJoinTest, AggWithmultiColNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(sum(l_a), <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(max(l_b), =, >=);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(min(l_c), <=, =);
}

#define NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(                 \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                          \
  /*LEFT JOIN ON INTEGER AND DOUBLE*/                                   \
  assertJoinQueryAndReset(                                              \
      "SELECT " #PROJECT                                                \
      " from table_probe LEFT JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b AND l_c " #JOIN_CONDITION2 " r_c ",                         \
      "",                                                               \
      false);                                                           \
  /*LEFT JOIN ON BIGINT AND FLOAT*/                                     \
  assertJoinQueryAndReset(                                              \
      "SELECT " #PROJECT                                                \
      " from table_probe LEFT JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a AND l_d " #JOIN_CONDITION2 " r_d ",                         \
      "",                                                               \
      false);                                                           \
  /*LEFT JOIN ON BIGINT AND CONSTANT*/                                  \
  assertJoinQueryAndReset(                                              \
      "SELECT " #PROJECT                                                \
      " from table_probe LEFT JOIN table_hash ON l_a " #JOIN_CONDITION1 \
      " r_a AND l_d " #JOIN_CONDITION2 " 10 ",                          \
      "",                                                               \
      false);                                                           \
  /*LEFT JOIN ON INTEGER AND CONSTANT*/                                 \
  assertJoinQueryAndReset(                                              \
      "SELECT " #PROJECT                                                \
      " from table_probe LEFT JOIN table_hash ON l_b " #JOIN_CONDITION1 \
      " r_b AND r_c " #JOIN_CONDITION2 " 10 ",                          \
      "",                                                               \
      false);

// FIXME: the return results of duckdb are incorrect. just compare the rows number.
TEST_F(CiderNonEqualJoinNullableSeqTest, multiColNonEqualLeftJoin) {
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, <>);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <>, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, =, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, >, =);
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
