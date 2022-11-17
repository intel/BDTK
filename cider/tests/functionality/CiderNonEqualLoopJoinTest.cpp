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

// TODO: (spevenhe) Add test cases using this test class
// For not null query, if set NOT NULL on column ddl, the final project
// will go to cider_agg_id_proj_xx() instead of cider_agg_id_proj_xx_nullable(), So the
// returned null values are incorrect.
class CiderArrowSeqNotNullInDDLJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowSeqNotNullInDDLJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT NOT NULL, l_int INTEGER NOT NULL, "
        "l_double DOUBLE NOT NULL, l_float FLOAT NOT NULL, l_varchar VARCHAR(10) NOT "
        "NULL);";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        10,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0},
        GeneratePattern::Sequence);
    input_ = {std::shared_ptr<CiderBatch>(new CiderBatch(
        actual_schema, actual_array, std::make_shared<CiderDefaultAllocator>()))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT NOT NULL, r_int INTEGER NOT NULL, "
        "r_double DOUBLE NOT NULL, r_float FLOAT NOT NULL, r_varchar VARCHAR(10) NOT "
        "NULL);";

    ArrowSchema* build_schema = nullptr;
    ArrowArray* build_array = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0});
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0});

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderArrowSeqNotNullJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowSeqNotNullJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, "
        "l_double DOUBLE, l_float FLOAT, l_varchar VARCHAR(10));";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        10,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0},
        GeneratePattern::Sequence);
    input_ = {std::shared_ptr<CiderBatch>(new CiderBatch(
        actual_schema, actual_array, std::make_shared<CiderDefaultAllocator>()))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT, r_int INTEGER, "
        "r_double DOUBLE, r_float FLOAT, r_varchar VARCHAR(10));";

    ArrowSchema* build_schema = nullptr;
    ArrowArray* build_array = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0});
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 0, 0, 0, 0});

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderArrowRandomNullableJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowRandomNullableJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, "
        "l_double DOUBLE, l_float FLOAT, l_varchar VARCHAR(10));";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        20,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        -5,
        5);
    input_ = {std::shared_ptr<CiderBatch>(new CiderBatch(
        actual_schema, actual_array, std::make_shared<CiderDefaultAllocator>()))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT, r_int INTEGER, "
        "r_double DOUBLE, r_float FLOAT, r_varchar VARCHAR(10));";

    ArrowSchema* build_schema = nullptr;
    ArrowArray* build_array = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        10,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        -4,
        4);
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        10,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        -4,
        4);

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

#define NON_EQUAL_JOIN_TEST_UNIT(PROJECT, JOIN_COMPARISON_OPERATOR)                      \
  /*INNER JOIN ON BIGINT*/                                                               \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe JOIN table_hash ON l_bigint " #JOIN_COMPARISON_OPERATOR         \
      " r_bigint");                                                                      \
  /*INNER JOIN ON INTEGER*/                                                              \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe JOIN table_hash ON l_int " #JOIN_COMPARISON_OPERATOR " r_int"); \
  /*INNER JOIN ON DOUBLE*/                                                               \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe JOIN table_hash ON l_double " #JOIN_COMPARISON_OPERATOR         \
      " r_double");                                                                      \
  /*INNER JOIN ON FLOAT*/                                                                \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe JOIN table_hash ON l_float " #JOIN_COMPARISON_OPERATOR          \
      " r_float");                                                                       \
  /*LEFT JOIN ON BIGINT*/                                                                \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe LEFT JOIN table_hash ON l_bigint " #JOIN_COMPARISON_OPERATOR    \
      " r_bigint");                                                                      \
  /*LEFT JOIN ON INTEGER*/                                                               \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe LEFT JOIN table_hash ON l_int " #JOIN_COMPARISON_OPERATOR       \
      " r_int");                                                                         \
  /*LEFT JOIN ON DOUBLE*/                                                                \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe LEFT JOIN table_hash ON l_double " #JOIN_COMPARISON_OPERATOR    \
      " r_double");                                                                      \
  /*LEFT JOIN ON FLOAT*/                                                                 \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                         \
      "SELECT " #PROJECT                                                                 \
      " from table_probe LEFT JOIN table_hash ON l_float " #JOIN_COMPARISON_OPERATOR     \
      " r_float");

TEST_F(CiderArrowRandomNullableJoinTest, JoinCondition_NON_EQUAL) {
  NON_EQUAL_JOIN_TEST_UNIT(*, <);
  NON_EQUAL_JOIN_TEST_UNIT(*, >);
  NON_EQUAL_JOIN_TEST_UNIT(*, <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >=);
  NON_EQUAL_JOIN_TEST_UNIT(*, <>);
}

TEST_F(CiderArrowRandomNullableJoinTest, AggWithNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_bigint), <);
  NON_EQUAL_JOIN_TEST_UNIT(min(l_int), >);
  NON_EQUAL_JOIN_TEST_UNIT(max(r_bigint), <=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(r_int), >=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_double), <>);
}

TEST_F(CiderArrowRandomNullableJoinTest, ComplexNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(*, +1 <);
  NON_EQUAL_JOIN_TEST_UNIT(*, > 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, -1 < 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >= 2 *);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <= 2 *);
}

TEST_F(CiderArrowSeqNotNullJoinTest, JoinCondition_NON_EQUAL) {
  NON_EQUAL_JOIN_TEST_UNIT(*, <);
  NON_EQUAL_JOIN_TEST_UNIT(*, >);
  NON_EQUAL_JOIN_TEST_UNIT(*, <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >=);
  NON_EQUAL_JOIN_TEST_UNIT(*, <>);
}

TEST_F(CiderArrowSeqNotNullJoinTest, AggWithNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_bigint), <);
  NON_EQUAL_JOIN_TEST_UNIT(min(l_int), >);
  NON_EQUAL_JOIN_TEST_UNIT(max(r_bigint), <=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(r_int), >=);
  NON_EQUAL_JOIN_TEST_UNIT(sum(l_double), <>);
}

TEST_F(CiderArrowSeqNotNullJoinTest, ComplexNonEqualJoin) {
  NON_EQUAL_JOIN_TEST_UNIT(*, +1 <);
  NON_EQUAL_JOIN_TEST_UNIT(*, > 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, -1 < 1 +);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <=);
  NON_EQUAL_JOIN_TEST_UNIT(*, >= 2 *);
  NON_EQUAL_JOIN_TEST_UNIT(*, *2 <= 2 *);
}

#define NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(                            \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                                          \
  /*INNER JOIN ON INTEGER AND DOUBLE*/                                                  \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_int " #JOIN_CONDITION1 \
      " r_int AND l_double " #JOIN_CONDITION2 " r_double ");                            \
  /*INNER JOIN ON BIGINT AND FLOAT*/                                                    \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT                                                                \
      " from table_probe JOIN table_hash ON l_bigint " #JOIN_CONDITION1                 \
      " r_bigint AND l_float " #JOIN_CONDITION2 " r_float ");                           \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                                 \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT                                                                \
      " from table_probe JOIN table_hash ON l_bigint " #JOIN_CONDITION1                 \
      " r_bigint AND l_float " #JOIN_CONDITION2 " 10 ");                                \
  /*INNER JOIN ON BIGINT AND CONSTANT*/                                                 \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_int " #JOIN_CONDITION1 \
      " r_int AND r_double " #JOIN_CONDITION2 " 10 ");

TEST_F(CiderArrowRandomNullableJoinTest, multiColInAndRelationshipNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <=, >=);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <=, <>);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(*, <>, >=);
}

#define NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(                             \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                                          \
  /*INNER JOIN ON INTEGER OR DOUBLE*/                                                   \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_int " #JOIN_CONDITION1 \
      " r_int OR l_double " #JOIN_CONDITION2 " r_double ");                             \
  /*INNER JOIN ON BIGINT OR FLOAT*/                                                     \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT                                                                \
      " from table_probe JOIN table_hash ON l_bigint " #JOIN_CONDITION1                 \
      " r_bigint OR l_float " #JOIN_CONDITION2 " r_float ");                            \
  /*INNER JOIN ON BIGINT OR CONSTANT*/                                                  \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT                                                                \
      " from table_probe JOIN table_hash ON l_bigint " #JOIN_CONDITION1                 \
      " r_bigint OR l_float " #JOIN_CONDITION2 " 10 ");                                 \
  /*INNER JOIN ON BIGINT OR CONSTANT*/                                                  \
  assertJoinQueryRowEqualForArrowFormatAndReset(                                        \
      "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_int " #JOIN_CONDITION1 \
      " r_int OR r_double " #JOIN_CONDITION2 " 10 ");

// data are generated randomly, using AND to test equal will always return 0 results.
TEST_F(CiderArrowRandomNullableJoinTest, multiColInOrRelationshipNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, >, =);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, =, >);
}

TEST_F(CiderArrowSeqNotNullJoinTest, multiColInOrRelationshipNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, >, =);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(*, =, >);
}

TEST_F(CiderArrowRandomNullableJoinTest, AggWithmultiColNonEqualInnerJoin) {
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_AND(sum(l_bigint), <, >);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(max(l_int), =, >=);
  NON_EQUAL_INNER_JOIN_MULTI_CONDITIONS_TEST_UNIT_OR(min(l_double), <=, =);
}

// the return results of duckdb are incorrect. Manually checked cider's results's
// correctness. Just compare the rows number.
#define NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(                      \
    PROJECT, JOIN_CONDITION1, JOIN_CONDITION2)                               \
  /*LEFT JOIN ON INTEGER AND DOUBLE*/                                        \
  assertJoinQueryRowEqualForArrowFormatAndReset(                             \
      "SELECT " #PROJECT                                                     \
      " from table_probe LEFT JOIN table_hash ON l_int " #JOIN_CONDITION1    \
      " r_int AND l_double " #JOIN_CONDITION2 " r_double ",                  \
      "",                                                                    \
      true,                                                                  \
      false);                                                                \
  /*LEFT JOIN ON BIGINT AND FLOAT*/                                          \
  assertJoinQueryRowEqualForArrowFormatAndReset(                             \
      "SELECT " #PROJECT                                                     \
      " from table_probe LEFT JOIN table_hash ON l_bigint " #JOIN_CONDITION1 \
      " r_bigint AND l_float " #JOIN_CONDITION2 " r_float ",                 \
      "",                                                                    \
      true,                                                                  \
      false);                                                                \
  /*LEFT JOIN ON BIGINT AND CONSTANT*/                                       \
  assertJoinQueryRowEqualForArrowFormatAndReset(                             \
      "SELECT " #PROJECT                                                     \
      " from table_probe LEFT JOIN table_hash ON l_bigint " #JOIN_CONDITION1 \
      " r_bigint AND l_float " #JOIN_CONDITION2 " 10 ",                      \
      "",                                                                    \
      true,                                                                  \
      false);                                                                \
  /*LEFT JOIN ON INTEGER AND CONSTANT*/                                      \
  assertJoinQueryRowEqualForArrowFormatAndReset(                             \
      "SELECT " #PROJECT                                                     \
      " from table_probe LEFT JOIN table_hash ON l_int " #JOIN_CONDITION1    \
      " r_int AND r_double " #JOIN_CONDITION2 " 10 ",                        \
      "",                                                                    \
      true,                                                                  \
      false);

// the return results of duckdb are incorrect. Manually checked cider's results's
// correctness. Just compare the rows number.
TEST_F(CiderArrowRandomNullableJoinTest, multiColNonEqualLeftJoin) {
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, <>);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <>, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, =, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, >, =);
}

TEST_F(CiderArrowSeqNotNullJoinTest, multiColNonEqualLeftJoin) {
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <=, <>);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, <>, >=);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, =, >);
  NON_EQUAL_LEFT_JOIN_MULTI_CONDITIONS_TEST_UNIT(*, >, =);
}

// below are original ciderbatch tests
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
  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::INFO;
  log_options.set_options();  // update default values
  logger::init(log_options);
  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
