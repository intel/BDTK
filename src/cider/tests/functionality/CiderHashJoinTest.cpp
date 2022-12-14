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

// arrow format test
class CiderArrowOneToOneSeqNotNullJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowOneToOneSeqNotNullJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT NOT NULL, l_int INTEGER NOT NULL, "
        "l_double DOUBLE NOT NULL, l_float FLOAT NOT NULL);";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        100,
        {"l_bigint", "l_int", "l_double", "l_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0},
        GeneratePattern::Sequence);
    input_ = {std::shared_ptr<CiderBatch>(new CiderBatch(
        actual_schema, actual_array, std::make_shared<CiderDefaultAllocator>()))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT NOT NULL, r_int INTEGER NOT NULL, "
        "r_double DOUBLE NOT NULL, r_float FLOAT NOT NULL);";

    ArrowSchema* build_schema = nullptr;
    ArrowArray* build_array = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        90,
        {"r_bigint", "r_int", "r_double", "r_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0});
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        90,
        {"r_bigint", "r_int", "r_double", "r_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0});

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderArrowOneToOneSeqNullableJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowOneToOneSeqNullableJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, "
        "l_double DOUBLE, l_float FLOAT, l_varchar VARCHAR(10));";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        100,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
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
        80,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2});
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        80,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2});

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderArrowOneToManyRandomNullableJoinTest : public CiderArrowFormatJoinTestBase {
 public:
  CiderArrowOneToManyRandomNullableJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, l_double DOUBLE, "
        "l_float FLOAT, l_varchar VARCHAR(10));";

    ArrowSchema* actual_schema = nullptr;
    ArrowArray* actual_array = nullptr;

    QueryArrowDataGenerator::generateBatchByTypes(
        actual_schema,
        actual_array,
        100,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        -50,
        50);
    input_ = {std::shared_ptr<CiderBatch>(new CiderBatch(
        actual_schema, actual_array, std::make_shared<CiderDefaultAllocator>()))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT, r_int INTEGER, r_double DOUBLE, "
        "r_float FLOAT, r_varchar VARCHAR(10));";

    ArrowSchema* build_schema = nullptr;
    ArrowArray* build_array = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        100,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {3, 3, 3, 3, 3},
        GeneratePattern::Random,
        -30,
        30);
    build_table_ = std::shared_ptr<CiderBatch>(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
  }

  void resetHashTable() override {
    ArrowArray* build_array = nullptr;
    ArrowSchema* build_schema = nullptr;
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema,
        build_array,
        100,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {3, 3, 3, 3, 3},
        GeneratePattern::Random,
        -30,
        30);

    build_table_.reset(new CiderBatch(
        build_schema, build_array, std::make_shared<CiderDefaultAllocator>()));
    duckDbQueryRunner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

#define LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                          \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN_A, JOIN_COMPARISON_OPERATOR)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");                              \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE l_" #COLUMN_A " >  10 "); \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE r_" #COLUMN_A " >  10 "); \
  }

// For not null query, if set NOT NULL on column ddl, the final project
// will go to cider_agg_id_proj_xx() instead of cider_agg_id_proj_xx_nullable(), So the
// returned null values are incorrect.
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest,
// ArrowOneToOneSeqNoNullJoinTest, *, int, =)  // NOLINT
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest,
// ArrowOneToOneSeqNoNullJoinTest2, *, bigint, =)  // NOLINT
LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, LeftJoinArrowOneToOneSeqNoNullableTest, *, int, =)  // NOLINT
LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, LeftJoinArrowOneToOneSeqNoNullableJoinTest2, *, bigint, =)  // NOLINT
LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, LeftJoinArrowOneToManyRandomNullJoinTest, *, int, =)  // NOLINT
LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, LeftJoinArrowOneToManyRandomNullJoinTest2, *, bigint, =)  // NOLINT

#define INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                         \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN_A, JOIN_COMPARISON_OPERATOR)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");                              \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE l_" #COLUMN_A " <  50 "); \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE r_" #COLUMN_A " <  50 "); \
    /*AVOID RECYCLE HASHTABLE*/                                                         \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT " from table_probe INNER JOIN table_hash ON l_" #COLUMN_A    \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");                              \
  }

INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest, ArrowOneToOneSeqNoNullJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest, ArrowOneToOneSeqNoNullJoinTest2, *, bigint, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, ArrowOneToOneSeqNoNullableJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, ArrowOneToOneSeqNoNullableJoinTest2, *, bigint, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ArrowOneToManyRandomNullJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ArrowOneToManyRandomNullJoinTest2, *, bigint, =)  // NOLINT

#define COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                  \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN, JOIN_COMPARISON_OPERATOR)              \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                 \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN       \
        " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");                          \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                 \
        "SELECT " #PROJECT " from table_probe INNER JOIN table_hash ON l_" #COLUMN \
        " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");                          \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                 \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN  \
        " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");                          \
  }

// agg and join condition on same column
COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                         AggJoinTest1,
                                         sum(l_bigint),
                                         bigint,
                                         =)
COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                         AggJoinTest2,
                                         sum(r_bigint),
                                         bigint,
                                         =)

COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ExprJoinRandomTest1, *, bigint, +1 =)  // NOLINT
COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ExprJoinRandomTest2, *, bigint, = 1 +)  // NOLINT
COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ExprJoinRandomTest3, *, int, -1 = 1 +)  // NOLINT

// using OR to avoid 0 results
#define DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(TEST_CLASS, UNIT_NAME, PROJECT)      \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_bigint =  r_bigint "); \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                                   \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_int = "     \
        "r_int ");                                                                      \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                               \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_int = 10 ");           \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                                 \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_bigint = "  \
        "10 ");                                                                         \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                              \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_int IS NOT NULL ");    \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                                \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                      \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_bigint IS " \
        "NOT NULL ");                                                                   \
  }
// TODO: (spevenhe) comment due to OR will fail back to loop join
// while AND is still hash join
// DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest,
// ORJoinConditionTest1, *)
// DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
// ORJoinConditionTest2, *)
// DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
//                               ORJoinConditionWithAggTest1,
//                               SUM(l_bigint))
// DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
//                               ORJoinConditionWithAggTest2,
//                               SUM(r_bigint))

#define DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(TEST_CLASS, UNIT_NAME, PROJECT)      \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                                   \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_bigint =  r_bigint "); \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                                    \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint AND l_int = "     \
        "r_int ");                                                                       \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                                \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_int = 10 ");           \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint AND l_bigint = "  \
        "10 ");                                                                          \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                               \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_int IS NOT NULL ");    \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                                 \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                       \
        "SELECT " #PROJECT                                                               \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint AND l_bigint IS " \
        "NOT NULL ");                                                                    \
  }

DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, ANDJoinConditionTest1, *)  // NOLINT
DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ANDJoinConditionTest2, *)  // NOLINT
DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                            ANDJoinConditionWithAggTest1,
                                            SUM(l_bigint))
DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                            ANDJoinConditionWithAggTest2,
                                            SUM(r_bigint))

#define HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(TEST_CLASS, UNIT_NAME, PROJECT)  \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                     \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE l_bigint <  " \
        "10 ");                                                                       \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE l_bigint IS " \
        "NOT NULL ");                                                                 \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint "                   \
        "WHERE l_bigint IS NOT NULL AND l_bigint < 10 ");                             \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE r_bigint < "  \
        "10 ");                                                                       \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE r_bigint IS " \
        "NOT NULL ");                                                                 \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint "                   \
        "WHERE r_bigint IS NOT NULL AND r_bigint < 10 ");                             \
    /*FILTER ON PROBE TABLE COLUMN*/                                                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE l_double <  10 ");  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE l_double IS NOT "   \
        "NULL ");                                                                     \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int "                         \
        "WHERE l_double IS NOT NULL AND l_double < 10 ");                             \
    /*FILTER ON BUILD TABLE COLUMN*/                                                  \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE r_double < 10 ");   \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE r_double IS NOT "   \
        "NULL ");                                                                     \
    assertJoinQueryRowEqualForArrowFormatAndReset(                                    \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_int = r_int "                         \
        "WHERE r_double IS NOT NULL AND r_double < 10 ");                             \
  }

HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, HashJoinWithFilterTest1, *)  // NOLINT
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, HashJoinWithFilterTest2, *)  // NOLINT
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                             HashJoinWithFilterAndAggTest1,
                                             SUM(l_bigint))
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                             HashJoinWithFilterAndAggTest2,
                                             SUM(r_bigint))

TEST_F(CiderArrowOneToOneSeqNullableJoinTest, selectTestSingleColumnBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualForArrowFormatAndReset(
      "SELECT r_bigint from table_probe JOIN table_hash ON l_bool = r_bool");
}

TEST_F(CiderArrowOneToOneSeqNullableJoinTest, selectFilterBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualForArrowFormatAndReset(
      "SELECT l_bool from table_probe JOIN table_hash ON l_bool = r_bool WHERE l_bool != "
      "true");
}

TEST_F(CiderArrowOneToOneSeqNullableJoinTest, innerJoinWithWhereBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualForArrowFormatAndReset(
      "SELECT l_bool from table_probe, table_hash WHERE table_probe.l_bool = "
      "table_hash.r_bool");
}

// original ciderbatch format test
class CiderOneToOneRandomJoinTest : public CiderJoinTestBase {
 public:
  CiderOneToOneRandomJoinTest() {
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
                                                  CREATE_SUBSTRAIT_TYPE(Bool)},
                                                 {2, 2, 2, 2, 2},
                                                 GeneratePattern::Random,
                                                 0,
                                                 100))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0}));
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
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderOneToOneSeqJoinTest : public CiderJoinTestBase {
 public:
  CiderOneToOneSeqJoinTest() {
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
                                                 GeneratePattern::Sequence))};

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0}));
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
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0})));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderOneToManyRandomJoinTest : public CiderJoinTestBase {
 public:
  CiderOneToManyRandomJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e "
        "BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(200,
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
        "CREATE TABLE table_hash(r_a BIGINT, r_b INTEGER, r_c DOUBLE, r_d FLOAT, r_e "
        "BOOLEAN, r_f INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0},
        GeneratePattern::Random,
        0,
        20));
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
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0},
        GeneratePattern::Random,
        0,
        20)));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

class CiderOneToManySeqJoinTest : public CiderJoinTestBase {
 public:
  CiderOneToManySeqJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_a BIGINT, l_b INTEGER, l_c DOUBLE, l_d FLOAT, l_e "
        "BOOLEAN);";
    input_ = {std::make_shared<CiderBatch>(
        QueryDataGenerator::generateBatchByTypes(30,
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
        100,
        {"r_a", "r_b", "r_c", "r_d", "r_e", "r_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Bool),
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0},
        GeneratePattern::Random,
        0,
        20));
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
         CREATE_SUBSTRAIT_TYPE(I32)},
        {3, 3, 3, 3, 3, 0},
        GeneratePattern::Random,
        0,
        20)));
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});
  }
};

TEST_F(CiderOneToOneRandomJoinTest, PostJoinFilterTest) {
  assertJoinQueryRowEqualAndReset(
      "SELECT * FROM table_probe JOIN table_hash ON l_a = r_a WHERE l_a < 10;",
      "post_join_filter1.json");
  assertJoinQueryRowEqualAndReset(
      "SELECT * FROM table_probe JOIN table_hash ON l_a = r_a AND l_a < 10;",
      "post_join_filter1.json");
  assertJoinQueryRowEqualAndReset(
      "SELECT * FROM table_probe JOIN table_hash ON l_a = r_a WHERE l_a + r_a < 10;",
      "post_join_filter2.json");
  assertJoinQueryRowEqualAndReset(
      "SELECT * FROM table_probe JOIN table_hash ON l_a = r_a AND l_a + r_a < 10;",
      "post_join_filter2.json");
}

#define HASH_JOIN_TEST_UNIT(                                                          \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN, JOIN_COMPARISON_OPERATOR)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                     \
    assertJoinQueryRowEqualAndReset("SELECT " #PROJECT                                \
                                    " from table_probe JOIN table_hash ON l_" #COLUMN \
                                    " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN ""); \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT " from table_probe INNER JOIN table_hash ON l_" #COLUMN    \
        " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");                             \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN     \
        " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");                             \
  }

HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, simpleJoinOnBigintTest, *, a, =)
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, simpleJoinOnIntegerTest, *, b, =)
HASH_JOIN_TEST_UNIT(CiderOneToOneSeqJoinTest, simpleJoinOnFloatTest, *, c, =)
HASH_JOIN_TEST_UNIT(CiderOneToOneSeqJoinTest, simpleJoinOnDoubleTest, *, d, =)

// agg and join condition on same column
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, AggJoinTest1, sum(l_a), a, =)
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, AggJoinTest2, sum(r_a), a, =)
// agg and join condition on different column
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, AggJoinTest3, sum(l_a), d, =)
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, AggJoinTest4, sum(r_a), d, =)

HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, ExprJoinRandomTest1, *, a, +1 =)
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, ExprJoinRandomTest2, *, a, = 1 +)
HASH_JOIN_TEST_UNIT(CiderOneToOneRandomJoinTest, ExprJoinRandomTest3, *, b, -1 = 1 +)
HASH_JOIN_TEST_UNIT(CiderOneToOneSeqJoinTest, ExprJoinSeqTest1, *, c, *2 =)
HASH_JOIN_TEST_UNIT(CiderOneToOneSeqJoinTest, ExprJoinSeqTest2, *, d, = 2 *)
HASH_JOIN_TEST_UNIT(CiderOneToOneSeqJoinTest, ExprJoinSeqTest3, *, d, *2 = 2 *)

// using OR to avoid 0 results
#define DOUBLE_JOIN_OR_CONDITION_TEST(TEST_CLASS, UNIT_NAME, PROJECT)               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                   \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                              \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe JOIN table_hash ON l_b = r_b OR l_c =  r_c ");           \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                               \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a OR l_d = r_d ");       \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                           \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe JOIN table_hash ON l_b = r_b OR l_b = 10 ");             \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                             \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a OR l_a = 10 ");        \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                          \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe JOIN table_hash ON l_b = r_b OR l_b IS NOT NULL ");      \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                            \
    assertJoinQueryRowEqualAndReset(                                                \
        "SELECT " #PROJECT                                                          \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a OR l_a IS NOT NULL "); \
  }
// TODO: (spevenhe) comment due to OR will fail back to loop join
// while AND is still hash join
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToOneSeqJoinTest, ORJoinConditionTest1, *)
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToManySeqJoinTest, ORJoinConditionTest2, *)
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToOneRandomJoinTest, ORJoinConditionTest3, *)
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToManyRandomJoinTest, ORJoinConditionTest4, *)
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToOneRandomJoinTest,
//                               ORJoinConditionWithAggTest1,
//                               SUM(l_a))
// DOUBLE_JOIN_OR_CONDITION_TEST(CiderOneToManyRandomJoinTest,
//                               ORJoinConditionWithAggTest2,
//                               SUM(r_a))

#define DOUBLE_JOIN_AND_CONDITION_TEST(TEST_CLASS, UNIT_NAME, PROJECT)               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                    \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                               \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe JOIN table_hash ON l_b = r_b AND l_c =  r_c ");           \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                                \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a AND l_d = r_d ");       \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                            \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe JOIN table_hash ON l_b = r_b AND l_b = 10 ");             \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                              \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a AND l_a = 10 ");        \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                           \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe JOIN table_hash ON l_b = r_b AND l_b IS NOT NULL ");      \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                             \
    assertJoinQueryRowEqualAndReset(                                                 \
        "SELECT " #PROJECT                                                           \
        " from table_probe LEFT JOIN table_hash ON l_a = r_a AND l_a IS NOT NULL "); \
  }

DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToOneSeqJoinTest, ANDJoinConditionTest1, *)
DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToManySeqJoinTest, ANDJoinConditionTest2, *)
DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToOneRandomJoinTest, ANDJoinConditionTest3, *)
DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToManyRandomJoinTest, ANDJoinConditionTest4, *)
DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToOneRandomJoinTest,
                               ANDJoinConditionWithAggTest1,
                               SUM(l_a))
DOUBLE_JOIN_AND_CONDITION_TEST(CiderOneToManyRandomJoinTest,
                               ANDJoinConditionWithAggTest2,
                               SUM(r_a))

#define HASH_JOIN_WITH_FILTER_TEST_UNIT(TEST_CLASS, UNIT_NAME, PROJECT)               \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                     \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_a = r_a WHERE l_a <  10 ");           \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_a = r_a WHERE l_a IS NOT NULL ");     \
    assertJoinQueryRowEqualAndReset("SELECT " #PROJECT                                \
                                    " from table_probe JOIN table_hash ON l_a = r_a " \
                                    "WHERE l_a IS NOT NULL AND l_a < 10 ");           \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_a = r_a WHERE r_a < 10 ");            \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_a = r_a WHERE r_a IS NOT NULL ");     \
    assertJoinQueryRowEqualAndReset("SELECT " #PROJECT                                \
                                    " from table_probe JOIN table_hash ON l_a = r_a " \
                                    "WHERE r_a IS NOT NULL AND r_a < 10 ");           \
    /*FILTER ON PROBE TABLE COLUMN*/                                                  \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_b = r_b WHERE l_c <  10 ");           \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_b = r_b WHERE l_c IS NOT NULL ");     \
    assertJoinQueryRowEqualAndReset("SELECT " #PROJECT                                \
                                    " from table_probe JOIN table_hash ON l_b = r_b " \
                                    "WHERE l_c IS NOT NULL AND l_c < 10 ");           \
    /*FILTER ON BUILD TABLE COLUMN*/                                                  \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_b = r_b WHERE r_c < 10 ");            \
    assertJoinQueryRowEqualAndReset(                                                  \
        "SELECT " #PROJECT                                                            \
        " from table_probe JOIN table_hash ON l_b = r_b WHERE r_c IS NOT NULL ");     \
    assertJoinQueryRowEqualAndReset("SELECT " #PROJECT                                \
                                    " from table_probe JOIN table_hash ON l_b = r_b " \
                                    "WHERE r_c IS NOT NULL AND r_c < 10 ");           \
  }

HASH_JOIN_WITH_FILTER_TEST_UNIT(CiderOneToOneRandomJoinTest, HashJoinWithFilterTest1, *)
HASH_JOIN_WITH_FILTER_TEST_UNIT(CiderOneToManyRandomJoinTest, HashJoinWithFilterTest2, *)
HASH_JOIN_WITH_FILTER_TEST_UNIT(CiderOneToOneRandomJoinTest,
                                HashJoinWithFilterAndAggTest1,
                                SUM(l_a))
HASH_JOIN_WITH_FILTER_TEST_UNIT(CiderOneToManyRandomJoinTest,
                                HashJoinWithFilterAndAggTest2,
                                SUM(r_a))

TEST_F(CiderOneToOneSeqJoinTest, selectTestSingleColumnBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualAndReset(
      "SELECT r_a from table_probe JOIN table_hash ON l_e = r_e");
}

TEST_F(CiderOneToOneSeqJoinTest, selectFilterBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualAndReset(
      "SELECT l_e from table_probe JOIN table_hash ON l_e = r_e WHERE l_e != true");
}

TEST_F(CiderOneToOneSeqJoinTest, innerJoinWithWhereBoolType) {
  GTEST_SKIP_("This kind of case is not One-To-One Hash Join, open it when supported.");
  assertJoinQueryRowEqualAndReset(
      "SELECT l_e from table_probe, table_hash WHERE table_probe.l_e = table_hash.r_e");
}

class CiderInnerJoinUsingTest : public CiderJoinTestBase {
 public:
  CiderInnerJoinUsingTest() {
    table_name_ = "table_probe";
    create_ddl_ = "CREATE TABLE table_probe(col_a BIGINT, col_b INTEGER);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I32)},
        {},
        GeneratePattern::Random,
        0,
        100))};

    build_table_name_ = "table_hash";
    build_table_ddl_ = "CREATE TABLE table_hash(col_a BIGINT, col_b INTEGER);";
    build_table_ = std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I32)}));
  }
};

TEST_F(CiderInnerJoinUsingTest, usingSyntaxTest) {
  GTEST_SKIP_("Isthmus does not support USING yet");
  assertJoinQuery("SELECT * from table_probe JOIN table_hash USING (col_a)");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  int err{0};
  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::INFO;
  log_options.set_options();  // update default values
  logger::init(log_options);
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
