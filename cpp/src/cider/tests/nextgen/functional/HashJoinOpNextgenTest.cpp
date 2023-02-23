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

#include <gtest/gtest.h>
#include "tests/utils/CiderNextgenTestBase.h"

using namespace cider::test::util;

// arrow format test
class CiderArrowOneToOneSeqNotNullJoinTest : public CiderJoinNextgenTestBase {
 public:
  CiderArrowOneToOneSeqNotNullJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT NOT NULL, l_int INTEGER NOT NULL, "
        "l_double DOUBLE NOT NULL, l_float FLOAT NOT NULL);";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        100,
        {"l_bigint", "l_int", "l_double", "l_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0},
        GeneratePattern::Sequence);

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT NOT NULL, r_int INTEGER NOT NULL, "
        "r_double DOUBLE NOT NULL, r_float FLOAT NOT NULL);";

    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
        90,
        {"r_bigint", "r_int", "r_double", "r_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0});
  }

  void resetHashTable() override {
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
        90,
        {"r_bigint", "r_int", "r_double", "r_float"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32)},
        {0, 0, 0, 0});

    duckdb_query_runner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, *build_array_, *build_schema_);
  }
};

class CiderArrowOneToOneSeqNullableJoinTest : public CiderJoinNextgenTestBase {
 public:
  CiderArrowOneToOneSeqNullableJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, "
        "l_double DOUBLE, l_float FLOAT, l_varchar VARCHAR(10));";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        10,
        {"l_bigint", "l_int", "l_double", "l_float", "l_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Sequence);

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT, r_int INTEGER, "
        "r_double DOUBLE, r_float FLOAT, r_varchar VARCHAR(10));";

    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2});
  }

  void resetHashTable() override {
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
        8,
        {"r_bigint", "r_int", "r_double", "r_float", "r_varchar"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2});

    duckdb_query_runner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, *build_array_, *build_schema_);
  }
};

class CiderArrowOneToManyRandomNullableJoinTest : public CiderJoinNextgenTestBase {
 public:
  CiderArrowOneToManyRandomNullableJoinTest() {
    table_name_ = "table_probe";
    create_ddl_ =
        "CREATE TABLE table_probe(l_bigint BIGINT, l_int INTEGER, l_double DOUBLE, "
        "l_float FLOAT, r_varchar VARCHAR(10));";

    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
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

    build_table_name_ = "table_hash";
    build_table_ddl_ =
        "CREATE TABLE table_hash(r_bigint BIGINT, r_int INTEGER, r_double DOUBLE, "
        "r_float FLOAT, r_varchar VARCHAR(10));";

    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
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
  }

  void resetHashTable() override {
    QueryArrowDataGenerator::generateBatchByTypes(
        build_schema_,
        build_array_,
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

    duckdb_query_runner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, *build_array_, *build_schema_);
  }
};

#define INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                         \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN_A, JOIN_COMPARISON_OPERATOR)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertNextGenJoinQuery("SELECT " #PROJECT                                           \
                           " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
                           " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");           \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                  \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE l_" #COLUMN_A " <  50 "); \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                  \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT " from table_probe JOIN table_hash ON l_" #COLUMN_A          \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE r_" #COLUMN_A " <  50 "); \
    /*AVOID RECYCLE HASHTABLE*/                                                         \
    assertNextGenJoinQuery("SELECT " #PROJECT                                           \
                           " from table_probe INNER JOIN table_hash ON l_" #COLUMN_A    \
                           " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");           \
  }

// INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest,
// ArrowOneToOneSeqNoNullJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest, ArrowOneToOneSeqNoNullJoinTest2, *, bigint, =)  // NOLINT
// INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest,
// ArrowOneToOneSeqNoNullableJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, ArrowOneToOneSeqNoNullableJoinTest2, *, bigint, =)  // NOLINT
// INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
// ArrowOneToManyRandomNullJoinTest, *, int, =)  // NOLINT
INNER_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ArrowOneToManyRandomNullJoinTest2, *, bigint, =)  // NOLINT

#define LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                          \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN_A, JOIN_COMPARISON_OPERATOR)                 \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertNextGenJoinQuery("SELECT " #PROJECT                                           \
                           " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
                           " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A "");           \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                  \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE l_" #COLUMN_A " >  10 "); \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                  \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT " from table_probe LEFT JOIN table_hash ON l_" #COLUMN_A     \
        " " #JOIN_COMPARISON_OPERATOR " r_" #COLUMN_A " WHERE r_" #COLUMN_A " >  10 "); \
  }

// For not null query, if set NOT NULL on column ddl, the final project
// will go to cider_agg_id_proj_xx() instead of cider_agg_id_proj_xx_nullable(), So the
// returned null values are incorrect.
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest,
// LeftJoinArrowOneToOneSeqNoNullTest, *, int, =)  // NOLINT
LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNotNullJoinTest, ArrowOneToOneSeqNoNullLeftJoinTest2, *, bigint, =)  // NOLINT
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest,
// LeftJoinArrowOneToOneSeqNoNullableTest, *, int, =)  // NOLINT
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest,
// LeftJoinArrowOneToOneSeqNoNullableJoinTest2, *, bigint, =)  // NOLINT
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
// LeftJoinArrowOneToManyRandomNullJoinTest, *, int, =)  // NOLINT
// LEFT_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
// LeftJoinArrowOneToManyRandomNullJoinTest2, *, bigint, =)  // NOLINT

#define COMPLEX_HASH_JOIN_TEST_UNIT_ARROW_FORMAT(                                  \
    TEST_CLASS, UNIT_NAME, PROJECT, COLUMN, JOIN_COMPARISON_OPERATOR)              \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                  \
    GTEST_SKIP_("This kind of case is not supported.");                            \
    assertNextGenJoinQuery("SELECT " #PROJECT                                      \
                           " from table_probe JOIN table_hash ON l_" #COLUMN       \
                           " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");       \
    assertNextGenJoinQuery("SELECT " #PROJECT                                      \
                           " from table_probe INNER JOIN table_hash ON l_" #COLUMN \
                           " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");       \
    assertNextGenJoinQuery("SELECT " #PROJECT                                      \
                           " from table_probe LEFT JOIN table_hash ON l_" #COLUMN  \
                           " " #JOIN_COMPARISON_OPERATOR "  r_" #COLUMN "");       \
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
    GTEST_SKIP_("This kind of case is not supported.");                                 \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                                  \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_bigint =  r_bigint "); \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                                   \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_int = "     \
        "r_int ");                                                                      \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                               \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_int = 10 ");           \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                                 \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_bigint = "  \
        "10 ");                                                                         \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                              \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe JOIN table_hash ON l_int = r_int OR l_int IS NOT NULL ");    \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                                \
    assertNextGenJoinQuery(                                                             \
        "SELECT " #PROJECT                                                              \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint OR l_bigint IS " \
        "NOT NULL ");                                                                   \
  }
// TODO: (spevenhe) comment due to OR will fail back to loop join
// while AND is still hash join
DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, ORJoinConditionTest1, *)  // NOLINT
DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, ORJoinConditionTest2, *)  // NOLINT
DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                           ORJoinConditionWithAggTest1,
                                           SUM(l_bigint))
DOUBLE_JOIN_OR_CONDITION_TEST_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                           ORJoinConditionWithAggTest2,
                                           SUM(r_bigint))

#define DOUBLE_JOIN_AND_CONDITION_TEST_ARROW_FORMAT(TEST_CLASS, UNIT_NAME, PROJECT)      \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                        \
    GTEST_SKIP_("This kind of case is not supported.");                                  \
    /*INNER JOIN ON INTEGER OR FLOAT*/                                                   \
    assertNextGenJoinQuery(                                                              \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_bigint =  r_bigint "); \
    /*LEFT JOIN ON BIGINT OR DOUBLE*/                                                    \
    assertNextGenJoinQuery(                                                              \
        "SELECT " #PROJECT                                                               \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint AND l_int = "     \
        "r_int ");                                                                       \
    /*INNER JOIN ON INTEGER OR CONSTANT*/                                                \
    assertNextGenJoinQuery(                                                              \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_int = 10 ");           \
    /*LEFT JOIN ON BIGINT OR CONSTANT*/                                                  \
    assertNextGenJoinQuery(                                                              \
        "SELECT " #PROJECT                                                               \
        " from table_probe LEFT JOIN table_hash ON l_bigint = r_bigint AND l_bigint = "  \
        "10 ");                                                                          \
    /*INNER JOIN ON INTEGER AND NOT NULL*/                                               \
    assertNextGenJoinQuery(                                                              \
        "SELECT " #PROJECT                                                               \
        " from table_probe JOIN table_hash ON l_int = r_int AND l_int IS NOT NULL ");    \
    /*LEFT JOIN ON BIGINT AND NOT NULL*/                                                 \
    assertNextGenJoinQuery(                                                              \
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

#define HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(TEST_CLASS, UNIT_NAME, PROJECT)   \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                      \
    GTEST_SKIP_("This kind of case is not supported.");                                \
    /*FILTER ON PROBE TABLE'S COLUMN WHICH IS ALSO IN JOIN CONDITION*/                 \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE l_bigint <  "  \
        "10 ");                                                                        \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE l_bigint IS "  \
        "NOT NULL ");                                                                  \
    assertNextGenJoinQuery("SELECT " #PROJECT                                          \
                           " from table_probe JOIN table_hash ON l_bigint = r_bigint " \
                           "WHERE l_bigint IS NOT NULL AND l_bigint < 10 ");           \
    /*FILTER ON BUILD TABLE'S COLUMN WHICH IS ASLO IN JOIN CONDITION*/                 \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE r_bigint < "   \
        "10 ");                                                                        \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_bigint = r_bigint WHERE r_bigint IS "  \
        "NOT NULL ");                                                                  \
    assertNextGenJoinQuery("SELECT " #PROJECT                                          \
                           " from table_probe JOIN table_hash ON l_bigint = r_bigint " \
                           "WHERE r_bigint IS NOT NULL AND r_bigint < 10 ");           \
    /*FILTER ON PROBE TABLE COLUMN*/                                                   \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE l_double <  10 ");   \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE l_double IS NOT "    \
        "NULL ");                                                                      \
    assertNextGenJoinQuery("SELECT " #PROJECT                                          \
                           " from table_probe JOIN table_hash ON l_int = r_int "       \
                           "WHERE l_double IS NOT NULL AND l_double < 10 ");           \
    /*FILTER ON BUILD TABLE COLUMN*/                                                   \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE r_double < 10 ");    \
    assertNextGenJoinQuery(                                                            \
        "SELECT " #PROJECT                                                             \
        " from table_probe JOIN table_hash ON l_int = r_int WHERE r_double IS NOT "    \
        "NULL ");                                                                      \
    assertNextGenJoinQuery("SELECT " #PROJECT                                          \
                           " from table_probe JOIN table_hash ON l_int = r_int "       \
                           "WHERE r_double IS NOT NULL AND r_double < 10 ");           \
  }

HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToOneSeqNullableJoinTest, HashJoinWithFilterTest1, *)  // NOLINT
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest, HashJoinWithFilterTest2, *)  // NOLINT
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                             HashJoinWithFilterAndAggTest1,
                                             SUM(l_bigint))
HASH_JOIN_WITH_FILTER_TEST_UNIT_ARROW_FORMAT(CiderArrowOneToManyRandomNullableJoinTest,
                                             HashJoinWithFilterAndAggTest2,
                                             SUM(r_bigint))

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }

  return err;
}
