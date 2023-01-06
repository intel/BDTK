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

class CiderArrowCaseWhenSequenceTestBase : public CiderTestBase {
 public:
  CiderArrowCaseWhenSequenceTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        10,
        {"col_int", "col_bigint", "col_double", "col_float", "col_str"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)});
  }
};

class CiderArrowCaseWhenSequenceWithNullTestBase : public CiderTestBase {
 public:
  CiderArrowCaseWhenSequenceWithNullTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        10,
        {"col_int", "col_bigint", "col_double", "col_float", "col_str"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {3, 3, 3, 3, 3});
  }
};

class CiderArrowCaseWhenRandomWithNullTestBase : public CiderTestBase {
 public:
  CiderArrowCaseWhenRandomWithNullTestBase() {
    table_name_ = "test";
    create_ddl_ =
        R"(CREATE TABLE test(col_int INTEGER, col_bigint BIGINT, col_double DOUBLE, col_float FLOAT, col_str VARCHAR(10));)";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        10,
        {"col_int", "col_bigint", "col_double", "col_float", "col_str"},
        {CREATE_SUBSTRAIT_TYPE(I32),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(Fp64),
         CREATE_SUBSTRAIT_TYPE(Fp32),
         CREATE_SUBSTRAIT_TYPE(Varchar)},
        {2, 2, 2, 2, 2},
        GeneratePattern::Random,
        2,
        102);
  }
};

// The IF function is actually a language construct that is equivalent to the following
// CASE expression: CASE
//    WHEN condition THEN true_value
//    [ ELSE false_value ]
// END
// The Scalar Function IF from presto will be transalted to if-then expression in
// substrait plan to cider, so tests are between IF function and case when
#define IF_ARROW_TEST(TEST_CLASS, UNIT_NAME)                                            \
  TEST_F(TEST_CLASS, UNIT_NAME) {                                                       \
    assertQueryArrow("SELECT IF(col_int = 1 ,10, 1) FROM test",                         \
                     "SELECT CASE WHEN col_int = 1 THEN 10 ELSE 1 END FROM test");      \
    assertQueryArrow(                                                                   \
        "SELECT IF(col_int > 20, col_bigint, col_double) FROM test",                    \
        "SELECT CASE WHEN col_int > 20 THEN col_bigint ELSE col_double END FROM test"); \
    GTEST_SKIP_(                                                                        \
        "TODO(Haiwei): Agg is not supported now, require future validation of the "     \
        "following case.");                                                             \
    assertQueryArrow(                                                                   \
        "SELECT SUM(IF(col_int > 20, col_double, 5)) FROM test",                        \
        "SELECT SUM(CASE WHEN col_int > 20 THEN col_double ELSE 5 END) FROM test");     \
    assertQueryArrow(                                                                   \
        "SELECT SUM(IF(col_double > 20, 5, col_double)) FROM test",                     \
        "SELECT SUM(CASE WHEN col_double > 20 THEN 5 ELSE col_double END) FROM test");  \
  }

IF_ARROW_TEST(CiderArrowCaseWhenSequenceTestBase, ifNotNullTestForArrow);
IF_ARROW_TEST(CiderArrowCaseWhenSequenceWithNullTestBase, ifSeqNullTestForArrow);
IF_ARROW_TEST(CiderArrowCaseWhenRandomWithNullTestBase, ifRandomNullTestForArrow);

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  logger::LogOptions log_options(argv[0]);
  // log_options.parse_command_line(argc, argv);
  // log_options.max_files_ = 0;  // stderr only by default
  log_options.severity_ = logger::Severity::DEBUG4;
  log_options.set_options();  // update default values
  logger::init(log_options);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
