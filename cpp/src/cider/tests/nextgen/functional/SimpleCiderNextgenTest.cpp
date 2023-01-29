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

// Extends CiderTestBase and create a (100 rows, 2 BIGINT columns) table for basic test.
class SimpleCiderArrowTest : public CiderTestBase {
 public:
  SimpleCiderArrowTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT NOT NULL, col_b BIGINT, col_c BIGINT);";
    QueryArrowDataGenerator::generateBatchByTypes(schema_,
                                                  array_,
                                                  100,
                                                  {"col_a", "col_b", "col_c"},
                                                  {
                                                      CREATE_SUBSTRAIT_TYPE(I64),
                                                      CREATE_SUBSTRAIT_TYPE(I64),
                                                      CREATE_SUBSTRAIT_TYPE(I64),
                                                  },
                                                  {0, 1, 2});
  }
};

TEST_F(SimpleCiderArrowTest, selectTest) {
  assertQueryArrow("SELECT * FROM test");

  assertQueryArrow("SELECT col_a FROM test");

  assertQueryArrow("SELECT col_b FROM test");

  assertQueryArrow("SELECT col_a, col_b FROM test");

  assertQueryArrow("SELECT col_b, col_a FROM test");
}

TEST_F(SimpleCiderArrowTest, nullTest) {
  // select * with single null
  assertQueryArrow("SELECT * FROM test WHERE col_a IS NULL");
  assertQueryArrow("SELECT * FROM test WHERE col_b IS NULL");
  assertQueryArrow("SELECT * FROM test WHERE col_c IS NULL");

  // select * with single not null
  assertQueryArrow("SELECT * FROM test WHERE col_a IS NOT NULL");
  assertQueryArrow("SELECT * FROM test WHERE col_b IS NOT NULL");
  assertQueryArrow("SELECT * FROM test WHERE col_c IS NOT NULL");

  // select all with single null
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_a IS NULL");
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_b IS NULL");
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_c IS NULL");

  // select one column with single null
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_b IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_b IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_b IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_c IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_c IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_c IS NULL");

  // select all with single not null
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_a IS NOT NULL");
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_b IS NOT NULL");
  assertQueryArrow("SELECT col_a, col_b, col_c FROM test WHERE col_c IS NOT NULL");

  // select one column with single not null
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NOT NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NOT NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_b IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_b IS NOT NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_b IS NOT NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_c IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_c IS NOT NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_c IS NOT NULL");

  // select one column with two conditions
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL AND col_b IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL AND col_c IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL AND col_c IS NOT NULL");
  assertQueryArrow(
      "SELECT col_a FROM test WHERE col_a IS NOT NULL AND col_c IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL AND col_b IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL AND col_c IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL AND col_c IS NOT NULL");
  assertQueryArrow(
      "SELECT col_b FROM test WHERE col_a IS NOT NULL AND col_c IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL AND col_b IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL AND col_c IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL AND col_c IS NOT NULL");
  assertQueryArrow(
      "SELECT col_c FROM test WHERE col_a IS NOT NULL AND col_c IS NOT NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL OR col_b IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL OR col_c IS NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NULL OR col_c IS NOT NULL");
  assertQueryArrow("SELECT col_a FROM test WHERE col_a IS NOT NULL OR col_c IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL OR col_b IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL OR col_c IS NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NULL OR col_c IS NOT NULL");
  assertQueryArrow("SELECT col_b FROM test WHERE col_a IS NOT NULL OR col_c IS NOT NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL OR col_b IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL OR col_c IS NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NULL OR col_c IS NOT NULL");
  assertQueryArrow("SELECT col_c FROM test WHERE col_a IS NOT NULL OR col_c IS NOT NULL");

  // select * column with three conditions
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL AND col_b IS NULL AND col_c IS NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL AND col_b IS NULL AND col_c IS NOT NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL AND col_b IS NOT NULL AND col_c IS NOT "
      "NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NOT NULL AND col_b IS NOT NULL AND col_c IS NOT "
      "NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL OR col_b IS NULL OR col_c IS NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL OR col_b IS NULL OR col_c IS NOT NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NULL OR col_b IS NOT NULL OR col_c IS NOT "
      "NULL");
  assertQueryArrow(
      "SELECT * FROM test WHERE col_a IS NOT NULL OR col_b IS NOT NULL OR col_c IS NOT "
      "NULL");
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
