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
class CiderConstantQueryTestBase : public CiderTestBase {
 public:
  CiderConstantQueryTestBase() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT, col_b BIGINT);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        100,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)},
        {2, 2}))};
  }
};

TEST_F(CiderConstantQueryTestBase, selectConstantTest) {
  assertQuery("SELECT true FROM test");
  assertQuery("SELECT 1 FROM test");
  assertQuery("SELECT true FROM test where col_a > 10");
  assertQuery("SELECT true, col_b FROM test where col_a > 10");
}

TEST_F(CiderConstantQueryTestBase, selectOperatorTest) {
  assertQuery("SELECT 3 < 2 FROM test");
  assertQuery("SELECT 3 > 2 FROM test");
  assertQuery("SELECT 3 = 2 FROM test");
  assertQuery("SELECT 3 <= 2  FROM test");
  assertQuery("SELECT 3 >= 2  FROM test");
  assertQuery("SELECT 3 <> 2 FROM test");
  assertQuery("SELECT CAST(null AS boolean) FROM test");
  assertQuery("SELECT NOT CAST(null AS boolean) FROM test");
  assertQuery("SELECT CAST(null AS boolean) AND true FROM test");
  assertQuery("SELECT CAST(null AS boolean) AND false FROM test");
  assertQuery("SELECT CAST(null AS boolean) OR true FROM test");
  assertQuery("SELECT CAST(null AS boolean) OR false FROM test");
  assertQuery("SELECT null and true FROM test");
  assertQuery("SELECT null and false FROM test");
  assertQuery("SELECT null or true FROM test");
  assertQuery("SELECT null or false FROM test");
  assertQuery("SELECT col_a = 2 AND col_b > 10 FROM test");
  assertQuery("SELECT col_a = 2 OR col_b > 10 FROM test");
}

class CiderConstantQueryArrowTest : public CiderTestBase {
 public:
  CiderConstantQueryArrowTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT, col_b BIGINT);";

    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        100,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)},
        {2, 2});
  }
};

TEST_F(CiderConstantQueryArrowTest, selectConstantTest) {
  assertQueryArrow("SELECT true FROM test");
  assertQueryArrow("SELECT 1 FROM test");
  assertQueryArrow("SELECT true FROM test where col_a > 10");
  assertQueryArrow("SELECT true, col_b FROM test where col_a > 10");
}

TEST_F(CiderConstantQueryArrowTest, selectOperatorTest) {
  assertQueryArrow("SELECT 3 < 2 FROM test");
  assertQueryArrow("SELECT 3 > 2 FROM test");
  assertQueryArrow("SELECT 3 = 2 FROM test");
  assertQueryArrow("SELECT 3 <= 2  FROM test");
  assertQueryArrow("SELECT 3 >= 2  FROM test");
  assertQueryArrow("SELECT 3 <> 2 FROM test");
  assertQueryArrow("SELECT CAST(null AS boolean) FROM test");
  assertQueryArrow("SELECT NOT CAST(null AS boolean) FROM test");
  assertQueryArrow("SELECT CAST(null AS boolean) AND true FROM test");
  assertQueryArrow("SELECT CAST(null AS boolean) AND false FROM test");
  assertQueryArrow("SELECT CAST(null AS boolean) OR true FROM test");
  assertQueryArrow("SELECT CAST(null AS boolean) OR false FROM test");
  assertQueryArrow("SELECT null and true FROM test");
  assertQueryArrow("SELECT null and false FROM test");
  assertQueryArrow("SELECT null or true FROM test");
  assertQueryArrow("SELECT null or false FROM test");
  assertQueryArrow("SELECT col_a = 2 AND col_b > 10 FROM test");
  assertQueryArrow("SELECT col_a = 2 OR col_b > 10 FROM test");
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
