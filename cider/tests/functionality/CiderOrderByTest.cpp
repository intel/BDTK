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
#include "CiderTestBase.h"

class CiderOrderByTest : public CiderTestBase {
 public:
  CiderOrderByTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT, col_c BIGINT, col_d BIGINT, "
        "col_e BIGINT, col_f BIGINT);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_a", "col_b", "col_c", "col_d", "col_e", "col_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64)},
        {0, 0, 0, 2, 2, 2},
        GeneratePattern::Random,
        1,
        10))};
  }
};

TEST_F(CiderOrderByTest, SelectColumnOrderByTest) {
  assertQuery("SELECT col_a FROM table_test ORDER BY 1");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a ASC");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a DESC");
}

TEST_F(CiderOrderByTest, AggGroupOrderByTest) {
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY 1");
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a");
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a ASC");
  assertQuery(
      "SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a DESC");
}

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
