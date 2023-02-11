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

#include "tests/utils/CiderArrowChecker.h"
#include "tests/utils/CiderNextgenQueryRunner.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "exec/plan/parser/TypeUtils.h"

using namespace cider::test::util;

TEST(CiderProcessorQueryRunnerTest, filterProjectTest) {
  CiderNextgenQueryRunner cider_query_runner;

  std::string create_ddl = "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT)";
  cider_query_runner.prepare(create_ddl);

  struct ArrowArray* input_array;
  struct ArrowSchema* input_schema;
  QueryArrowDataGenerator::generateBatchByTypes(
      input_schema,
      input_array,
      99,
      {"col_a", "col_b"},
      {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(I64)});

  std::string sql_select_star = "SELECT col_a, col_b FROM table_test";
  struct ArrowArray output_array;
  struct ArrowSchema output_schema;
  cider_query_runner.runQueryOneBatch(
      sql_select_star, *input_array, *input_schema, output_array, output_schema);

  EXPECT_TRUE(CiderArrowChecker::checkArrowEq(
      input_array, &output_array, input_schema, &output_schema));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
