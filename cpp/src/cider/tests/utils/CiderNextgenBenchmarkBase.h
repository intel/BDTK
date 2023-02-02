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

#ifndef CIDER_TESTS_UTILS_NEXTGEN_BENCHMARK_BASE_H_
#define CIDER_TESTS_UTILS_NEXTGEN_BENCHMARK_BASE_H_

#include <gtest/gtest.h>

#include <string>

#include "tests/utils/CiderNextgenBenchmarkRunner.h"
#include "tests/utils/CiderNextgenQueryRunner.h"
#include "tests/utils/CiderNextgenTestBase.h"
#include "tests/utils/DuckDbQueryRunner.h"
#include "tests/utils/QueryArrowDataGenerator.h"

namespace cider::test::util {

// User can extend this class and add default setup function
class CiderNextgenBenchmarkBase : public CiderNextgenTestBase {
 public:
  void SetUp() override {
    if (input_array_ && input_schema_) {
      duckdb_query_runner_.createTableAndInsertArrowData(
          table_name_, create_ddl_, *input_array_, *input_schema_);
    }
    cider_nextgen_query_runner_ = std::make_shared<CiderNextgenBenchmarkRunner>();
    cider_nextgen_query_runner_->prepare(create_ddl_);
  }

  void benchSQL(const std::string& sql);

  void setupDdl(std::string& table_name, std::string& create_ddl) {
    table_name_ = table_name;
    create_ddl_ = create_ddl;
  }
};

}  // namespace cider::test::util

#endif  // CIDER_TESTS_UTILS_NEXTGEN_TEST_BASE_H_
