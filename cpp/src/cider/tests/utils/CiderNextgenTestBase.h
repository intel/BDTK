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

#ifndef CIDER_TESTS_UTILS_NEXTGEN_TEST_BASE_H_
#define CIDER_TESTS_UTILS_NEXTGEN_TEST_BASE_H_

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <string>

#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/CiderNextgenQueryRunner.h"
#include "tests/utils/DuckDbQueryRunner.h"
#include "tests/utils/QueryArrowDataGenerator.h"

namespace cider::test::util {

// User can extend this class and add default setup function
class CiderNextgenTestBase : public testing::Test {
 public:
  void SetUp() override {
    if (input_array_ && input_schema_) {
      duckdb_query_runner_.createTableAndInsertArrowData(
          table_name_,
          duckdb_create_ddl_ == "" ? create_ddl_ : duckdb_create_ddl_,
          *input_array_,
          *input_schema_);
    }
    cider_nextgen_query_runner_->prepare(create_ddl_);
  }

  // each assert call will reset DuckDbQueryRunner and CiderQueryRunner
  void assertQuery(const std::string& sql,
                   const std::string& json_file_or_sql = "",
                   const bool ignore_order = false);

  void assertQuery(const std::string& sql,
                   const struct ArrowArray* array,
                   const struct ArrowSchema* schema,
                   bool ignore_order = false);

  void assertQueryIgnoreOrder(const std::string& sql,
                              const std::string& json_file_or_sql = "") {
    assertQuery(sql, json_file_or_sql, true);
  }
  bool executeIncorrectQuery(const std::string& wrong_sql);

  void setupDdl(std::string& table_name, std::string& create_ddl) {
    table_name_ = table_name;
    create_ddl_ = create_ddl;
  }

  void setCodegenOptions(cider::exec::nextgen::context::CodegenOptions& codegen_options) {
    codegen_options_ = codegen_options;
  }

 protected:
  std::string table_name_;
  std::string create_ddl_;
  std::string duckdb_create_ddl_ = "";
  ArrowArray* input_array_{nullptr};
  ArrowSchema* input_schema_{nullptr};
  DuckDbQueryRunner duckdb_query_runner_;
  CiderNextgenQueryRunnerPtr cider_nextgen_query_runner_ =
      std::make_shared<CiderNextgenQueryRunner>();
  cider::exec::nextgen::context::CodegenOptions codegen_options_ = {};
};

class CiderJoinNextgenTestBase : public CiderNextgenTestBase {
 public:
  void SetUp() override {
    duckdb_query_runner_.createTableAndInsertArrowData(
        table_name_, create_ddl_, *input_array_, *input_schema_);
    duckdb_query_runner_.createTableAndInsertArrowData(
        build_table_name_, build_table_ddl_, *build_array_, *build_schema_);
    cider_nextgen_query_runner_->prepare(create_ddl_ + " " + build_table_ddl_);
  }

  void assertJoinQuery(const std::string& sql,
                       const std::string& json_file = "",
                       const bool ignore_order = true);

  virtual void resetHashTable() {}

  void assertNextGenJoinQuery(const std::string& sql,
                              const std::string& json_file = "",
                              const bool ignore_order = true) {
    assertJoinQuery(sql, json_file, ignore_order);
    resetHashTable();
  }

 protected:
  std::string build_table_name_;
  std::string build_table_ddl_;
  ArrowArray* build_array_{nullptr};
  ArrowSchema* build_schema_{nullptr};
};

class CiderStandaloneNextgenTestBase : public CiderNextgenTestBase {
 public:
  void SetUp() override { cider_nextgen_query_runner_->prepare(create_ddl_); }
};

}  // namespace cider::test::util

#endif  // CIDER_TESTS_UTILS_NEXTGEN_TEST_BASE_H_
