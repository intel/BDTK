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

#ifndef CIDER_CIDERTESTBASE_H
#define CIDER_CIDERTESTBASE_H

#include <gtest/gtest.h>

#include <string>
#include "CiderQueryRunner.h"
#include "DuckDbQueryRunner.h"
#include "QueryDataGenerator.h"
#include "cider/CiderBatch.h"
#include "cider/CiderException.h"
// User can extend this class and add default setup function
class CiderTestBase : public testing::Test {
 public:
  void SetUp() override {
    duckDbQueryRunner_.createTableAndInsertData(table_name_, create_ddl_, input_);
    ciderQueryRunner_.prepare(create_ddl_);
  }
  // each assert call will reset DuckDbQueryRunner and CiderQueryRunner
  void assertQuery(const std::string& sql,
                   const std::string& json_file_or_sql = "",
                   const bool ignore_order = false);
  // compare cider query result with a specified batch when duckdb doesn't support the op
  void assertQuery(const std::string& sql,
                   const std::shared_ptr<CiderBatch> expected_batch,
                   const bool ignoreOrder = false);
  // a method for test count distinct with multi batch case
  void assertQueryForCountDistinct(
      const std::string& sql,
      const std::vector<std::shared_ptr<CiderBatch>>& expected_batches);
  void assertQueryIgnoreOrder(const std::string& sql, const std::string& json_file = "");
  // execute a wrong query to check if will throw exception
  bool executeIncorrectQuery(const std::string& wrong_sql);
  void setupDdl(std::string& table_name, std::string& create_ddl) {
    table_name_ = table_name;
    create_ddl_ = create_ddl;
  }
  void setupInput(std::vector<std::shared_ptr<CiderBatch>>& input) { input_ = input; }

 protected:
  std::string table_name_;
  std::string create_ddl_;
  std::vector<std::shared_ptr<CiderBatch>> input_;
  DuckDbQueryRunner duckDbQueryRunner_;
  CiderQueryRunner ciderQueryRunner_;
};

class CiderJoinTestBase : public CiderTestBase {
 public:
  void SetUp() override {
    duckDbQueryRunner_.createTableAndInsertData(table_name_, create_ddl_, input_);
    duckDbQueryRunner_.createTableAndInsertData(
        build_table_name_, build_table_ddl_, {build_table_});

    ciderQueryRunner_.prepare(create_ddl_ + " " + build_table_ddl_);
  }

  void assertJoinQuery(const std::string& sql,
                       const std::string& json_file = "",
                       const bool compare_value = true);

  void assertJoinQueryRowEqual(const std::string& sql,
                               const std::string& json_file = "",
                               const bool ignore_order = true);

  virtual void resetHashTable() {}

  void assertJoinQueryAndReset(const std::string& sql,
                               const std::string& json_file = "",
                               const bool compare_value = true) {
    assertJoinQuery(sql, json_file, compare_value);
    resetHashTable();
  }

  void assertJoinQueryRowEqualAndReset(const std::string& sql,
                                       const std::string& json_file = "",
                                       const bool ignore_order = true) {
    assertJoinQueryRowEqual(sql, json_file, ignore_order);
    resetHashTable();
  }

 protected:
  std::string build_table_name_;
  std::string build_table_ddl_;
  std::shared_ptr<CiderBatch> build_table_;
};

#endif  // CIDER_CIDERTESTBASE_H
