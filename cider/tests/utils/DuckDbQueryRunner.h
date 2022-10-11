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

#ifndef CIDER_DUCKDBQUERYRUNNER_H
#define CIDER_DUCKDBQUERYRUNNER_H

#include <vector>
#include "cider/CiderBatch.h"
#include "duckdb.hpp"

class DuckDbQueryRunner {
 public:
  DuckDbQueryRunner() {
    duckdb::DBConfig config;
    config.maximum_threads = 1;
    db_ = std::move(duckdb::DuckDB("", &config));
  }

  // create a basic table and insert data for test, only int32 type support.
  void createTableAndInsertData(const std::string& table_name,
                                const std::string& create_ddl,
                                const std::vector<std::vector<int32_t>>& table_data,
                                bool use_tpch_schema = false,
                                const std::vector<std::vector<bool>>& null_data = {});

  void createTableAndInsertData(const std::string& table_name,
                                const std::string& create_ddl,
                                const std::vector<std::shared_ptr<CiderBatch>>& data);

  void createTableAndInsertData(const std::string& table_name,
                                const std::string& create_ddl,
                                const std::shared_ptr<CiderBatch>& data);

  std::unique_ptr<::duckdb::MaterializedQueryResult> runSql(const std::string& sql);

 private:
  ::duckdb::DuckDB db_;
  void appendNullableTableData(::duckdb::Connection& con,
                               const std::string& table_name,
                               const std::vector<std::vector<int32_t>>& table_data,
                               const std::vector<std::vector<bool>>& null_data);
  void appendTableData(::duckdb::Connection& con,
                       const std::string& table_name,
                       const std::vector<std::vector<int32_t>>& table_data);
};

class DuckDbResultConvertor {
 public:
  static std::vector<std::shared_ptr<CiderBatch>> fetchDataToCiderBatch(
      std::unique_ptr<::duckdb::MaterializedQueryResult>& result);
  static std::vector<std::shared_ptr<CiderBatch>> fetchDataToArrowFormattedCiderBatch(
      std::unique_ptr<::duckdb::MaterializedQueryResult>& result);

 private:
  static CiderBatch fetchOneBatch(std::unique_ptr<duckdb::DataChunk>& chunk);
  static std::unique_ptr<CiderBatch> fetchOneArrowFormattedBatch(
      std::unique_ptr<duckdb::DataChunk>& chunk);
};

#endif  // CIDER_DUCKDBQUERYRUNNER_H
