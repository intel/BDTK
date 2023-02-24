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

#ifndef CIDER_DUCKDBQUERYRUNNER_H
#define CIDER_DUCKDBQUERYRUNNER_H

#include <vector>
#include "duckdb.hpp"

class DuckDbQueryRunner {
 public:
  DuckDbQueryRunner() {
    duckdb::DBConfig config;
    config.maximum_threads = 1;
    db_ = std::move(duckdb::DuckDB("", &config));
  }

  void createTableAndInsertArrowData(const std::string& table_name,
                                     const std::string& create_ddl,
                                     const ArrowArray& array,
                                     const ArrowSchema& schema);

  std::unique_ptr<::duckdb::MaterializedQueryResult> runSql(const std::string& sql);

 private:
  ::duckdb::DuckDB db_;
};

class DuckDbResultConvertor {
 public:
  static std::vector<
      std::pair<std::unique_ptr<struct ArrowArray>, std::unique_ptr<struct ArrowSchema>>>
  fetchDataToArrow(std::unique_ptr<::duckdb::MaterializedQueryResult>& result);
};

#endif  // CIDER_DUCKDBQUERYRUNNER_H
