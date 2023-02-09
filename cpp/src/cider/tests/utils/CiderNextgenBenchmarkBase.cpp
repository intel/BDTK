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

#include <string>

#include "tests/utils/CiderNextgenBenchmarkBase.h"
#include "tests/utils/CiderNextgenQueryRunner.h"
#include "tests/utils/DuckDbQueryRunner.h"
#include "tests/utils/QueryArrowDataGenerator.h"
#include "util/measure.h"

namespace cider::test::util {

void CiderNextgenBenchmarkBase::benchSQL(const std::string& sql) {
  g_enable_debug_timer = true;
  // duckdb
  {
    INJECT_TIMER(DuckDb);
    auto duck_res = duckdb_query_runner_.runSql(sql);
  }
  {
    INJECT_TIMER(Nextgen);
    struct ArrowArray output_array;
    struct ArrowSchema output_schema;

    auto file_or_sql = sql;
    // auto file_or_sql = json_file.size() ? json_file : sql;
    cider_nextgen_query_runner_->runQueryOneBatch(
        file_or_sql, *input_array_, *input_schema_, output_array, output_schema);
  }
}
}  // namespace cider::test::util
