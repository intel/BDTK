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

#ifndef MODULARSQL_CIDERPROFLINGBASE_H
#define MODULARSQL_CIDERPROFLINGBASE_H

#include "tests/utils/CiderTestBase.h"
#include "util/measure.h"

extern bool g_enable_debug_timer;

class CiderProfilingBase : public CiderTestBase {
 public:
  void benchSQL(const std::string& sql) {
    g_enable_debug_timer = true;
    // duckdb
    {
      INJECT_TIMER(DuckDb);
      auto duck_res = duckDbQueryRunner_.runSql(sql);
      auto duck_res_batch = DuckDbResultConvertor::fetchDataToCiderBatch(duck_res);
    }

    // cider
    {
      INJECT_TIMER(Cider);
      auto cider_res_batch = ciderQueryRunner_.runQueryOneBatch(sql, input_[0]);
    }
  }
};

#endif  // MODULARSQL_CIDERPROFLINGBASE_H
