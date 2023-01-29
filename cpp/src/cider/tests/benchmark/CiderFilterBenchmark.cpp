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

#include "tests/utils/CiderBenchmarkRunner.h"
#include "tests/utils/QueryDataGenerator.h"

#include "CiderBenchmarkBase.h"

class CiderFilterBenchmark : public CiderBenchmarkBaseFixture {
 public:
  CiderFilterBenchmark() {
    runner.prepare(
        "CREATE TABLE test(col_1 INTEGER, col_2 BIGINT, col_3 FLOAT, col_4 DOUBLE, "
        "col_5 INTEGER, col_6 BIGINT, col_7 FLOAT, col_8 DOUBLE);");
  }

  std::shared_ptr<CiderBatch> input_batch;
};

#define GEN_FILTER_BENCHMARK(FIXTURE_NAME, FILTER_CASE, QUERY_STR) \
  GEN_BENCHMARK(FIXTURE_NAME, 1000, FILTER_CASE, QUERY_STR, 0)     \
  GEN_BENCHMARK(FIXTURE_NAME, 10000, FILTER_CASE, QUERY_STR, 0)    \
  GEN_BENCHMARK(FIXTURE_NAME, 100000, FILTER_CASE, QUERY_STR, 0)

char* sql_gt_int_constant("SELECT * FROM test where col_1 > 0");
char* sql_gt_long_constant("SELECT * FROM test where col_2 > 0");
char* sql_gt_float_constant("SELECT * FROM test where col_3 > 0");
char* sql_gt_double_constant("SELECT * FROM test where col_4 > 0");
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_I32, sql_gt_int_constant);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_I64, sql_gt_long_constant);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_F32, sql_gt_float_constant);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_F64, sql_gt_double_constant);

char* sql_gt_int_int("SELECT * FROM test where col_1 > col_5");
char* sql_gt_long_long("SELECT * FROM test where col_2 > col_6");
char* sql_gt_float_float("SELECT * FROM test where col_3 > col_7");
char* sql_gt_double_double("SELECT * FROM test where col_4 > col_8");
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_I32_I32, sql_gt_int_int);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_I64_I64, sql_gt_long_long);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_F32_F32, sql_gt_float_float);
GEN_FILTER_BENCHMARK(CiderFilterBenchmark, GT_F64_F64, sql_gt_double_double);

// Run the benchmark
BENCHMARK_MAIN();
