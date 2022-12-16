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

#ifndef MODULARSQL_CIDERBENCHMARKBASE_H
#define MODULARSQL_CIDERBENCHMARKBASE_H

#include "ArrowDataReader.h"
#include "benchmark/benchmark.h"
#include "tests/utils/QueryArrowDataGenerator.h"
class CiderBenchmarkBaseFixture : public benchmark::Fixture {
 public:
  // add members as needed
  CiderBenchmarkRunner runner;
  CiderBenchmarkBaseFixture() {}
};

std::shared_ptr<CiderBatch> genBatch(int row_num) {
  ArrowArray* array_ = nullptr;
  ArrowSchema* schema_ = nullptr;
  QueryArrowDataGenerator::generateBatchByTypes(
      schema_,
      array_,
      row_num,
      {"col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8"},
      {CREATE_SUBSTRAIT_TYPE(I32),
       CREATE_SUBSTRAIT_TYPE(I64),
       CREATE_SUBSTRAIT_TYPE(Fp32),
       CREATE_SUBSTRAIT_TYPE(Fp64),
       CREATE_SUBSTRAIT_TYPE(I32),
       CREATE_SUBSTRAIT_TYPE(I64),
       CREATE_SUBSTRAIT_TYPE(Fp32),
       CREATE_SUBSTRAIT_TYPE(Fp64)},
      {},
      GeneratePattern::Random,
      -1000'000,
      1000'000);
  return std::make_shared<CiderBatch>(
      schema_, array_, std::make_shared<CiderDefaultAllocator>());
}

#define GEN_BENCHMARK(FIXTURE_NAME, BATCH_SIZE, CASE, QUERY_STR, ITER)       \
  BENCHMARK_F(FIXTURE_NAME, CASE##_##BATCH_SIZE)(benchmark::State & state) { \
    input_batch = genBatch(BATCH_SIZE);                                      \
    runner.compile(QUERY_STR);                                               \
    for (auto _ : state) {                                                   \
      runner.runNextBatch(input_batch);                                      \
    }                                                                        \
  }

#define GEN_BENCHMARK_FROM_FILE(                                           \
    FIXTURE_NAME, CASE, ARROW_DATA_READER, FILE_NAME, QUERY_STR, COL_NAME) \
  BENCHMARK_F(FIXTURE_NAME, CASE)(benchmark::State & state) {              \
    input_batch = ARROW_DATA_READER(FILE_NAME, COL_NAME).read();           \
    runner.compile(QUERY_STR);                                             \
    for (auto _ : state) {                                                 \
      runner.runNextBatch(input_batch);                                    \
    }                                                                      \
  }

#endif  // MODULARSQL_CIDERBENCHMARKBASE_H
