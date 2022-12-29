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

#include "benchmark/benchmark.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

using arrow::util::Codec;

namespace icl {
namespace bench {

std::vector<uint8_t> ReadCompressibleData(int data_size) {
  std::vector<uint8_t> data(data_size);
  std::ifstream calgary("calgary.tar", std::ifstream::binary);
  if (calgary.is_open()) {
    calgary.read(reinterpret_cast<char*>(data.data()), data_size);
    calgary.close();
  }

  return data;
}

static void BM_Compression(arrow::Compression::type compression,
                           const std::vector<uint8_t>& data,
                           benchmark::State& state) {  // NOLINT non-const reference
  std::unique_ptr<Codec> codec;
  if (compression != arrow::Compression::SNAPPY) {
    ASSERT_OK_AND_ASSIGN(codec, Codec::Create(compression, 1));
  } else {
    ASSERT_OK_AND_ASSIGN(codec, Codec::Create(compression));
  }

  int max_compressed_len =
      static_cast<int>(codec->MaxCompressedLen(data.size(), data.data()));
  std::vector<uint8_t> compressed_data(max_compressed_len);

  while (state.KeepRunning()) {
    int64_t compressed_size = 0;
    ASSERT_OK_AND_ASSIGN(
        compressed_size,
        codec->Compress(
            data.size(), data.data(), compressed_data.size(), compressed_data.data()));
    state.counters["ratio"] = benchmark::Counter(
        static_cast<double>(data.size()) / static_cast<double>(compressed_size),
        benchmark::Counter::kAvgThreads);
  }
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <arrow::Compression::type COMPRESSION>
static void Compression(benchmark::State& state) {
  const int64_t data_size = state.range(0);
  auto data = ReadCompressibleData(data_size);
  BM_Compression(COMPRESSION, data, state);
}

static void BM_Decompression(arrow::Compression::type compression,
                             const std::vector<uint8_t>& data,
                             benchmark::State& state) {  // NOLINT non-const reference
  std::unique_ptr<Codec> codec;
  if (compression != arrow::Compression::SNAPPY) {
    ASSERT_OK_AND_ASSIGN(codec, Codec::Create(compression, 1));
  } else {
    ASSERT_OK_AND_ASSIGN(codec, Codec::Create(compression));
  }
  int max_compressed_len =
      static_cast<int>(codec->MaxCompressedLen(data.size(), data.data()));

  std::vector<uint8_t> compressed_data(max_compressed_len);
  auto compressed_size =
      codec
          ->Compress(data.size(), data.data(), max_compressed_len, compressed_data.data())
          .ValueOrDie();
  compressed_data.resize(compressed_size);
  state.counters["ratio"] = benchmark::Counter(
      static_cast<double>(data.size()) / static_cast<double>(compressed_data.size()),
      benchmark::Counter::kAvgThreads);
  std::vector<uint8_t> decompressed_data(data.size());
  int64_t decompressed_size = 0;

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(decompressed_size,
                         codec->Decompress(compressed_data.size(),
                                           compressed_data.data(),
                                           decompressed_data.size(),
                                           decompressed_data.data()));
    benchmark::DoNotOptimize(decompressed_size);
  }
  ARROW_CHECK(decompressed_size == static_cast<int64_t>(data.size()));
  state.SetBytesProcessed(state.iterations() * data.size());
}

template <arrow::Compression::type COMPRESSION>
static void Decompression(benchmark::State& state) {
  const int64_t data_size = state.range(0);
  auto data = ReadCompressibleData(data_size);
  BM_Decompression(COMPRESSION, data, state);
}

static const std::vector<int32_t> kBatchSizes = {128 * 1024,
                                                 256 * 1024,
                                                 512 * 1024,
                                                 1 * 1024 * 1024,
                                                 2 * 1024 * 1024,
                                                 3 * 1024 * 1024};

static void SetBenchmarkArgs(benchmark::internal::Benchmark* b) {
  for (const int32_t s : kBatchSizes) {
    b->Args({s});
  }
  b->Threads(1);
}

#define COMPRESSION_BENCHMARK(FuncName, Compression)                  \
  BENCHMARK_TEMPLATE(FuncName, Compression)->Apply(SetBenchmarkArgs); \
  BENCHMARK_TEMPLATE(FuncName, Compression)                           \
      ->Arg(1024 * 1024)                                              \
      ->RangeMultiplier(2)                                            \
      ->ThreadRange(1, 128);

COMPRESSION_BENCHMARK(Compression, arrow::Compression::GZIP);
COMPRESSION_BENCHMARK(Compression, arrow::Compression::SNAPPY);
COMPRESSION_BENCHMARK(Compression, arrow::Compression::ZSTD);
COMPRESSION_BENCHMARK(Decompression, arrow::Compression::GZIP);
COMPRESSION_BENCHMARK(Decompression, arrow::Compression::SNAPPY);
COMPRESSION_BENCHMARK(Decompression, arrow::Compression::ZSTD);

}  // namespace bench
}  // namespace icl
