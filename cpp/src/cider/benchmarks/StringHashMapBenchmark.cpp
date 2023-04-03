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

// #pragma GCC optimize(1)
// #pragma GCC optimize(2)
// #pragma GCC optimize(3, "Ofast", "inline")

#include <random>
#include <vector>

#include <benchmark/benchmark.h>
#include <common/base/StringRef.h>
#include <common/hashtable/HashTableAllocator.h>
#include <common/hashtable/StringHashMap.h>
#include <folly/container/F14Map.h>
#include <tests/utils/Utils.h>

using namespace cider::hashtable;

enum GeneratePattern { Sequence, Random };

size_t s_str_len = 10;
size_t l_str_len = 500;

#define N_MAX std::numeric_limits<T>::max()
#define N_MIN std::numeric_limits<T>::min()

#define NO_INLINE __attribute__((__noinline__))

static std::string sequence_string(size_t length, size_t index) {
  auto randchar = [index]() -> char {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t mod = (sizeof(charset) - 1);
    return charset[index % mod];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

static std::vector<std::string> prepare_string_data(size_t length, size_t row_num) {
  std::vector<std::string> data(row_num);
  for (size_t i = 0; i < row_num; i++) {
    data.push_back(sequence_string(length, i));
  }
  return data;
}

template <typename KeyType>
void NO_INLINE bench_std_tree(const std::vector<KeyType>& data) {
  std::map<KeyType, int8_t> map;

  for (auto key : data) {
    map.emplace(key, 1);
  }
}

template <typename KeyType>
void NO_INLINE bench_std_unordered(const std::vector<KeyType>& data) {
  std::unordered_map<KeyType, int8_t> map;

  for (auto key : data) {
    map.emplace(key, 1);
  }
}

template <typename KeyType>
void NO_INLINE bench_folly_f14(const std::vector<KeyType>& data) {
  folly::F14FastMap<KeyType, int8_t> map;
  for (auto key : data) {
    map[key] = 1;
  }
}

void NO_INLINE bench_cider_short_string(const std::vector<std::string>& data) {
  StringHashMap<int8_t> map;
  for (auto key : data) {
    map[key] = 1;
  }
}

void NO_INLINE bench_cider_long_string(const std::vector<std::string>& data) {
  HashMapWithSavedHash<StringRef, int8_t> map;
  for (auto key : data) {
    map[key] = 1;
  }
}

static void BM_std_short_string_treemap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(s_str_len, row_num);
  for (auto _ : state) {
    bench_std_tree<std::string>(data);
  }
}

static void BM_std_short_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(s_str_len, row_num);
  for (auto _ : state) {
    bench_std_unordered<std::string>(data);
  }
}

static void BM_folly_f14_short_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(s_str_len, row_num);
  for (auto _ : state) {
    bench_folly_f14<std::string>(data);
  }
}

static void BM_cider_short_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(s_str_len, row_num);
  for (auto _ : state) {
    bench_cider_short_string(data);
  }
}

static void BM_std_long_string_treemap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(l_str_len, row_num);
  for (auto _ : state) {
    bench_std_tree<std::string>(data);
  }
}

static void BM_std_long_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(l_str_len, row_num);
  for (auto _ : state) {
    bench_std_unordered<std::string>(data);
  }
}

static void BM_folly_f14_long_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(l_str_len, row_num);
  for (auto _ : state) {
    bench_folly_f14<std::string>(data);
  }
}

static void BM_cider_long_string_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<std::string> data = prepare_string_data(l_str_len, row_num);
  for (auto _ : state) {
    bench_cider_long_string(data);
  }
}

BENCHMARK(BM_std_short_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_std_short_string_treemap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_folly_f14_short_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_cider_short_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);

BENCHMARK(BM_std_long_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_std_long_string_treemap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_folly_f14_long_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);
BENCHMARK(BM_cider_long_string_hashmap)->RangeMultiplier(10)->Range(10000, 1000000);

BENCHMARK_MAIN();
