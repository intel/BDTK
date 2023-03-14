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

#include <random>
#include <vector>

#include <benchmark/benchmark.h>
#include <common/hashtable/FixedHashMap.h>
#include <common/hashtable/HashMap.h>
#include <common/hashtable/HashTable.h>
#include <common/hashtable/HashTableAllocator.h>
#include <folly/container/F14Map.h>
#include <tests/utils/Utils.h>

using namespace cider::hashtable;

enum GeneratePattern { Sequence, Random };

#define N_MAX std::numeric_limits<T>::max()
#define N_MIN std::numeric_limits<T>::min()

#define NO_INLINE __attribute__((__noinline__))

template <typename KeyType, typename Map>
int32_t NO_INLINE bench(const std::vector<KeyType>& data) {
  Map map;

  for (auto key : data) {
    typename Map::LookupResult it;
    bool inserted;

    map.emplace(key, it, inserted);
    if (inserted)
      it->getMapped() = 1;
    else
      ++it->getMapped();
  }

  for (auto key : data) {
    auto it = map.find(key);
    auto curr = ++it;
    if (curr)
      curr->getMapped();
  }

  return map.getCollisions();
}

template <typename T>
static std::tuple<std::vector<T>, std::vector<bool>> generateAndFillVector(
    const size_t row_num,
    const GeneratePattern pattern,
    const int32_t
        null_chance,  // Null chance for each column, -1 represents for unnullable
                      // column, 0 represents for nullable column but all data is not
                      // null, 1 represents for all rows are null, values >= 2 means
                      // each row has 1/x chance to be null.
    const T value_min = N_MIN,
    const T value_max = N_MAX) {
  std::vector<T> col_data(row_num);
  std::vector<bool> null_data(row_num);
  std::mt19937 rng(std::random_device{}());  // NOLINT
  switch (pattern) {
    case GeneratePattern::Sequence:
      for (auto i = 0; i < row_num; ++i) {
        null_data[i] = Random::oneIn(null_chance, rng) ? (col_data[i] = N_MIN, true)
                                                       : (col_data[i] = i, false);
      }
      break;
    case GeneratePattern::Random:
      if (std::is_integral<T>::value) {
        for (auto i = 0; i < col_data.size(); ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = N_MIN, true)
                             : (col_data[i] = static_cast<T>(
                                    Random::randInt64(value_min, value_max, rng)),
                                false);
        }
      } else if (std::is_floating_point<T>::value) {
        for (auto i = 0; i < col_data.size(); ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = N_MIN, true)
                             : (col_data[i] = static_cast<T>(
                                    Random::randFloat(value_min, value_max, rng)),
                                false);
        }
      } else {
        std::string str = "Unexpected type:";
        str.append(typeid(T).name()).append(", could not generate data.");
        CIDER_THROW(CiderCompileException, str);
      }
      break;
  }
  return std::make_tuple(col_data, null_data);
}

template <typename KeyType, typename HashTableType>
static void BM_Lookup(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<KeyType> data = prepare_data<KeyType>(row_num);
  for (auto _ : state) {
    int32_t collisions = bench<KeyType, HashTableType>(data);
    // state.counters["Collisions"] = collisions;
  }
}

template <typename KeyType>
void NO_INLINE bench_std_tree(const std::vector<KeyType>& data) {
  std::map<KeyType, int8_t> map;

  for (auto key : data) {
    map.emplace(key, 1);
  }

  for (auto key : data) {
    auto it = map.find(key);
  }
}

template <typename KeyType>
void NO_INLINE bench_std_unordered(const std::vector<KeyType>& data) {
  std::unordered_map<KeyType, int8_t> map;

  for (auto key : data) {
    map.emplace(key, 1);
  }

  for (auto key : data) {
    auto it = map.find(key);
  }
}

template <typename KeyType>
void NO_INLINE bench_folly_f14(const std::vector<KeyType>& data) {
  folly::F14FastMap<KeyType, int8_t> map;
  for (auto key : data) {
    map[key] = 1;
  }

  for (auto key : data) {
    auto it = map.find(key);
  }
}

template <typename KeyType>
static void BM_std_treemap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<KeyType> data = prepare_data<KeyType>(row_num);
  for (auto _ : state) {
    bench_std_tree<KeyType>(data);
    // state.counters["Collisions"] = -1;
  }
}

template <typename KeyType>
static void BM_std_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<KeyType> data = prepare_data<KeyType>(row_num);
  for (auto _ : state) {
    bench_std_unordered<KeyType>(data);
    // state.counters["Collisions"] = -1;
  }
}

template <typename KeyType>
static void BM_folly_f14_hashmap(benchmark::State& state) {
  size_t row_num = state.range(0);
  std::vector<KeyType> data = prepare_data<KeyType>(row_num);
  for (auto _ : state) {
    bench_folly_f14<KeyType>(data);
    // state.counters["Collisions"] = -1;
  }
}

template <typename KeyType>
static std::vector<KeyType> prepare_data(size_t row_num) {
  KeyType value_min = std::numeric_limits<KeyType>::min();
  KeyType value_max = std::numeric_limits<KeyType>::max();
  size_t null_chance = 0;
  GeneratePattern pattern = GeneratePattern::Sequence;
  std::vector<KeyType> data(row_num);
  std::vector<bool> data_null;
  {
    std::tie(data, data_null) =
        value_min > value_max
            ? generateAndFillVector<KeyType>(row_num, pattern, null_chance)
            : generateAndFillVector<KeyType>(
                  row_num, pattern, null_chance, value_min, value_max);
  }
  return data;
}

template <typename KeyType>
static void BM_cider_basic_hashmap(benchmark::State& state) {
  using BaselineLookup = HashMap<KeyType, int8_t, HashCRC32<KeyType>>;
  BM_Lookup<KeyType, BaselineLookup>(state);
}
template <typename KeyType>
static void BM_cider_optimzied_hashmap(benchmark::State& state) {
  using OptimizedLookup = FixedHashMap<KeyType, int8_t>;
  BM_Lookup<KeyType, OptimizedLookup>(state);
}

BENCHMARK(BM_std_treemap<uint8_t>)->RangeMultiplier(10)->Range(10000, 10000000);
BENCHMARK(BM_std_hashmap<uint8_t>)->RangeMultiplier(10)->Range(10000, 10000000);
BENCHMARK(BM_folly_f14_hashmap<uint8_t>)->RangeMultiplier(10)->Range(10000, 10000000);
BENCHMARK(BM_cider_basic_hashmap<uint8_t>)->RangeMultiplier(10)->Range(10000, 10000000);
BENCHMARK(BM_cider_optimzied_hashmap<uint8_t>)
    ->RangeMultiplier(10)
    ->Range(10000, 10000000);

// Remove uint16_t benchmark to make the report clear.
// Besides, the result of uint16_t is same to uint8_t.

BENCHMARK_MAIN();
