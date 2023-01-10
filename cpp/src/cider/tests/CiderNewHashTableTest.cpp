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

#include <common/hashtable/HashTableAllocator.h>
#include <gtest/gtest.h>
#include "TestHelpers.h"
#include "common/hashtable/FixedHashMap.h"

using namespace TestHelpers;

class CiderNewHashTableTest : public ::testing::Test {};

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

struct Block {
  int64_t sum = 0;
  size_t count = 0;

  Block() {}

  Block(int64_t sum, size_t count) {
    this->sum = sum;
    this->count = count;
  }

  void add(int16_t value) {
    sum += value;
    ++count;
  }

  template <size_t unroll_count = 128 / sizeof(int16_t)>
  void addBatch(const int16_t* ptr, size_t size) {
    /// Compiler cannot unroll this loop, do it manually.
    /// (at least for floats, most likely due to the lack of -fassociative-math)

    int16_t partial_sums[unroll_count]{};

    const auto* end = ptr + size;
    const auto* unrolled_end = ptr + (size / unroll_count * unroll_count);

    while (ptr < unrolled_end) {
      for (size_t i = 0; i < unroll_count; ++i)
        partial_sums[i] += ptr[i];
      ptr += unroll_count;
    }

    for (size_t i = 0; i < unroll_count; ++i)
      sum += partial_sums[i];

    while (ptr < end) {
      sum += *ptr;
      ++ptr;
    }

    count += size;
  }

  void merge(const Block& other) {
    sum += other.sum;
    count += other.count;
  }

  int64_t getSum() const { return sum; }

  size_t getCount() const { return count; }

  double getAvg() const { return (double)sum / count; }

  bool operator!() const { return !count; }
};

TEST_F(CiderNewHashTableTest, aggInt8Test) {
  using AggregatedDataWithUInt8Key =
      FixedImplicitZeroHashMapWithStoredSize<int8_t, Block>;
  AggregatedDataWithUInt8Key map;

  std::vector<int8_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<int8_t> keys2{1, 2, 3, 4, 5};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = map[keys2[i]];
    block.add(values2[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), 60);
    CHECK_EQ(map[keys[i]].getCount(), 2);
    CHECK_EQ(map[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggInt16Test) {
  using AggregatedDataWithUInt16Key =
      FixedImplicitZeroHashMapWithCalculatedSize<int16_t, Block>;
  AggregatedDataWithUInt16Key map;

  std::vector<int16_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<int8_t> keys2{1, 2, 3, 4, 5};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = map[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), 60);
    CHECK_EQ(map[keys[i]].getCount(), 2);
    CHECK_EQ(map[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, hashTableTest) {
  using Cell = FixedHashMapCell<int16_t, Block>;
  FixedHashTable<int16_t, Cell, FixedHashTableStoredSize<Cell>, HashTableAllocator> fht;

  int16_t key = 1;
  PairNoInit<int16_t, Block> in;
  in.first = key;
  in.second = Block(10, key);

  // emplace api of FixedHashTable
  std::pair<Cell*, bool> res;

  fht.emplace(key, res.first, res.second);

  if (res.second)
    insertSetMapped(res.first->getMapped(), in);

  // find api of FixedHashTable
  auto find_res = fht.find(key);
  CHECK_EQ(find_res[0].getMapped().getSum(), 10);
  CHECK_EQ(find_res[0].getMapped().getCount(), 1);

  // hash api of FixedHashTable
  CHECK_EQ(fht.hash(key), 1);

  // empty api of FixedHashTable
  CHECK_EQ(fht.size(), 1);

  // empty api of FixedHashTable
  CHECK_EQ(fht.empty(), false);

  // contains api of FixedHashTable
  CHECK_EQ(fht.contains(key), true);
  CHECK_EQ(fht.contains(0), false);

  // clear api of FixedHashTable
  fht.clear();
  CHECK_EQ(fht.empty(), true);
  CHECK_EQ(fht.contains(key), false);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
