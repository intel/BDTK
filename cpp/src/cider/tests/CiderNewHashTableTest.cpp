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

#include <common/base/wide_integer.h>
#include <common/hashtable/HashTableAllocator.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "common/hashtable/FixedHashMap.h"
#include "util/Logger.h"

using namespace cider::hashtable;

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

TEST_F(CiderNewHashTableTest, aggUInt8Test) {
  using AggregatedHashTableForUInt8Key =
      FixedImplicitZeroHashMapWithStoredSize<int8_t, Block>;
  AggregatedHashTableForUInt8Key map;

  std::vector<uint8_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<uint8_t> keys2{1, 2, 3, 4, 5};
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

TEST_F(CiderNewHashTableTest, aggUInt16Test) {
  using AggregatedHashTableForUInt16Key =
      FixedImplicitZeroHashMapWithCalculatedSize<int16_t, Block>;
  AggregatedHashTableForUInt16Key map;

  std::vector<uint16_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = map[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(map[keys[i]].getSum(), values[i]);
    CHECK_EQ(map[keys[i]].getCount(), 1);
  }

  std::vector<uint8_t> keys2{1, 2, 3, 4, 5};
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

TEST_F(CiderNewHashTableTest, fixedHashTableTest) {
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

TEST_F(CiderNewHashTableTest, aggUInt32Test) {
  using AggregatedHashTableForUInt32Key = HashMap<uint32_t, Block, HashCRC32<uint32_t>>;
  AggregatedHashTableForUInt32Key ht_uint32;

  std::vector<uint32_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_uint32[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint32[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_uint32[keys[i]].getCount(), 1);
  }

  std::vector<uint32_t> keys2{1, 2, 3, 4, 5};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_uint32[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint32[keys[i]].getSum(), 60);
    CHECK_EQ(ht_uint32[keys[i]].getCount(), 2);
    CHECK_EQ(ht_uint32[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggUInt64Test) {
  using AggregatedHashTableForUInt64Key = HashMap<uint64_t, Block, HashCRC32<uint64_t>>;
  AggregatedHashTableForUInt64Key ht_uint64;

  std::vector<uint64_t> keys{1, 2, 3, 4, 5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_uint64[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint64[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_uint64[keys[i]].getCount(), 1);
  }

  std::vector<uint64_t> keys2{1, 2, 3, 4, 5};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_uint64[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint64[keys[i]].getSum(), 60);
    CHECK_EQ(ht_uint64[keys[i]].getCount(), 2);
    CHECK_EQ(ht_uint64[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggUInt128Test) {
  using AggregatedHashTableForKeys128 = HashMap<UInt128, Block, UInt128HashCRC32>;
  AggregatedHashTableForKeys128 ht_keys128;

  std::vector<UInt128> keys;
  keys.push_back(cider::wide::Integer<128, unsigned>(1));
  keys.push_back(cider::wide::Integer<128, unsigned>(2));
  keys.push_back(cider::wide::Integer<128, unsigned>(3));
  keys.push_back(cider::wide::Integer<128, unsigned>(4));
  keys.push_back(cider::wide::Integer<128, unsigned>(5));

  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_keys128[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys128[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_keys128[keys[i]].getCount(), 1);
  }

  std::vector<UInt128> keys2;
  keys2.push_back(cider::wide::Integer<128, unsigned>(1));
  keys2.push_back(cider::wide::Integer<128, unsigned>(2));
  keys2.push_back(cider::wide::Integer<128, unsigned>(3));
  keys2.push_back(cider::wide::Integer<128, unsigned>(4));
  keys2.push_back(cider::wide::Integer<128, unsigned>(5));

  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_keys128[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys128[keys[i]].getSum(), 60);
    CHECK_EQ(ht_keys128[keys[i]].getCount(), 2);
    CHECK_EQ(ht_keys128[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggUInt256Test) {
  using AggregatedHashTableForKeys256 = HashMap<UInt256, Block, UInt256HashCRC32>;
  AggregatedHashTableForKeys256 ht_keys256;

  std::vector<UInt256> keys;
  keys.push_back(cider::wide::Integer<256, unsigned>(1));
  keys.push_back(cider::wide::Integer<256, unsigned>(2));
  keys.push_back(cider::wide::Integer<256, unsigned>(3));
  keys.push_back(cider::wide::Integer<256, unsigned>(4));
  keys.push_back(cider::wide::Integer<256, unsigned>(5));

  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_keys256[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys256[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_keys256[keys[i]].getCount(), 1);
  }

  std::vector<UInt256> keys2;
  keys2.push_back(cider::wide::Integer<256, unsigned>(1));
  keys2.push_back(cider::wide::Integer<256, unsigned>(2));
  keys2.push_back(cider::wide::Integer<256, unsigned>(3));
  keys2.push_back(cider::wide::Integer<256, unsigned>(4));
  keys2.push_back(cider::wide::Integer<256, unsigned>(5));

  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_keys256[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys256[keys[i]].getSum(), 60);
    CHECK_EQ(ht_keys256[keys[i]].getCount(), 2);
    CHECK_EQ(ht_keys256[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggInt128Test) {
  using AggregatedHashTableForKeys128 = HashMap<Int128, Block, UInt128HashCRC32>;
  AggregatedHashTableForKeys128 ht_keys128;

  std::vector<UInt128> keys;
  keys.push_back(cider::wide::Integer<128, signed>(1));
  keys.push_back(cider::wide::Integer<128, signed>(2));
  keys.push_back(cider::wide::Integer<128, signed>(3));
  keys.push_back(cider::wide::Integer<128, signed>(4));
  keys.push_back(cider::wide::Integer<128, signed>(5));

  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_keys128[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys128[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_keys128[keys[i]].getCount(), 1);
  }

  std::vector<Int128> keys2;
  keys2.push_back(cider::wide::Integer<128, signed>(1));
  keys2.push_back(cider::wide::Integer<128, signed>(2));
  keys2.push_back(cider::wide::Integer<128, signed>(3));
  keys2.push_back(cider::wide::Integer<128, signed>(4));
  keys2.push_back(cider::wide::Integer<128, signed>(5));

  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_keys128[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys128[keys[i]].getSum(), 60);
    CHECK_EQ(ht_keys128[keys[i]].getCount(), 2);
    CHECK_EQ(ht_keys128[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggInt256Test) {
  using AggregatedHashTableForKeys256 = HashMap<Int256, Block, UInt256HashCRC32>;
  AggregatedHashTableForKeys256 ht_keys256;

  std::vector<Int256> keys;
  keys.push_back(cider::wide::Integer<256, signed>(1));
  keys.push_back(cider::wide::Integer<256, signed>(2));
  keys.push_back(cider::wide::Integer<256, signed>(3));
  keys.push_back(cider::wide::Integer<256, signed>(4));
  keys.push_back(cider::wide::Integer<256, signed>(5));

  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_keys256[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys256[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_keys256[keys[i]].getCount(), 1);
  }

  std::vector<Int256> keys2;
  keys2.push_back(cider::wide::Integer<256, signed>(1));
  keys2.push_back(cider::wide::Integer<256, signed>(2));
  keys2.push_back(cider::wide::Integer<256, signed>(3));
  keys2.push_back(cider::wide::Integer<256, signed>(4));
  keys2.push_back(cider::wide::Integer<256, signed>(5));

  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_keys256[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_keys256[keys[i]].getSum(), 60);
    CHECK_EQ(ht_keys256[keys[i]].getCount(), 2);
    CHECK_EQ(ht_keys256[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggFloatTest) {
  using AggregatedHashTableForFloatKey = HashMap<float, Block, HashCRC32<float>>;
  AggregatedHashTableForFloatKey ht_float;

  std::vector<float> keys{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_float[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_float[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_float[keys[i]].getCount(), 1);
  }

  std::vector<float> keys2{1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_float[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_float[keys[i]].getSum(), 60);
    CHECK_EQ(ht_float[keys[i]].getCount(), 2);
    CHECK_EQ(ht_float[keys[i]].getAvg(), 30);
  }
}

TEST_F(CiderNewHashTableTest, aggDoubleTest) {
  using AggregatedHashTableForDoubleKey = HashMap<double, Block, HashCRC32<double>>;
  AggregatedHashTableForDoubleKey ht_double;

  std::vector<double> keys{1.11, 2.22, 3.33, 4.44, 5.55};
  std::vector<int64_t> values{10, 20, 30, 40, 50};

  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_double[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_double[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_double[keys[i]].getCount(), 1);
  }

  std::vector<double> keys2{1.11, 2.22, 3.33, 4.44, 5.55};
  std::vector<int64_t> values2{50, 40, 30, 20, 10};
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_double[keys2[i]];
    block.add(values2[i]);
  }
  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_double[keys[i]].getSum(), 60);
    CHECK_EQ(ht_double[keys[i]].getCount(), 2);
    CHECK_EQ(ht_double[keys[i]].getAvg(), 30);
  }
}

// Large data set test cases
TEST_F(CiderNewHashTableTest, aggUInt64LargeDatasetTest) {
  using AggregatedHashTableForUInt64Key = HashMap<uint64_t, Block, HashCRC32<uint64_t>>;
  AggregatedHashTableForUInt64Key ht_uint64;

  std::vector<uint64_t> keys;
  std::vector<int64_t> values;
  for (int i = 0; i < 5000; i++) {
    keys.emplace_back(i);
    values.emplace_back(10);
  }
  for (int i = 0; i < keys.size(); i++) {
    Block& block = ht_uint64[keys[i]];
    block.add(values[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint64[keys[i]].getSum(), values[i]);
    CHECK_EQ(ht_uint64[keys[i]].getCount(), 1);
  }

  std::vector<uint64_t> keys2;
  std::vector<int64_t> values2;
  for (int i = 0; i < 5000; i++) {
    keys2.emplace_back(i);
    values2.emplace_back(40);
  }
  for (int i = 0; i < keys2.size(); i++) {
    Block& block = ht_uint64[keys2[i]];
    block.add(values2[i]);
  }

  for (int i = 0; i < keys.size(); i++) {
    CHECK_EQ(ht_uint64[keys[i]].getSum(), 50);
    CHECK_EQ(ht_uint64[keys[i]].getCount(), 2);
    CHECK_EQ(ht_uint64[keys[i]].getAvg(), 25);
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
