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

#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include "TestHelpers.h"
#include "common/interpreters/AggregationHashTable.h"

using namespace TestHelpers;

class CiderNewAggHashTableTest : public ::testing::Test {};

static const std::shared_ptr<CiderAllocator> allocator =
    std::make_shared<CiderDefaultAllocator>();

TEST_F(CiderNewAggHashTableTest, aggUInt8Test) {
  // SQL: SELECT SUM(int8), COUNT(int8), MIN(int8), MAX(int8) FROM table GROUP BY int8
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int8
  std::vector<SQLTypes> key_types;
  key_types.push_back(SQLTypes::kTINYINT);

  // value of HT: SUM(int8)-int64 + COUNT(int8)-int32 + MIN(int8)-int8 +
  // MAX(int8)-int8
  uint32_t init_value_len = 14;
  int8_t* init_value_ptr = allocator->allocate(init_value_len);
  reinterpret_cast<int64_t*>(init_value_ptr)[0] = (int64_t)0;
  reinterpret_cast<int32_t*>(init_value_ptr + 8)[0] = (int32_t)0;
  reinterpret_cast<int8_t*>(init_value_ptr + 12)[0] = std::numeric_limits<int8_t>::max();
  reinterpret_cast<int8_t*>(init_value_ptr + 13)[0] = std::numeric_limits<int8_t>::min();

  cider::hashtable::AggregationHashTable aggregator(
      key_types, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key1_ptr)[0] = (bool)false;
  reinterpret_cast<uint8_t*>(key1_ptr + 2)[0] = (uint8_t)1;
  // Use get api and return value address
  int8_t* value1_ptr = aggregator.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_ptr + 12)[0],
           std::numeric_limits<int8_t>::max());
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_ptr + 13)[0],
           std::numeric_limits<int8_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value1_ptr)[0] += 10;
  reinterpret_cast<int32_t*>(value1_ptr + 8)[0] += 1;
  reinterpret_cast<int8_t*>(value1_ptr + 12)[0] =
      std::min(reinterpret_cast<int8_t*>(value1_ptr + 12)[0], (int8_t)10);
  reinterpret_cast<int8_t*>(value1_ptr + 13)[0] =
      std::max(reinterpret_cast<int8_t*>(value1_ptr + 13)[0], (int8_t)10);

  // Row1:
  int8_t* key2_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key2_ptr)[0] = (bool)false;
  reinterpret_cast<uint8_t*>(key2_ptr + 2)[0] = (uint8_t)2;
  // Use get api and return value address
  int8_t* value2_ptr = aggregator.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_ptr + 12)[0],
           std::numeric_limits<int8_t>::max());
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_ptr + 13)[0],
           std::numeric_limits<int8_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value2_ptr)[0] += 20;
  reinterpret_cast<int32_t*>(value2_ptr + 8)[0] += 1;
  reinterpret_cast<int8_t*>(value2_ptr + 12)[0] =
      std::min(reinterpret_cast<int8_t*>(value2_ptr + 12)[0], (int8_t)20);
  reinterpret_cast<int8_t*>(value2_ptr + 13)[0] =
      std::max(reinterpret_cast<int8_t*>(value2_ptr + 13)[0], (int8_t)20);

  // Row2:
  int8_t* key3_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key3_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key3_ptr + 2)[0] = (uint8_t)1;
  // Use get api and return value address
  int8_t* value3_ptr = aggregator.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value3_ptr)[0] += 30;
  reinterpret_cast<int32_t*>(value3_ptr + 8)[0] += 1;
  reinterpret_cast<int8_t*>(value3_ptr + 12)[0] =
      std::min(reinterpret_cast<int8_t*>(value3_ptr + 12)[0], (int8_t)30);
  reinterpret_cast<int8_t*>(value3_ptr + 13)[0] =
      std::max(reinterpret_cast<int8_t*>(value3_ptr + 13)[0], (int8_t)30);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key1_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint8_t*>(key1_check_ptr + 2)[0] = (uint8_t)1;
  // Use get api and return value address
  int8_t* value1_check_ptr = aggregator.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 8)[0], 2);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_check_ptr + 12)[0], 10);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_check_ptr + 13)[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key2_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint8_t*>(key2_check_ptr + 2)[0] = (uint8_t)2;
  // Use get api and return value address
  int8_t* value2_check_ptr = aggregator.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 8)[0], 1);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_check_ptr + 12)[0], 20);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_check_ptr + 13)[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt16Test) {
  // SQL:
  // SELECT SUM(int16), COUNT(int16), MIN(int16), MAX(int16) FROM table GROUP BY int16.
  // The example below has 3 rows of data. Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int16
  std::vector<SQLTypes> keys;
  keys.push_back(SQLTypes::kSMALLINT);

  // value of HT: SUM(int16)-int64 + COUNT(int16)-int32 + MIN(int16)-int16 +
  // MAX(int16)-int16
  uint32_t init_value_len = 16;
  int8_t* init_value_ptr = allocator->allocate(init_value_len);
  reinterpret_cast<int64_t*>(init_value_ptr)[0] = (int64_t)0;
  reinterpret_cast<int32_t*>(init_value_ptr + 8)[0] = (int32_t)0;
  reinterpret_cast<int16_t*>(init_value_ptr + 12)[0] =
      std::numeric_limits<int16_t>::max();
  reinterpret_cast<int16_t*>(init_value_ptr + 14)[0] =
      std::numeric_limits<int16_t>::min();

  cider::hashtable::AggregationHashTable aggregator(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key1_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key1_ptr + 2)[0] = (uint16_t)1;
  // Use get api and return value address
  int8_t* value1_ptr = aggregator.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_ptr + 12)[0],
           std::numeric_limits<int16_t>::max());
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_ptr + 14)[0],
           std::numeric_limits<int16_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value1_ptr)[0] += 10;
  reinterpret_cast<int32_t*>(value1_ptr + 8)[0] += 1;
  reinterpret_cast<int16_t*>(value1_ptr + 12)[0] =
      std::min(reinterpret_cast<int16_t*>(value1_ptr + 12)[0], (int16_t)10);
  reinterpret_cast<int16_t*>(value1_ptr + 14)[0] =
      std::max(reinterpret_cast<int16_t*>(value1_ptr + 14)[0], (int16_t)10);

  // Row1:
  int8_t* key2_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key2_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key2_ptr + 2)[0] = (uint16_t)2;
  // Use get api and return value address
  int8_t* value2_ptr = aggregator.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_ptr + 12)[0],
           std::numeric_limits<int16_t>::max());
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_ptr + 14)[0],
           std::numeric_limits<int16_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value2_ptr)[0] += 20;
  reinterpret_cast<int32_t*>(value2_ptr + 8)[0] += 1;
  reinterpret_cast<int16_t*>(value2_ptr + 12)[0] =
      std::min(reinterpret_cast<int16_t*>(value2_ptr + 12)[0], (int16_t)20);
  reinterpret_cast<int16_t*>(value2_ptr + 14)[0] =
      std::max(reinterpret_cast<int16_t*>(value2_ptr + 14)[0], (int16_t)20);

  // Row2:
  int8_t* key3_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key3_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key3_ptr + 2)[0] = (uint16_t)1;
  // Use get api and return value address
  int8_t* value3_ptr = aggregator.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value3_ptr)[0] += 30;
  reinterpret_cast<int32_t*>(value3_ptr + 8)[0] += 1;
  reinterpret_cast<int16_t*>(value3_ptr + 12)[0] =
      std::min(reinterpret_cast<int16_t*>(value3_ptr + 12)[0], (int16_t)30);
  reinterpret_cast<int16_t*>(value3_ptr + 14)[0] =
      std::max(reinterpret_cast<int16_t*>(value3_ptr + 14)[0], (int16_t)30);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key1_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key1_check_ptr + 2)[0] = (uint16_t)1;
  // Use get api and return value address
  int8_t* value1_check_ptr = aggregator.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 8)[0], 2);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_check_ptr + 12)[0], 10);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_check_ptr + 14)[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key2_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint16_t*>(key2_check_ptr + 2)[0] = (uint16_t)2;
  // Use get api and return value address
  int8_t* value2_check_ptr = aggregator.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 8)[0], 1);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_check_ptr + 12)[0], 20);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_check_ptr + 14)[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt32Test) {
  // SQL:
  // SELECT SUM(int32), COUNT(int32), MIN(int32), MAX(int32) FROM table GROUP BY int32.
  // The example below has 3 rows of data. Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int32
  std::vector<SQLTypes> keys;
  keys.push_back(SQLTypes::kINT);

  // value of HT: SUM(int32)-int64 + COUNT(int32)-int32 + MIN(int32)-int32 +
  // MAX(int32)-int32
  uint32_t init_value_len = 20;
  int8_t* init_value_ptr = allocator->allocate(init_value_len);
  reinterpret_cast<int64_t*>(init_value_ptr)[0] = (int64_t)0;
  reinterpret_cast<int32_t*>(init_value_ptr + 8)[0] = (int32_t)0;
  reinterpret_cast<int32_t*>(init_value_ptr + 12)[0] =
      std::numeric_limits<int32_t>::max();
  reinterpret_cast<int32_t*>(init_value_ptr + 16)[0] =
      std::numeric_limits<int32_t>::min();

  cider::hashtable::AggregationHashTable aggregator(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = allocator->allocate(6);
  reinterpret_cast<bool*>(key1_ptr)[0] = (bool)false;
  reinterpret_cast<uint32_t*>(key1_ptr + 2)[0] = (uint32_t)1;
  // Use get api and return value address
  int8_t* value1_ptr = aggregator.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 12)[0],
           std::numeric_limits<int32_t>::max());
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 16)[0],
           std::numeric_limits<int32_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value1_ptr)[0] += 10;
  reinterpret_cast<int32_t*>(value1_ptr + 8)[0] += 1;
  reinterpret_cast<int32_t*>(value1_ptr + 12)[0] =
      std::min(reinterpret_cast<int32_t*>(value1_ptr + 12)[0], (int32_t)10);
  reinterpret_cast<int32_t*>(value1_ptr + 16)[0] =
      std::max(reinterpret_cast<int32_t*>(value1_ptr + 16)[0], (int32_t)10);

  // Row1:
  int8_t* key2_ptr = allocator->allocate(6);
  reinterpret_cast<bool*>(key2_ptr)[0] = (bool)false;
  reinterpret_cast<uint32_t*>(key2_ptr + 2)[0] = (uint32_t)2;
  // Use get api and return value address
  int8_t* value2_ptr = aggregator.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 12)[0],
           std::numeric_limits<int32_t>::max());
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 16)[0],
           std::numeric_limits<int32_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value2_ptr)[0] += 20;
  reinterpret_cast<int32_t*>(value2_ptr + 8)[0] += 1;
  reinterpret_cast<int32_t*>(value2_ptr + 12)[0] =
      std::min(reinterpret_cast<int32_t*>(value2_ptr + 12)[0], (int32_t)20);
  reinterpret_cast<int32_t*>(value2_ptr + 16)[0] =
      std::max(reinterpret_cast<int32_t*>(value2_ptr + 16)[0], (int32_t)20);

  // Row2:
  int8_t* key3_ptr = allocator->allocate(6);
  reinterpret_cast<bool*>(key3_ptr)[0] = (bool)false;
  reinterpret_cast<uint32_t*>(key3_ptr + 2)[0] = (uint32_t)1;
  // Use get api and return value address
  int8_t* value3_ptr = aggregator.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value3_ptr)[0] += 30;
  reinterpret_cast<int32_t*>(value3_ptr + 8)[0] += 1;
  reinterpret_cast<int32_t*>(value3_ptr + 12)[0] =
      std::min(reinterpret_cast<int32_t*>(value3_ptr + 12)[0], (int32_t)30);
  reinterpret_cast<int32_t*>(value3_ptr + 16)[0] =
      std::max(reinterpret_cast<int32_t*>(value3_ptr + 16)[0], (int32_t)30);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = allocator->allocate(6);
  reinterpret_cast<bool*>(key1_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint32_t*>(key1_check_ptr + 2)[0] = (uint32_t)1;
  // Use get api and return value address
  int8_t* value1_check_ptr = aggregator.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 8)[0], 2);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 12)[0], 10);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 16)[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = allocator->allocate(6);
  reinterpret_cast<bool*>(key2_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint32_t*>(key2_check_ptr + 2)[0] = (uint32_t)2;
  // Use get api and return value address
  int8_t* value2_check_ptr = aggregator.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 8)[0], 1);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 12)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 16)[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt64Test) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY int64.
  // The example below has 3 rows of data. Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int64
  std::vector<SQLTypes> keys;
  keys.push_back(SQLTypes::kBIGINT);

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = allocator->allocate(init_value_len);
  reinterpret_cast<int64_t*>(init_value_ptr)[0] = (int64_t)0;
  reinterpret_cast<int32_t*>(init_value_ptr + 8)[0] = (int32_t)0;
  reinterpret_cast<int64_t*>(init_value_ptr + 12)[0] =
      std::numeric_limits<int64_t>::max();
  reinterpret_cast<int64_t*>(init_value_ptr + 20)[0] =
      std::numeric_limits<int64_t>::min();

  cider::hashtable::AggregationHashTable aggregator(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = allocator->allocate(10);
  reinterpret_cast<bool*>(key1_ptr)[0] = (bool)false;
  reinterpret_cast<uint64_t*>(key1_ptr + 2)[0] = (uint64_t)1;
  // Use get api and return value address
  int8_t* value1_ptr = aggregator.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + 12)[0],
           std::numeric_limits<int64_t>::max());
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + 20)[0],
           std::numeric_limits<int64_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value1_ptr)[0] += 10;
  reinterpret_cast<int32_t*>(value1_ptr + 8)[0] += 1;
  reinterpret_cast<int64_t*>(value1_ptr + 12)[0] =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + 12)[0], (int64_t)10);
  reinterpret_cast<int64_t*>(value1_ptr + 20)[0] =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + 20)[0], (int64_t)10);

  // Row1:
  int8_t* key2_ptr = allocator->allocate(10);
  reinterpret_cast<bool*>(key2_ptr)[0] = (bool)false;
  reinterpret_cast<uint64_t*>(key2_ptr + 2)[0] = (uint64_t)2;
  // Use get api and return value address
  int8_t* value2_ptr = aggregator.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + 8)[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + 12)[0],
           std::numeric_limits<int64_t>::max());
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + 20)[0],
           std::numeric_limits<int64_t>::min());

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value2_ptr)[0] += 20;
  reinterpret_cast<int32_t*>(value2_ptr + 8)[0] += 1;
  reinterpret_cast<int64_t*>(value2_ptr + 12)[0] =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + 12)[0], (int64_t)20);
  reinterpret_cast<int64_t*>(value2_ptr + 20)[0] =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + 20)[0], (int64_t)20);

  // Row2:
  int8_t* key3_ptr = allocator->allocate(4);
  reinterpret_cast<bool*>(key3_ptr)[0] = (bool)false;
  reinterpret_cast<uint64_t*>(key3_ptr + 2)[0] = (uint64_t)1;
  // Use get api and return value address
  int8_t* value3_ptr = aggregator.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  reinterpret_cast<int64_t*>(value3_ptr)[0] += 30;
  reinterpret_cast<int32_t*>(value3_ptr + 8)[0] += 1;
  reinterpret_cast<int64_t*>(value3_ptr + 12)[0] =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + 12)[0], (int64_t)30);
  reinterpret_cast<int64_t*>(value3_ptr + 20)[0] =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + 20)[0], (int64_t)30);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = allocator->allocate(10);
  reinterpret_cast<bool*>(key1_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint64_t*>(key1_check_ptr + 2)[0] = (uint64_t)1;
  // Use get api and return value address
  int8_t* value1_check_ptr = aggregator.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + 8)[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + 12)[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + 20)[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = allocator->allocate(10);
  reinterpret_cast<bool*>(key2_check_ptr)[0] = (bool)false;
  reinterpret_cast<uint64_t*>(key2_check_ptr + 2)[0] = (uint64_t)2;
  // Use get api and return value address
  int8_t* value2_check_ptr = aggregator.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + 8)[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + 12)[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + 20)[0], 20);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
