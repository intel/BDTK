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

#include <common/base/StringRef.h>
#include <common/base/wide_integer.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include "common/interpreters/AggregationHashTable.h"
#include "util/Logger.h"

using namespace cider::hashtable;

class CiderNewAggHashTableTest : public ::testing::Test {};

static const std::shared_ptr<CiderAllocator> default_allocator =
    std::make_shared<CiderDefaultAllocator>();

TEST_F(CiderNewAggHashTableTest, aggUInt8Test) {
  // SQL: SELECT SUM(int8), COUNT(int8), MIN(int8), MAX(int8) FROM table GROUP BY int8
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int8
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kTINYINT));

  // 1byte is_null(bool) + 1byte padding + 1byte int8 + 1byte padding
  uint8_t key_len = 4;
  bool key_null = false;

  uint8_t key1 = 1;
  uint8_t key2 = 2;
  uint8_t key3 = 1;

  int8_t val1 = 10;
  int8_t val2 = 20;
  int8_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 13};

  // value of HT: SUM(int8)-int64 + COUNT(int8)-int32 + MIN(int8)-int8 +
  // MAX(int8)-int8
  uint32_t init_value_len = 14;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int8_t min_init_val = std::numeric_limits<int8_t>::max();
  int8_t max_init_val = std::numeric_limits<int8_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int8_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int8_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<uint8_t*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int8_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int8_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int8_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int8_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<uint8_t*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], sum_init_val);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], cnt_init_val);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int8_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int8_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int8_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int8_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int8_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int8_t*>(value3_ptr + 12)[0], val3);
  *reinterpret_cast<int8_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int8_t*>(value3_ptr + 13)[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<uint8_t*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int8_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<uint8_t*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int8_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt16Test) {
  // SQL:
  // SELECT SUM(int16), COUNT(int16), MIN(int16), MAX(int16) FROM table GROUP BY int16.
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int16
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kSMALLINT));

  // 1byte is_null(bool) + 1byte padding + 2bytes int16
  uint8_t key_len = 4;
  bool key_null = false;

  uint16_t key1 = 1;
  uint16_t key2 = 2;
  uint16_t key3 = 1;

  int16_t val1 = 10;
  int16_t val2 = 20;
  int16_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 14};

  // value of HT: SUM(int16)-int64 + COUNT(int16)-int32 + MIN(int16)-int16 +
  // MAX(int16)-int16
  uint32_t init_value_len = 16;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int16_t min_init_val = std::numeric_limits<int16_t>::max();
  int16_t max_init_val = std::numeric_limits<int16_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int16_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int16_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int16_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int16_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int16_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int16_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int16_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int16_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int16_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int16_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int16_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int16_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int16_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int16_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int16_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<uint16_t*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int16_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt32Test) {
  // SQL:
  // SELECT SUM(int32), COUNT(int32), MIN(int32), MAX(int32) FROM table GROUP BY int32.
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int32
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kINT));

  // 1byte is_null(bool) + 1byte padding + 4bytes int32
  uint8_t key_len = 6;
  bool key_null = false;

  uint32_t key1 = 1;
  uint32_t key2 = 2;
  uint32_t key3 = 1;

  int32_t val1 = 10;
  int32_t val2 = 20;
  int32_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 16};

  // value of HT: SUM(int32)-int64 + COUNT(int32)-int32 + MIN(int32)-int32 +
  // MAX(int32)-int32
  uint32_t init_value_len = 20;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int32_t min_init_val = std::numeric_limits<int32_t>::max();
  int32_t max_init_val = std::numeric_limits<int32_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<uint32_t*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value1_ptr + 16)[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<uint32_t*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<uint32_t*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<uint32_t*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<uint32_t*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggUInt64Test) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY int64.
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int64
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kBIGINT));

  // 1byte is_null(bool) + 1byte padding + 8bytes int64
  uint8_t key_len = 10;
  bool key_null = false;

  uint64_t key1 = 1;
  uint64_t key2 = 2;
  uint64_t key3 = 1;

  int64_t val1 = 10;
  int64_t val2 = 20;
  int64_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 20};

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int64_t min_init_val = std::numeric_limits<int64_t>::max();
  int64_t max_init_val = std::numeric_limits<int64_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<uint64_t*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<uint64_t*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<uint64_t*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<uint64_t*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<uint64_t*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggKeys128Test) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY
  // int128. The example below has 3 rows of data. Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int128
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kINT128));

  // 1byte is_null(bool) + 1byte padding + 16bytes int128
  uint8_t key_len = 18;
  bool key_null = false;

  UInt128 key1 = UInt128(1);
  UInt128 key2 = UInt128(2);
  UInt128 key3 = UInt128(1);

  int64_t val1 = 10;
  int64_t val2 = 20;
  int64_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 20};

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int64_t min_init_val = std::numeric_limits<int64_t>::max();
  int64_t max_init_val = std::numeric_limits<int64_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<UInt128*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<UInt128*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<UInt128*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<UInt128*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<UInt128*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggKeys256Test) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY
  // int256. The example below has 3 rows of data. Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int256
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kINT256));

  // 1byte is_null(bool) + 1byte padding + 32bytes int256
  uint8_t key_len = 34;
  bool key_null = false;

  UInt256 key1 = UInt256(1);
  UInt256 key2 = UInt256(2);
  UInt256 key3 = UInt256(1);

  int64_t val1 = 10;
  int64_t val2 = 20;
  int64_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 20};

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int64_t min_init_val = std::numeric_limits<int64_t>::max();
  int64_t max_init_val = std::numeric_limits<int64_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<UInt256*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<UInt256*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<UInt256*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<UInt256*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<UInt256*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggFloatTest) {
  // SQL:
  // SELECT SUM(int32), COUNT(int32), MIN(int32), MAX(int32) FROM table GROUP BY float.

  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1.1       10
  //     1         2,2       20
  //     2         1.1       30

  // key of HT: float
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kFLOAT));

  // 1byte is_null(bool) + 1byte padding + 4bytes float
  uint8_t key_len = 6;
  bool key_null = false;

  float key1 = 1.1;
  float key2 = 2.2;
  float key3 = 1.1;

  int32_t val1 = 10;
  int32_t val2 = 20;
  int32_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 16};

  // value of HT: SUM(int32)-int64_t + COUNT(int32)-int32 + MIN(int32)-int32 +
  // MAX(int32)-int32
  uint32_t init_value_len = 20;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int32_t min_init_val = std::numeric_limits<int32_t>::max();
  int32_t max_init_val = std::numeric_limits<int32_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<float*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value1_ptr + 16)[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<float*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<float*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int32_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int32_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<float*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<float*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

TEST_F(CiderNewAggHashTableTest, aggDoubleTest) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY
  // double. The example below has 3 rows of data.
  // Row number   key    value
  //     0         1.1       10
  //     1         2.2       20
  //     2         1.1       30

  // key of HT: int64
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kDOUBLE));

  // 1byte is_null(bool) + 1byte padding + 8bytes double
  uint8_t key_len = 10;
  bool key_null = false;

  double key1 = 1;
  double key2 = 2;
  double key3 = 1;

  int64_t val1 = 10;
  int64_t val2 = 20;
  int64_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 20};

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int64_t min_init_val = std::numeric_limits<int64_t>::max();
  int64_t max_init_val = std::numeric_limits<int64_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<double*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<double*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<double*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<double*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<double*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

// Large dataset test cases
TEST_F(CiderNewAggHashTableTest, aggStringTest) {
  // SQL:
  // SELECT SUM(int64), COUNT(int64), MIN(int64), MAX(int64) FROM table GROUP BY int64.
  // The example below has 3 rows of data.
  // Row number   key    value
  //     0         1       10
  //     1         2       20
  //     2         1       30

  // key of HT: int64
  std::vector<SQLTypeInfo> keys;
  keys.push_back(SQLTypeInfo(SQLTypes::kTEXT, 1, 0));

  // 1byte is_null(bool) + 1byte padding + 1byte string
  uint8_t key_len = 3;
  bool key_null = false;

  StringRef key1("1");
  StringRef key2("2");
  StringRef key3("1");

  int64_t val1 = 10;
  int64_t val2 = 20;
  int64_t val3 = 30;

  // `offset_vec[0]` is key offset and `offset_vec[1]` to `offset_vec[3]` are values'
  std::vector<int8_t> offset_vec{2, 8, 12, 20};

  // value of HT: SUM(int64)-int64 + COUNT(int64)-int32 + MIN(int64)-int64 +
  // MAX(int64)-int64
  uint32_t init_value_len = 28;
  int8_t* init_value_ptr = default_allocator->allocate(init_value_len);
  int64_t sum_init_val = 0;
  int32_t cnt_init_val = 0;
  int64_t min_init_val = std::numeric_limits<int64_t>::max();
  int64_t max_init_val = std::numeric_limits<int64_t>::min();
  *reinterpret_cast<int64_t*>(init_value_ptr) = sum_init_val;
  *reinterpret_cast<int32_t*>(init_value_ptr + offset_vec[1]) = cnt_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[2]) = min_init_val;
  *reinterpret_cast<int64_t*>(init_value_ptr + offset_vec[3]) = max_init_val;

  AggregationHashTable agg_ht(keys, init_value_ptr, init_value_len);

  // Row0:
  // Generate a key = 1
  int8_t* key1_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_ptr) = key_null;
  *reinterpret_cast<StringRef*>(key1_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_ptr = agg_ht.get(key1_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value1_ptr) += val1;
  *reinterpret_cast<int32_t*>(value1_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[2])[0], val1);
  *reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value1_ptr + offset_vec[3])[0], val1);

  // Row1:
  int8_t* key2_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_ptr) = key_null;
  *reinterpret_cast<StringRef*>(key2_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_ptr = agg_ht.get(key2_ptr);

  // Check init value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr)[0], 0);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1])[0], 0);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], min_init_val);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], max_init_val);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value2_ptr) += val2;
  *reinterpret_cast<int32_t*>(value2_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[2])[0], val2);
  *reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value2_ptr + offset_vec[3])[0], val2);

  // Row2:
  int8_t* key3_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key3_ptr) = key_null;
  *reinterpret_cast<StringRef*>(key3_ptr + offset_vec[0]) = key3;
  // Use get api and return value address
  int8_t* value3_ptr = agg_ht.get(key3_ptr);

  // Some agg operations and update the value, like SUM, COUNT, MIN, MAX
  *reinterpret_cast<int64_t*>(value3_ptr) += val3;
  *reinterpret_cast<int32_t*>(value3_ptr + offset_vec[1]) += 1;
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2]) =
      std::min(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[2])[0], val3);
  *reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3]) =
      std::max(reinterpret_cast<int64_t*>(value3_ptr + offset_vec[3])[0], val3);

  // Final check
  // Check key = 1
  int8_t* key1_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key1_check_ptr) = key_null;
  *reinterpret_cast<StringRef*>(key1_check_ptr + offset_vec[0]) = key1;
  // Use get api and return value address
  int8_t* value1_check_ptr = agg_ht.get(key1_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr)[0], 40);
  CHECK_EQ(reinterpret_cast<int32_t*>(value1_check_ptr + offset_vec[1])[0], 2);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[2])[0], 10);
  CHECK_EQ(reinterpret_cast<int64_t*>(value1_check_ptr + offset_vec[3])[0], 30);

  // Check key = 2
  int8_t* key2_check_ptr = default_allocator->allocate(key_len);
  *reinterpret_cast<bool*>(key2_check_ptr) = key_null;
  *reinterpret_cast<StringRef*>(key2_check_ptr + offset_vec[0]) = key2;
  // Use get api and return value address
  int8_t* value2_check_ptr = agg_ht.get(key2_check_ptr);

  // Check agg result value
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr)[0], 20);
  CHECK_EQ(reinterpret_cast<int32_t*>(value2_check_ptr + offset_vec[1])[0], 1);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[2])[0], 20);
  CHECK_EQ(reinterpret_cast<int64_t*>(value2_check_ptr + offset_vec[3])[0], 20);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
