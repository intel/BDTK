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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <any>
#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>
#include "cider/CiderException.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/operator/join/CiderF14HashTable.h"
#include "exec/operator/join/CiderJoinHashTable.h"
#include "exec/operator/join/CiderStdUnorderedHashTable.h"
#include "exec/operator/join/HashTableSelector.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/ArrowArrayBuilder.h"
#include "type/data/sqltypes.h"
#include "util/Logger.h"

// hash function for test collision
struct Hash {
  size_t operator()(int v) { return v % 7; }
};

std::random_device rd;
std::mt19937 gen(rd());

int random(int low, int high) {
  std::uniform_int_distribution<> dist(low, high);
  return dist(gen);
}

TEST(CiderHashTableTest, HashFuntionTests) {
  int32_t key32 = 0x12345678;
  int64_t key64 = 0x1234567812345678;
  int8_t* array_32 = reinterpret_cast<int8_t*>(&key32);
  int8_t* array_64 = reinterpret_cast<int8_t*>(&key64);
  auto hash_32 = cider_hashtable::hash_functions::RowHash()(array_32, sizeof(int32_t));
  auto hash_64 = cider_hashtable::hash_functions::RowHash()(array_64, sizeof(int64_t));
  auto row_key32 = cider_hashtable::HT_Row();
  row_key32.make_row(key32);
  EXPECT_EQ(hash_32, cider_hashtable::hash_functions::RowHash()(row_key32));
  auto row_key64 = cider_hashtable::HT_Row();
  row_key64.make_row(key64);
  EXPECT_EQ(hash_64, cider_hashtable::hash_functions::RowHash()(row_key64));

  float key_f = 1.234;
  double key_d = 1234.1234;
  int8_t* array_f = reinterpret_cast<int8_t*>(&key_f);
  int8_t* array_d = reinterpret_cast<int8_t*>(&key_d);
  auto hash_f = cider_hashtable::hash_functions::RowHash()(array_f, sizeof(float));
  auto hash_d = cider_hashtable::hash_functions::RowHash()(array_d, sizeof(double));
  auto row_f = cider_hashtable::HT_Row();
  row_f.make_row(key_f);
  EXPECT_EQ(hash_f, cider_hashtable::hash_functions::RowHash()(row_f));
  auto row_d = cider_hashtable::HT_Row();
  row_d.make_row(key_d);
  EXPECT_EQ(hash_d, cider_hashtable::hash_functions::RowHash()(row_d));

  std::string key_str("six666");
  const int8_t* array_string = reinterpret_cast<const int8_t*>(key_str.c_str());
  auto hash_string =
      cider_hashtable::hash_functions::MurmurHash3_32(array_string, key_str.size());
  auto row_string = cider_hashtable::HT_Row();
  row_string.make_row(key_str);
  EXPECT_EQ(hash_string, cider_hashtable::hash_functions::RowHash()(row_string));
}

TEST(CiderHashTableTest, JoinHashTableCreateTest) {
  cider::exec::processor::JoinHashTable join_hashtable1(
      cider_hashtable::HashTableType::LINEAR_PROBING);
  auto hm1 = join_hashtable1.getLPHashTable();
  cider::exec::processor::JoinHashTable join_hashtable2(
      cider_hashtable::HashTableType::CHAINED);
  auto hm2 = join_hashtable2.getChainedHashTable();
}

void joinHashTableTest(cider_hashtable::HashTableType hashtable_type) {
  using namespace cider::exec::nextgen::context;

  auto joinHashTable1 = new cider::exec::processor::JoinHashTable(hashtable_type);
  auto joinHashTable2 = new cider::exec::processor::JoinHashTable(hashtable_type);
  auto joinHashTable3 = new cider::exec::processor::JoinHashTable(hashtable_type);

  auto input_builder = ArrowArrayBuilder();

  auto&& [schema, array] =
      input_builder.setRowNum(10)
          .addColumn<int64_t>(
              "l_bigint", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
          .addColumn<int32_t>(
              "l_int", CREATE_SUBSTRAIT_TYPE(I32), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
          .build();
  Batch build_batch(*schema, *array);

  for (int i = 0; i < 10; i++) {
    int key = *(
        (reinterpret_cast<int*>(const_cast<void*>(array->children[1]->buffers[1]))) + i);
    joinHashTable1->emplace(key, {&build_batch, i});
    joinHashTable2->emplace(key, {&build_batch, i});
    joinHashTable3->emplace(key, {&build_batch, i});
  }
  switch (hashtable_type) {
    case cider_hashtable::HashTableType::LINEAR_PROBING: {
      auto hm = joinHashTable1->getLPHashTable();
      EXPECT_EQ(hm->size(), 10);
      break;
    }
    case cider_hashtable::HashTableType::CHAINED: {
      auto hm = joinHashTable1->getChainedHashTable();
      EXPECT_EQ(hm->size(), 10);
      break;
    }
    default:
      return;
  }
  for (auto key : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
    auto hm_res_vec = joinHashTable1->findAll(key);
    EXPECT_EQ(hm_res_vec.size(), 1);
    EXPECT_EQ(hm_res_vec[0].batch_offset, key);
  }
  std::vector<std::unique_ptr<cider::exec::processor::JoinHashTable>> otherTables;
  otherTables.emplace_back(
      std::move(std::unique_ptr<cider::exec::processor::JoinHashTable>(joinHashTable2)));
  otherTables.emplace_back(
      std::move(std::unique_ptr<cider::exec::processor::JoinHashTable>(joinHashTable3)));
  joinHashTable1->merge_other_hashtables(otherTables);
  EXPECT_EQ(joinHashTable1->size(), 30);
}

// test build and probe for cider
TEST(CiderHashTableTest, JoinHashTableTest) {
  joinHashTableTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  joinHashTableTest(cider_hashtable::HashTableType::CHAINED);
}

TEST(CiderHashTableTest, keyCollisionTest) {
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key
  cider_hashtable::LinearProbeHashTable<int, int, Hash, cider_hashtable::Equal>
      linear_probing_hm(16, NULL);
  cider_hashtable::ChainedHashTable<int, int, Hash, cider_hashtable::Equal> chained_hm;
  StdMapDuplicateKeyWrapper<int, int> udup_map;
  linear_probing_hm.emplace(1, 1);
  linear_probing_hm.emplace(1, 2);
  linear_probing_hm.emplace(15, 1515);
  linear_probing_hm.emplace(8, 88);
  linear_probing_hm.emplace(1, 5);
  linear_probing_hm.emplace(1, 6);
  chained_hm.emplace(1, 1);
  chained_hm.emplace(1, 2);
  chained_hm.emplace(15, 1515);
  chained_hm.emplace(8, 88);
  chained_hm.emplace(1, 5);
  chained_hm.emplace(1, 6);
  udup_map.insert(1, 1);
  udup_map.insert(1, 2);
  udup_map.insert(15, 1515);
  udup_map.insert(8, 88);
  udup_map.insert(1, 5);
  udup_map.insert(1, 6);

  for (auto key_iter : udup_map.getMap()) {
    auto dup_res_vec = udup_map.findAll(key_iter.first);
    auto linear_probing_hm_res_vec = linear_probing_hm.findAll(key_iter.first);
    auto chained_hm_res_vec = chained_hm.findAll(key_iter.first);
    std::sort(dup_res_vec.begin(), dup_res_vec.end());
    std::sort(linear_probing_hm_res_vec.begin(), linear_probing_hm_res_vec.end());
    std::sort(chained_hm_res_vec.begin(), chained_hm_res_vec.end());
    EXPECT_TRUE(dup_res_vec == linear_probing_hm_res_vec);
    EXPECT_TRUE(dup_res_vec == chained_hm_res_vec);
  }
}

void hashtableRandomInsertTest(cider_hashtable::HashTableType hashtable_type) {
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::hash_functions::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>
      hashTableSelector;
  auto hm = hashTableSelector.createForJoin(hashtable_type);
  StdMapDuplicateKeyWrapper<int, int> dup_map;
  for (int i = 0; i < 10000; i++) {
    int key = random(-1000, 1000);
    int value = random(-1000, 1000);
    hm->emplace(key, value);
    dup_map.insert(std::move(key), std::move(value));
  }
  for (auto key_iter : dup_map.getMap()) {
    auto dup_res_vec = dup_map.findAll(key_iter.first);
    auto hm_res_vec = hm->findAll(key_iter.first);
    std::sort(dup_res_vec.begin(), dup_res_vec.end());
    std::sort(hm_res_vec.begin(), hm_res_vec.end());
    EXPECT_TRUE(dup_res_vec == hm_res_vec);
  }
}

void HTRowAsKeyHashtableRandomInsertTest(cider_hashtable::HashTableType hashtable_type) {
  cider_hashtable::HashTableSelector<
      cider_hashtable::HT_Row,
      int,
      cider_hashtable::hash_functions::RowHash,
      cider_hashtable::HTRowEqual,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<cider_hashtable::HT_Row>, int>>>
      hashTableSelector;
  auto hm = hashTableSelector.createForJoin(hashtable_type);
  StdMapDuplicateKeyWrapper<int, int> dup_map;
  for (int i = 0; i < 10000; i++) {
    int key = random(-1000, 1000);
    auto key_row_tmp = new cider_hashtable::HT_Row();
    key_row_tmp->make_row(key);
    int value = random(-1000, 1000);
    hm->emplace(*key_row_tmp, value);
    dup_map.insert(std::move(key), std::move(value));
  }
  for (auto key_iter : dup_map.getMap()) {
    auto dup_res_vec = dup_map.findAll(key_iter.first);
    cider_hashtable::HT_Row key_row_tmp;
    int key_value_not_const = key_iter.first;
    key_row_tmp.make_row(key_value_not_const);
    auto hm_res_vec = hm->findAll(key_row_tmp);
    std::sort(dup_res_vec.begin(), dup_res_vec.end());
    std::sort(hm_res_vec.begin(), hm_res_vec.end());
    EXPECT_TRUE(dup_res_vec == hm_res_vec);
  }
}

TEST(CiderHashTableTest, randomInsertAndfindTest) {
  hashtableRandomInsertTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  // hashtableRandomInsertTest(cider_hashtable::HashTableType::CHAINED);
}

TEST(CiderHashTableTest, randomInsertAndfindTestOfRowKey) {
  HTRowAsKeyHashtableRandomInsertTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  HTRowAsKeyHashtableRandomInsertTest(cider_hashtable::HashTableType::CHAINED);
}

TEST(CiderHashTableTest, LPHashMapTest) {
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::hash_functions::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>
      hashTableSelector;
  auto hm =
      hashTableSelector.createForJoin(cider_hashtable::HashTableType::LINEAR_PROBING);
  for (int i = 0; i < 10000; i++) {
    int key = random(-100000, 10000);
    int value = random(-10000, 10000);
    hm->emplace(key, value);
  }
  for (int i = 0; i < 1000000; i++) {
    int key = random(-10000, 10000);
    auto hm_res_vec = hm->findAll(key);
  }
}

TEST(CiderHashTableTest, ChainedHashMapTest) {
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::hash_functions::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>
      hashTableSelector;
  auto hm = hashTableSelector.createForJoin(cider_hashtable::HashTableType::CHAINED);
  for (int i = 0; i < 10000; i++) {
    int key = random(-100000, 10000);
    int value = random(-10000, 10000);
    hm->emplace(key, value);
  }
  for (int i = 0; i < 1000000; i++) {
    int key = random(-10000, 10000);
    auto hm_res_vec = hm->findAll(key);
  }
}

TEST(CiderHashTableTest, dupMapTest) {
  StdMapDuplicateKeyWrapper<int, int> dup_map;
  for (int i = 0; i < 100000; i++) {
    int key = random(-10000, 10000);
    int value = random(-10000, 10000);
    dup_map.insert(std::move(key), std::move(value));
  }
  for (int i = 0; i < 1000000; i++) {
    int key = random(-10000, 10000);
    auto dup_res_vec = dup_map.findAll(key);
  }
}

TEST(CiderHashTableTest, f14MapTest) {
  F14MapDuplicateKeyWrapper<int, int> f14_map;
  for (int i = 0; i < 100000; i++) {
    int key = random(-10000, 10000);
    int value = random(-10000, 10000);
    f14_map.insert(std::move(key), std::move(value));
  }
  for (int i = 0; i < 1000000; i++) {
    int key = random(-10000, 10000);
    auto f14_res_vec = f14_map.findAll(key);
  }
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
