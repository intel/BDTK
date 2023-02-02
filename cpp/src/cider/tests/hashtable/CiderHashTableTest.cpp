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
#include "exec/operator/join/HashTableFactory.h"
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

TEST(CiderHashTableTest, factoryTest) {
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>
      hashTableSelector;
  auto hm =
      hashTableSelector.createForJoin(cider_hashtable::HashTableType::LINEAR_PROBING);
  auto hm2 = hashTableSelector.createForJoin(cider_hashtable::HashTableType::CHAINED);
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
  EXPECT_EQ(joinHashTable1->getHashTable()->size(), 10);
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
  EXPECT_EQ(joinHashTable1->getHashTable()->size(), 30);
}

// test build and probe for cider
TEST(CiderHashTableTest, JoinHashTableTest) {
  joinHashTableTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  joinHashTableTest(cider_hashtable::HashTableType::CHAINED);
}

void mergeTest(cider_hashtable::HashTableType hashtable_type) {
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>
      hashTableSelector;
  auto hm1 = hashTableSelector.createForJoin(hashtable_type);
  auto hm2 = hashTableSelector.createForJoin(hashtable_type);
  auto hm3 = hashTableSelector.createForJoin(hashtable_type);
  for (int i = 0; i < 100; i++) {
    int value = random(-1000, 1000);
    hm1->emplace(random(-10, 10), value);
    hm2->emplace(random(-10, 10), value);
    hm3->emplace(random(-10, 10), value);
  }
  // cider_hashtable::BaseHashTable<int, int, MurmurHash, Equal> hb1 = hm1;
  std::vector<std::shared_ptr<cider_hashtable::BaseHashTable<
      int,
      int,
      cider_hashtable::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>>>
      other_tables;
  other_tables.emplace_back(std::move(hm1));
  other_tables.emplace_back(std::move(hm2));
  other_tables.emplace_back(std::move(hm3));

  auto hm_final = hashTableSelector.createForJoin(hashtable_type);
  hm_final->merge_other_hashtables(std::move(other_tables));
  EXPECT_EQ(hm_final->size(), 300);
}

TEST(CiderHashTableTest, mergeTest) {
  mergeTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  mergeTest(cider_hashtable::HashTableType::CHAINED);
}

// test value type for probe
TEST(CiderHashTableTest, batchAsValueTest) {
  using namespace cider::exec::nextgen::context;
  auto input_builder = ArrowArrayBuilder();

  auto&& [schema, array] =
      input_builder.setRowNum(10)
          .addColumn<int64_t>(
              "l_bigint", CREATE_SUBSTRAIT_TYPE(I64), {1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
          .addColumn<int32_t>(
              "l_int", CREATE_SUBSTRAIT_TYPE(I32), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
          .build();

  Batch build_batch(*schema, *array);
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key using
  cider_hashtable::HashTableSelector<
      int,
      std::pair<cider::exec::nextgen::context::Batch*, int>,
      cider_hashtable::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>,
                               std::pair<cider::exec::nextgen::context::Batch*, int>>>>
      hashTableSelector;
  auto hm =
      hashTableSelector.createForJoin(cider_hashtable::HashTableType::LINEAR_PROBING);

  StdMapDuplicateKeyWrapper<int, std::pair<Batch*, int>> dup_map;

  for (int i = 0; i < 10; i++) {
    int key = *(
        (reinterpret_cast<int*>(const_cast<void*>(array->children[1]->buffers[1]))) + i);
    hm->emplace(key, std::make_pair(&build_batch, i));
    dup_map.insert(std::move(key), std::make_pair(&build_batch, i));
  }
  for (auto key_iter : dup_map.getMap()) {
    auto dup_res_vec = dup_map.findAll(key_iter.first);
    auto hm_res_vec = hm->findAll(key_iter.first);
    // this is the same logic in codegen
    auto hm_res_vec_ptr =
        std::make_shared<std::vector<std::pair<Batch*, int>>>(hm_res_vec);
    auto hm_res_vec_value = *hm_res_vec_ptr;
    std::sort(dup_res_vec.begin(), dup_res_vec.end());
    std::sort(hm_res_vec_value.begin(), hm_res_vec_value.end());
    EXPECT_TRUE(dup_res_vec == hm_res_vec_value);
  }
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
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::MurmurHash,
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

TEST(CiderHashTableTest, randomInsertAndfindTest) {
  hashtableRandomInsertTest(cider_hashtable::HashTableType::LINEAR_PROBING);
  hashtableRandomInsertTest(cider_hashtable::HashTableType::CHAINED);
}

TEST(CiderHashTableTest, LPHashMapTest) {
  // Create a LinearProbeHashTable  with 16 buckets and 0 as the empty key
  cider_hashtable::HashTableSelector<
      int,
      int,
      cider_hashtable::MurmurHash,
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
      cider_hashtable::MurmurHash,
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
