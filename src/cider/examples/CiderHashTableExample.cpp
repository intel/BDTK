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

#include <exec/operator/join/HashTableFactory.h>
#include <gflags/gflags.h>
#include <algorithm>
#include <iostream>
#include <random>
#include <vector>
#include "exec/operator/join/CiderLinearProbingHashTable.h"
#include "util/Logger.h"

// utils for generate datas
std::random_device rd;
std::mt19937 gen(rd());

int random(int low, int high) {
  std::uniform_int_distribution<> dist(low, high);
  return dist(gen);
}

// hashfunction would be in a sepearete hashfunction lib in future, right now is hardcode
// only for int key
struct MurmurHash {
  size_t operator()(int64_t rawHash) {
    rawHash ^= unsigned(rawHash) >> 33;
    rawHash *= 0xff51afd7ed558ccdL;
    rawHash ^= unsigned(rawHash) >> 33;
    rawHash *= 0xc4ceb9fe1a85ec53L;
    rawHash ^= unsigned(rawHash) >> 33;
    return rawHash;
  }
};

// Different data types should have different equal functions like float and double may
// compare only partial precision Different data types. Equal functions would be in a
// sepearete lib. This is INT equal function.
struct IntEqual {
  bool operator()(int lhs, int rhs) { return lhs == rhs; }
};

// this part should be done in future hashtable selector's code with strategy, right now
// is hardcode. BaseHashTable is the base class and LinearProbeHashTable is the impl in
// this case. key(int) and value(int) data type should be decided by input data type. Hash
// function(MurmurHash) and equal function(IntEqual) would have default in selector for
// each data type. Grower and allocator would be default in impl but currently need to
// copy from impl to set basehashtable
cider_hashtable::HashTableRegistrar<
    cider_hashtable::BaseHashTable<
        int,
        int,
        MurmurHash,
        IntEqual,
        void,
        std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>,
    cider_hashtable::LinearProbeHashTable<int, int, MurmurHash, IntEqual>>
    linear_hashtable("linear_int");

int main(int argc, char** argv) {
  // consturct hashtable from factory
  // BaseHashTable is base hashtable interface
  // all template arguments for basehashtable would be decided during selector
  auto hm = cider_hashtable::HashTableFactory<cider_hashtable::BaseHashTable<
      int,
      int,
      MurmurHash,
      IntEqual,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, int>>>>::Instance()
                .getHashTable("linear_int");

  // build hashtable
  for (int i = 0; i < 100; i++) {
    int key = random(-25, 25);
    int value = random(-1000, 1000);
    hm->emplace(key, value);
  }

  // probe hashtable
  for (int i = 0; i < 100; i++) {
    int probe_key = random(-100, 100);
    auto hm_res_vec = hm->findAll(probe_key);
    if (hm_res_vec.size() > 0) {
      std::cout << "key:" << probe_key << " value:";
      for (auto value : hm_res_vec) {
        std::cout << value << ",";
      }
      std::cout << std::endl;
    }
  }
}
