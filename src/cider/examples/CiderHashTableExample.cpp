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
#include <string>
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
        std::string,
        MurmurHash,
        IntEqual,
        void,
        std::allocator<std::pair<cider_hashtable::table_key<int>, std::string>>>,
    cider_hashtable::LinearProbeHashTable<int, std::string, MurmurHash, IntEqual>>
    linear_hashtable(cider_hashtable::hashtableName::LINEAR_PROBING_INT);

int main(int argc, char** argv) {
  // query: SELECT t_right.col_2 FROM t_left JOIN t_right ON t_left.col_1 = t_right.col_1;

  // input data
  // left table has 200 rows; we use it as probe table
  const int left_table_row_num = 200;
  std::vector<int> table_left_col_1(left_table_row_num);
  for (int i = 0; i < left_table_row_num; i++) {
    table_left_col_1[i] = random(-50, 50);
  }
  // right table has 100 rows and two columns; we use it as build table
  const int right_table_row_num = 100;
  std::vector<int> table_right_col_1(right_table_row_num);
  std::vector<std::string> table_right_col_2(right_table_row_num);
  for (int i = 0; i < right_table_row_num; i++) {
    int tmp = random(-25, 25);
    table_right_col_1[i] = tmp;
    table_right_col_2[i] = std::to_string(tmp);
  }

  // consturct hashtable from factory.
  // BaseHashTable is base hashtable interface.
  // All template arguments for basehashtable would be decided during selector.
  // int is the key's data type, and since the project is string the value type is string.
  auto hm = cider_hashtable::HashTableFactory<cider_hashtable::BaseHashTable<
      int,
      std::string,
      MurmurHash,
      IntEqual,
      void,
      std::allocator<std::pair<cider_hashtable::table_key<int>, std::string>>>>::
                Instance()
                    .getHashTable(cider_hashtable::hashtableName::LINEAR_PROBING_INT);

  // build hashtable based on right table, key is col_1, value is col_2
  // For multi project cols in join, the value type can be vector<col1,col2...> or
  // tuple<col1, col2..> for row databases. While for column stored databases, the value
  // can be column's pointer and offset instead of true value. See Uts named
  // batchAsValueTest in CiderHashTableTest.cpp
  for (int i = 0; i < right_table_row_num; i++) {
    hm->emplace(table_right_col_1[i], table_right_col_2[i]);
  }
  // do probe
  for (int i = 0; i < left_table_row_num; i++) {
    auto probe_key = table_left_col_1[i];
    auto hm_res_vec = hm->findAll(probe_key);
    // matched one ore more key in build table.
    // if the value is part of projected columns, it needs to combine with other columns.
    // if the value is pointer and offset, the true result need further extraction.
    // in this case, the value is just the project column, so just print it.
    if (hm_res_vec.size() > 0) {
      std::cout << "key:" << probe_key << " value:";
      for (auto value : hm_res_vec) {
        std::cout << value << ",";
      }
      std::cout << std::endl;
    }
  }
}
