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
#pragma once

#include "exec/nextgen/context/Batch.h"
#include "exec/operator/join/CiderChainedHashTable.h"
#include "exec/operator/join/CiderLinearProbingHashTable.h"
#include "exec/operator/join/HashTableSelector.h"

namespace cider::exec::processor {

struct BatchAndOffset {
  cider::exec::nextgen::context::Batch* batch_ptr;
  int64_t batch_offset;
};

using CiderJoinBaseKey = int;
using CiderJoinBaseValue = BatchAndOffset;

#define LP_TEMPLATE                                                  \
  CiderJoinBaseKey, CiderJoinBaseValue, cider_hashtable::MurmurHash, \
      cider_hashtable::Equal, void,                                  \
      std::allocator<                                                \
          std::pair<cider_hashtable::table_key<CiderJoinBaseKey>, CiderJoinBaseValue>>
#define CHAINED_TEMPLATE                                             \
  CiderJoinBaseKey, CiderJoinBaseValue, cider_hashtable::MurmurHash, \
      cider_hashtable::Equal, void,                                  \
      std::allocator<                                                \
          std::pair<cider_hashtable::table_key<CiderJoinBaseKey>, CiderJoinBaseValue>>

using JoinLPHashTable = cider_hashtable::BaseHashTable<LP_TEMPLATE>;

using JoinChainedHashTable = cider_hashtable::BaseHashTable<CHAINED_TEMPLATE>;

class JoinHashTable {
 public:
  JoinHashTable(cider_hashtable::HashTableType hashTableType =
                    cider_hashtable::HashTableType::LINEAR_PROBING);

  bool set_hash_table_type(cider_hashtable::HashTableType hashTableType);

  std::shared_ptr<JoinLPHashTable> getLPHashTable() { return LPHashTableInstance_; }
  std::shared_ptr<JoinChainedHashTable> getChainedHashTable() {
    return chainedHashTableInstance_;
  }

  void merge_other_hashtables(
      std::vector<std::unique_ptr<JoinHashTable>>& otherJoinTables);

  bool emplace(CiderJoinBaseKey key, CiderJoinBaseValue value);

  std::vector<CiderJoinBaseValue> findAll(const CiderJoinBaseKey key);

  size_t size();

 private:
  cider_hashtable::HashTableType hashTableType_;
  std::shared_ptr<JoinLPHashTable> LPHashTableInstance_;
  std::shared_ptr<JoinChainedHashTable> chainedHashTableInstance_;
};
}  // namespace cider::exec::processor