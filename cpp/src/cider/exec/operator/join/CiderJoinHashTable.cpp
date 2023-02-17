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

#include "exec/operator/join/CiderJoinHashTable.h"

namespace cider::exec::processor {

JoinHashTable::JoinHashTable(cider_hashtable::HashTableType hashTableType) {
  hashTableType_ = hashTableType;
  switch (hashTableType_) {
    case cider_hashtable::HashTableType::LINEAR_PROBING:
      LPHashTableInstance_ =
          std::make_shared<cider_hashtable::LinearProbeHashTable<LP_TEMPLATE>>();
    case cider_hashtable::HashTableType::CHAINED:
      chainedHashTableInstance_ =
          std::make_shared<cider_hashtable::ChainedHashTable<CHAINED_TEMPLATE>>();
  }
}

void JoinHashTable::merge_other_hashtables(
    std::vector<std::unique_ptr<JoinHashTable>>& otherJoinTables) {
  switch (hashTableType_) {
    case cider_hashtable::HashTableType::LINEAR_PROBING: {
      std::vector<std::shared_ptr<JoinLPHashTable>> otherHashTables;
      for (auto& otherJoinTable : otherJoinTables) {
        otherHashTables.emplace_back(otherJoinTable->getLPHashTable());
      }
      LPHashTableInstance_->merge_other_hashtables(otherHashTables);
      break;
    }
    case cider_hashtable::HashTableType::CHAINED: {
      std::vector<std::shared_ptr<JoinChainedHashTable>> otherHashTables;
      for (auto& otherJoinTable : otherJoinTables) {
        otherHashTables.emplace_back(otherJoinTable->getChainedHashTable());
      }
      chainedHashTableInstance_->merge_other_hashtables(otherHashTables);
      break;
    }
    default:
      return;
  }
}

bool JoinHashTable::emplace(int key, CiderJoinBaseValue value) {
  auto key_row_tmp = new cider_hashtable::HT_Row();
  key_row_tmp->make_row(key);
  switch (hashTableType_) {
    case cider_hashtable::HashTableType::LINEAR_PROBING:
      return LPHashTableInstance_->emplace(*key_row_tmp, value);
    case cider_hashtable::HashTableType::CHAINED:
      return chainedHashTableInstance_->emplace(*key_row_tmp, value);
    default:
      return false;
  }
}

std::vector<CiderJoinBaseValue> JoinHashTable::findAll(const int key) {
  cider_hashtable::HT_Row key_row_tmp;
  key_row_tmp.make_row(key);
  switch (hashTableType_) {
    case cider_hashtable::HashTableType::LINEAR_PROBING:
      return LPHashTableInstance_->findAll(key_row_tmp);
    case cider_hashtable::HashTableType::CHAINED:
      return chainedHashTableInstance_->findAll(key_row_tmp);
    default:
      return std::vector<CiderJoinBaseValue>();
  }
}

size_t JoinHashTable::size() {
  switch (hashTableType_) {
    case cider_hashtable::HashTableType::LINEAR_PROBING:
      return LPHashTableInstance_->size();
    case cider_hashtable::HashTableType::CHAINED:
      return chainedHashTableInstance_->size();
    default:
      return LPHashTableInstance_->size();
  }
}

}  // namespace cider::exec::processor