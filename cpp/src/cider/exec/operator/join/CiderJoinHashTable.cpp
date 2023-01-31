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
  cider_hashtable::HashTableSelector<
      CiderJoinBaseKey,
      CiderJoinBaseValue,
      cider_hashtable::MurmurHash,
      cider_hashtable::Equal,
      void,
      std::allocator<
          std::pair<cider_hashtable::table_key<CiderJoinBaseKey>, CiderJoinBaseValue>>>
      hashTableSelector;
  hashTableInstance_ = std::move(hashTableSelector.createForJoin(hashTableType));
}
// choose hashtable, right now just one
std::shared_ptr<CiderJoinBaseHashTable> JoinHashTable::getHashTable() {
  return hashTableInstance_;
}

void JoinHashTable::merge_other_hashtables(
    std::vector<std::unique_ptr<JoinHashTable>>& otherJoinTables) {
  std::vector<std::shared_ptr<CiderJoinBaseHashTable>> otherHashTables;
  for (auto& otherJoinTable : otherJoinTables) {
    otherHashTables.emplace_back(otherJoinTable->getHashTable());
  }
  hashTableInstance_->merge_other_hashtables(otherHashTables);
}

bool JoinHashTable::emplace(CiderJoinBaseKey key, CiderJoinBaseValue value) {
  return hashTableInstance_->emplace(key, value);
}
std::vector<CiderJoinBaseValue> JoinHashTable::findAll(const CiderJoinBaseKey key) {
  return hashTableInstance_->findAll(key);
}

}  // namespace cider::exec::processor