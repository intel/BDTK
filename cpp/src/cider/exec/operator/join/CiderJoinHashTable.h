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
#pragma once

#include <any>
#include <memory>
#include <vector>
#include "exec/nextgen/context/Batch.h"
#include "exec/operator/join/HashTableFactory.h"

namespace cider::exec::processor {

struct BatchOffset {
  cider::exec::nextgen::context::Batch* batch_ptr;
  int64_t batch_offset;
};

using CiderJoinBaseKey = int;
using CiderJoinBaseValue = BatchOffset;

using CiderJoinBaseHashTable = cider_hashtable::BaseHashTable<
    CiderJoinBaseKey,
    CiderJoinBaseValue,
    cider_hashtable::MurmurHash,
    cider_hashtable::Equal,
    void,
    std::allocator<
        std::pair<cider_hashtable::table_key<CiderJoinBaseKey>, CiderJoinBaseValue>>>;

class JoinHashTable {
 public:
  JoinHashTable(cider_hashtable::HashTableType hashTableType =
                    cider_hashtable::HashTableType::LINEAR_PROBING);
  // choose hashtable, right now just one
  std::shared_ptr<CiderJoinBaseHashTable> getHashTable();

  void merge_other_hashtables(
      std::vector<std::unique_ptr<JoinHashTable>>& otherJoinTables);

  bool emplace(CiderJoinBaseKey key, CiderJoinBaseValue value);

  std::vector<CiderJoinBaseValue> findAll(const CiderJoinBaseKey key);

 private:
  std::shared_ptr<CiderJoinBaseHashTable> hashTableInstance_;
};
}  // namespace cider::exec::processor