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

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#define CIDER_BATCH_PROCESSOR_CONTEXT_H

#include <any>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include "cider/CiderAllocator.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/operator/join/HashTableFactory.h"
namespace cider::exec::processor {
using CiderJoinBaseHashTableTemplate = cider_hashtable::BaseHashTable<
    std::any,
    std::pair<cider::exec::nextgen::context::Batch*, int>,
    cider_hashtable::AnyMurmurHash,
    cider_hashtable::AnyEqual,
    void,
    std::allocator<std::pair<cider_hashtable::table_key<std::any>,
                             std::pair<cider::exec::nextgen::context::Batch*, int>>>>;

class JoinHashTable {
 public:
  JoinHashTable(cider_hashtable::HashTableType hashTableType =
                    cider_hashtable::HashTableType::LINEAR_PROBING) {
    cider_hashtable::HashTableSelector<
        std::any,
        std::pair<cider::exec::nextgen::context::Batch*, int>,
        cider_hashtable::AnyMurmurHash,
        cider_hashtable::AnyEqual,
        void,
        std::allocator<std::pair<cider_hashtable::table_key<std::any>,
                                 std::pair<cider::exec::nextgen::context::Batch*, int>>>>
        hashTableSelector;
    hashTableInstance_ = std::move(hashTableSelector.createForJoin(hashTableType));
  }
  // choose hashtable, right now just one
  std::unique_ptr<CiderJoinBaseHashTableTemplate> getHashTable() {
    return std::move(hashTableInstance_);
  }

  void merge_other_hashtables(
      std::vector<std::unique_ptr<JoinHashTable>>& otherJoinTables) {
    std::vector<std::unique_ptr<CiderJoinBaseHashTableTemplate>> otherHashTables;
    for (auto& otherJoinTable : otherJoinTables) {
      otherHashTables.emplace_back(otherJoinTable->getHashTable());
    }
    hashTableInstance_->merge_other_hashtables(otherHashTables);
  }

 private:
  std::unique_ptr<CiderJoinBaseHashTableTemplate> hashTableInstance_;
};

struct HashBuildResult {
  explicit HashBuildResult(std::shared_ptr<JoinHashTable> _table)
      : table(std::move(_table)) {}
  std::shared_ptr<JoinHashTable> table;
};

using HashBuildTableSupplier = std::function<std::optional<HashBuildResult>()>;

class BatchProcessorContext {
 public:
  explicit BatchProcessorContext(const std::shared_ptr<CiderAllocator>& allocator)
      : allocator_(allocator) {}

  const std::shared_ptr<CiderAllocator>& getAllocator() const { return allocator_; }

  void setHashBuildTableSupplier(const HashBuildTableSupplier& hashBuildTableSupplier) {
    buildTableSupplier_ = hashBuildTableSupplier;
  }

  const HashBuildTableSupplier& getHashBuildTableSupplier() const {
    return buildTableSupplier_;
  }

 private:
  std::shared_ptr<CiderAllocator> allocator_;
  HashBuildTableSupplier buildTableSupplier_;
};

using BatchProcessorContextPtr = std::shared_ptr<BatchProcessorContext>;
}  // namespace cider::exec::processor

#endif  // CIDER_BATCH_PROCESSOR_CONTEXT_H
