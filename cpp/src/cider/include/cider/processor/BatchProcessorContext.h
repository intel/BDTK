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

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#define CIDER_BATCH_PROCESSOR_CONTEXT_H

#include <functional>
#include <memory>
#include <optional>

#include "cider/CiderAllocator.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/operator/join/CiderJoinHashTable.h"

using namespace cider::exec::nextgen::context;

namespace cider::exec::processor {

struct HashBuildResult {
  explicit HashBuildResult(std::shared_ptr<JoinHashTable> _table)
      : table(std::move(_table)) {}
  std::shared_ptr<JoinHashTable> table;
};

using HashBuildTableSupplier = std::function<std::optional<HashBuildResult>()>;
using CrossBuildTableSupplier = std::function<std::optional<std::shared_ptr<Batch>>()>;

class BatchProcessorContext {
 public:
  explicit BatchProcessorContext(const std::shared_ptr<CiderAllocator>& allocator)
      : allocator_(allocator) {}

  const std::shared_ptr<CiderAllocator>& getAllocator() const { return allocator_; }

  void setHashBuildTableSupplier(const HashBuildTableSupplier& hashBuildTableSupplier) {
    hashBuildTableSupplier_ = hashBuildTableSupplier;
  }

  const HashBuildTableSupplier& getHashBuildTableSupplier() const {
    return hashBuildTableSupplier_;
  }

  void setCrossJoinBuildTableSupplier(
      const CrossBuildTableSupplier& crossBuildTableSupplier) {
    crossBuildTableSupplier_ = crossBuildTableSupplier;
  }

  const CrossBuildTableSupplier& getCrossJoinBuildTableSupplier() const {
    return crossBuildTableSupplier_;
  }

 private:
  std::shared_ptr<CiderAllocator> allocator_;
  HashBuildTableSupplier hashBuildTableSupplier_;
  CrossBuildTableSupplier crossBuildTableSupplier_;
};

using BatchProcessorContextPtr = std::shared_ptr<BatchProcessorContext>;
}  // namespace cider::exec::processor

#endif  // CIDER_BATCH_PROCESSOR_CONTEXT_H
