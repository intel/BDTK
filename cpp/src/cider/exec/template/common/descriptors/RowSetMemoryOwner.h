/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include <boost/noncopyable.hpp>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cider/CiderAllocator.h"
#include "exec/template/StringDictionaryGenerations.h"
#include "robin_hood.h"
#include "type/data/string/StringDictionaryProxy.h"
#include "util/Logger.h"
#include "util/memory/Buffer/AbstractBuffer.h"
#include "util/memory/DataProvider.h"
#include "util/memory/allocators/ArenaAllocator.h"
#include "util/quantile.h"
namespace Catalog_Namespace {
class Catalog;
}

class ResultSet;

/**
 * Handles allocations and outputs for all stages in a query, either explicitly or via a
 * managed allocator object
 */
class RowSetMemoryOwner final : public SimpleAllocator, boost::noncopyable {
 public:
  RowSetMemoryOwner(DataProvider* data_provider,
                    const size_t arena_block_size,
                    const size_t num_kernel_threads = 0)
      : data_provider_(data_provider), arena_block_size_(arena_block_size) {}

  RowSetMemoryOwner(DataProvider* data_provider,
                    std::shared_ptr<CiderAllocator> allocator)
      : data_provider_(data_provider), allocator_(allocator) {
    CHECK(allocator_ != nullptr);
  }

  int8_t* allocate(const size_t num_bytes, const size_t thread_idx = 0) override {
    return allocator_->allocate(num_bytes);
  }

  int8_t* allocateCountDistinctBuffer(const size_t num_bytes,
                                      const size_t thread_idx = 0) {
    int8_t* buffer = allocator_->allocate(num_bytes);
    std::memset(buffer, 0, num_bytes);
    addCountDistinctBuffer(buffer, num_bytes, /*physical_buffer=*/true);
    return buffer;
  }

  void addCountDistinctBuffer(int8_t* count_distinct_buffer,
                              const size_t bytes,
                              const bool physical_buffer) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    count_distinct_bitmaps_.emplace_back(
        CountDistinctBitmapBuffer{count_distinct_buffer, bytes, physical_buffer});
  }

  void addCountDistinctSet(robin_hood::unordered_set<int64_t>* count_distinct_set) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    count_distinct_sets_.push_back(count_distinct_set);
  }

  std::string* addString(const std::string& str) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    strings_.emplace_back(str);
    return &strings_.back();
  }

  std::vector<int64_t>* addArray(const std::vector<int64_t>& arr) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    arrays_.emplace_back(arr);
    return &arrays_.back();
  }

  StringDictionaryProxy* addStringDict(std::shared_ptr<StringDictionary> str_dict,
                                       const int dict_id,
                                       const int64_t generation) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = str_dict_proxy_owned_.find(dict_id);
    if (it != str_dict_proxy_owned_.end()) {
      CHECK_EQ(it->second->getDictionary(), str_dict.get());
      it->second->updateGeneration(generation);
      return it->second.get();
    }
    it = str_dict_proxy_owned_
             .emplace(
                 dict_id,
                 std::make_shared<StringDictionaryProxy>(str_dict, dict_id, generation))
             .first;
    return it->second.get();
  }

  StringDictionaryProxy* getStringDictProxy(const int dict_id) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = str_dict_proxy_owned_.find(dict_id);
    CHECK(it != str_dict_proxy_owned_.end());
    return it->second.get();
  }

  StringDictionaryProxy* getOrAddStringDictProxy(const int db_id,
                                                 const int dict_id_in,
                                                 const bool with_generation);

  void addLiteralStringDictProxy(
      std::shared_ptr<StringDictionaryProxy> lit_str_dict_proxy) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    lit_str_dict_proxy_ = lit_str_dict_proxy;
  }

  StringDictionaryProxy* getLiteralStringDictProxy() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return lit_str_dict_proxy_.get();
  }

  ~RowSetMemoryOwner() {
    for (auto count_distinct_set : count_distinct_sets_) {
      delete count_distinct_set;
    }
  }

  void setDictionaryGenerations(StringDictionaryGenerations generations) {
    string_dictionary_generations_ = generations;
  }

  StringDictionaryGenerations& getStringDictionaryGenerations() {
    return string_dictionary_generations_;
  }

  quantile::TDigest* nullTDigest(double const q);

 private:
  struct CountDistinctBitmapBuffer {
    int8_t* ptr;
    const size_t size;
    const bool physical_buffer;
  };

  std::vector<CountDistinctBitmapBuffer> count_distinct_bitmaps_;
  std::vector<robin_hood::unordered_set<int64_t>*> count_distinct_sets_;
  std::list<std::string> strings_;
  std::list<std::vector<int64_t>> arrays_;
  std::unordered_map<int, std::shared_ptr<StringDictionaryProxy>> str_dict_proxy_owned_;
  std::shared_ptr<StringDictionaryProxy> lit_str_dict_proxy_;
  StringDictionaryGenerations string_dictionary_generations_;
  std::vector<std::unique_ptr<quantile::TDigest>> t_digests_;

  DataProvider* data_provider_;  // for metadata lookups
  size_t arena_block_size_;      // for cloning
  std::shared_ptr<CiderAllocator> allocator_;

  mutable std::mutex state_mutex_;

  friend class ResultSet;
  friend class QueryExecutionContext;
};
