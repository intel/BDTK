/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "exec/template/operator/join/hashtable/PerfectHashTable.h"

#include "util/scope.h"

class PerfectJoinHashTableBuilder {
 public:
  PerfectJoinHashTableBuilder() {}

  void allocateDeviceMemory(const JoinColumn& join_column,
                            const HashType layout,
                            HashEntryInfo& hash_entry_info,
                            const size_t shard_count,
                            const int device_id,
                            const int device_count,
                            const Executor* executor) {
    UNREACHABLE();
  }

  std::pair<const StringDictionaryProxy*, const StringDictionaryProxy*> getStrDictProxies(
      const InnerOuter& cols,
      const Executor* executor) const {
    const auto inner_col = cols.first;
    CHECK(inner_col);
    const auto inner_ti = inner_col->get_type_info();
    const auto outer_col = dynamic_cast<const Analyzer::ColumnVar*>(cols.second);
    std::pair<const StringDictionaryProxy*, const StringDictionaryProxy*>
        inner_outer_str_dict_proxies{nullptr, nullptr};
    if (inner_ti.is_string() && outer_col) {
      CHECK(outer_col->get_type_info().is_string());
      inner_outer_str_dict_proxies.first =
          executor->getStringDictionaryProxy(inner_col->get_comp_param(), true);
      CHECK(inner_outer_str_dict_proxies.first);
      inner_outer_str_dict_proxies.second =
          executor->getStringDictionaryProxy(outer_col->get_comp_param(), true);
      CHECK(inner_outer_str_dict_proxies.second);
      if (*inner_outer_str_dict_proxies.first == *inner_outer_str_dict_proxies.second) {
        // Dictionaries are the same - don't need to translate
        CHECK(inner_col->get_comp_param() == outer_col->get_comp_param());
        inner_outer_str_dict_proxies.first = nullptr;
        inner_outer_str_dict_proxies.second = nullptr;
      }
    }
    return inner_outer_str_dict_proxies;
  }

  void initOneToOneHashTableOnCpu(const JoinColumn& join_column,
                                  const ExpressionRange& col_range,
                                  const bool is_bitwise_eq,
                                  const InnerOuter& cols,
                                  const JoinType join_type,
                                  const HashType hash_type,
                                  const HashEntryInfo hash_entry_info,
                                  const int32_t hash_join_invalid_val,
                                  const Executor* executor) {
    auto timer = DEBUG_TIMER(__func__);
    const auto inner_col = cols.first;
    CHECK(inner_col);
    const auto& ti = inner_col->get_type_info();

    CHECK(!hash_table_);
    hash_table_ =
        std::make_unique<PerfectHashTable>(executor->getBufferProvider(),
                                           hash_type,
                                           hash_entry_info.getNormalizedHashEntryCount(),
                                           0);

    auto cpu_hash_table_buff = reinterpret_cast<int32_t*>(hash_table_->getCpuBuffer());
    const auto inner_outer_str_dict_proxies = getStrDictProxies(cols, executor);
    // Should not use all threads to build hash table since this is a library.
    int thread_count = 1;
    int thread_idx = 0;
    std::vector<std::thread> init_cpu_buff_threads;
    init_cpu_buff_threads.emplace_back([hash_entry_info,
                                        hash_join_invalid_val,
                                        thread_idx,
                                        thread_count,
                                        cpu_hash_table_buff] {
      init_hash_join_buff(cpu_hash_table_buff,
                          hash_entry_info.getNormalizedHashEntryCount(),
                          hash_join_invalid_val,
                          thread_idx,
                          thread_count);
    });
    for (auto& t : init_cpu_buff_threads) {
      t.join();
    }
    init_cpu_buff_threads.clear();

    const bool for_semi_join = for_semi_anti_join(join_type);
    std::atomic<int> err{0};
    for (int thread_idx = 0; thread_idx < thread_count; ++thread_idx) {
      init_cpu_buff_threads.emplace_back([hash_join_invalid_val,
                                          &join_column,
                                          inner_outer_str_dict_proxies,
                                          thread_idx,
                                          thread_count,
                                          &ti,
                                          &err,
                                          &col_range,
                                          &is_bitwise_eq,
                                          &for_semi_join,
                                          cpu_hash_table_buff,
                                          hash_entry_info] {
        int partial_err = fill_hash_join_buff_bucketized(
            cpu_hash_table_buff,
            hash_join_invalid_val,
            for_semi_join,
            join_column,
            {static_cast<size_t>(ti.get_size()),
             col_range.getIntMin(),
             col_range.getIntMax(),
             inline_fixed_encoding_null_val(ti),
             is_bitwise_eq,
             col_range.getIntMax() + 1,
             get_join_column_type_kind(ti)},
            inner_outer_str_dict_proxies.first,   // inner proxy
            inner_outer_str_dict_proxies.second,  // outer proxy
            thread_idx,
            thread_count,
            hash_entry_info.bucket_normalization);
        int zero{0};
        err.compare_exchange_strong(zero, partial_err);
      });
    }
    for (auto& t : init_cpu_buff_threads) {
      t.join();
    }
    if (err) {
      // Too many hash entries, need to retry with a 1:many table
      hash_table_ = nullptr;  // clear the hash table buffer
      CIDER_THROW(CiderOneToMoreHashException, "Needs one to many hash");
    }
  }

  void initOneToManyHashTableOnCpu(
      const JoinColumn& join_column,
      const ExpressionRange& col_range,
      const bool is_bitwise_eq,
      const std::pair<const Analyzer::ColumnVar*, const Analyzer::Expr*>& cols,
      const HashEntryInfo hash_entry_info,
      const int32_t hash_join_invalid_val,
      const Executor* executor) {
    auto timer = DEBUG_TIMER(__func__);
    const auto inner_col = cols.first;
    CHECK(inner_col);
    const auto& ti = inner_col->get_type_info();
    CHECK(!hash_table_);
    hash_table_ =
        std::make_unique<PerfectHashTable>(executor->getBufferProvider(),
                                           HashType::OneToMany,
                                           hash_entry_info.getNormalizedHashEntryCount(),
                                           join_column.num_elems);

    auto cpu_hash_table_buff = reinterpret_cast<int32_t*>(hash_table_->getCpuBuffer());
    const auto inner_outer_str_dict_proxies = getStrDictProxies(cols, executor);
    int thread_count = 1;
    std::vector<std::future<void>> init_threads;
    {
      auto timer = DEBUG_TIMER("Perfect Hash OneToMany: Init Hash Join Buffer");
      for (int thread_idx = 0; thread_idx < thread_count; ++thread_idx) {
        init_threads.emplace_back(
            std::async(std::launch::async,
                       init_hash_join_buff,
                       cpu_hash_table_buff,
                       hash_entry_info.getNormalizedHashEntryCount(),
                       hash_join_invalid_val,
                       thread_idx,
                       thread_count));
      }
      for (auto& child : init_threads) {
        child.wait();
      }
      for (auto& child : init_threads) {
        child.get();
      }
    }
    if (ti.get_type() == kDATE) {
      fill_one_to_many_hash_table_bucketized(
          cpu_hash_table_buff,
          hash_entry_info,
          hash_join_invalid_val,
          join_column,
          {static_cast<size_t>(ti.get_size()),
           col_range.getIntMin(),
           col_range.getIntMax(),
           inline_fixed_encoding_null_val(ti),
           is_bitwise_eq,
           col_range.getIntMax() + 1,
           get_join_column_type_kind(ti)},
          inner_outer_str_dict_proxies.first,   // inner proxy
          inner_outer_str_dict_proxies.second,  // outer proxy
          thread_count);
    } else {
      fill_one_to_many_hash_table(cpu_hash_table_buff,
                                  hash_entry_info,
                                  hash_join_invalid_val,
                                  join_column,
                                  {static_cast<size_t>(ti.get_size()),
                                   col_range.getIntMin(),
                                   col_range.getIntMax(),
                                   inline_fixed_encoding_null_val(ti),
                                   is_bitwise_eq,
                                   col_range.getIntMax() + 1,
                                   get_join_column_type_kind(ti)},
                                  inner_outer_str_dict_proxies.first,   // inner proxy
                                  inner_outer_str_dict_proxies.second,  // outer proxy
                                  thread_count);
    }
  }

  std::unique_ptr<PerfectHashTable> getHashTable() { return std::move(hash_table_); }

  static size_t get_entries_per_shard(const size_t total_entry_count,
                                      const size_t shard_count) {
    CHECK_NE(size_t(0), shard_count);
    return (total_entry_count + shard_count - 1) / shard_count;
  }

  const bool for_semi_anti_join(const JoinType join_type) {
    return join_type == JoinType::SEMI || join_type == JoinType::ANTI;
  }

 private:
  std::unique_ptr<PerfectHashTable> hash_table_;
};
