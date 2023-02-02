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

#include "exec/template/operator/join/hashtable/RangeJoinHashTable.h"

#include "exec/template/CodeGenerator.h"
#include "exec/template/Execute.h"
#include "exec/template/operator/join/JoinLoop.h"
#include "exec/template/operator/join/hashtable/PerfectJoinHashTable.h"
#include "exec/template/operator/join/hashtable/builders/BaselineHashTableBuilder.h"
#include "exec/template/operator/join/hashtable/runtime/HashJoinKeyHandlers.h"

// clang-format off
///
/// NOTE(jclay): Handling Range Joins With Mixed Compression:
/// ========================================
///
///  First, let's take a concrete example of a query that is rewritten as a range join.
///  Notice in the first code block, that the condition operator is an Overlaps operator.
///  The LHS is a column, and the RHS is the range operator. In order to have the hash table
///  build and probe work properly, we need to ensure that the approriate runtime functions 
///  are selected. The following breakdown is provided to help document how the appropriate 
///  runtime funditon is selected.
///
///    * The LHS of the RangeOper is used to build the hash table
///    * The LHS of the OverlapsOper + the RHS of the RangeOper is used as probe
///
// clang-format on

std::shared_ptr<RangeJoinHashTable> RangeJoinHashTable::getInstance(
    const std::shared_ptr<Analyzer::BinOper> condition,
    const Analyzer::RangeOper* range_expr,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const JoinType join_type,
    const int device_count,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    Executor* executor,
    const HashTableBuildDagMap& hashtable_build_dag_map,
    const RegisteredQueryHint& query_hint,
    const TableIdToNodeMap& table_id_to_node_map) {
  UNREACHABLE();
  return nullptr;
}

void RangeJoinHashTable::reifyWithLayout(const HashType layout) {
  auto timer = DEBUG_TIMER(__func__);
  CHECK(layout == HashType::OneToMany);

  const auto& query_info =
      get_inner_query_info(HashJoin::getInnerTableId(inner_outer_pairs_), query_infos_)
          .info;

  if (query_info.fragments.empty()) {
    return;
  }

  VLOG(1) << "Reify with layout " << getHashTypeString(layout)
          << "for table_id: " << getInnerTableId();

  std::vector<ColumnsForDevice> columns_per_device;
  ;

  auto buffer_provider = executor_->getBufferProvider();
  const auto shard_count = shardCount();
  for (int device_id = 0; device_id < device_count_; ++device_id) {
    const auto fragments = query_info.fragments;
    const auto columns_for_device = fetchColumnsForDevice(fragments, device_id);
    columns_per_device.push_back(columns_for_device);
  }

  inverse_bucket_sizes_for_dimension_.clear();

  const auto bucket_range =
      dynamic_cast<const Analyzer::Constant*>(range_expr_->get_right_operand());

  CHECK(bucket_range);
  CHECK(bucket_range->get_type_info().is_fp() &&
        bucket_range->get_type_info().get_size() == 8);  // TODO

  const auto bucket_range_datum = bucket_range->get_constval();

  inverse_bucket_sizes_for_dimension_.emplace_back(1. / bucket_range_datum.doubleval);
  inverse_bucket_sizes_for_dimension_.emplace_back(1. / bucket_range_datum.doubleval);

  setInverseBucketSizeInfo(
      inverse_bucket_sizes_for_dimension_, columns_per_device, device_count_);

  auto [entry_count, emitted_keys_count] =
      computeRangeHashTableCounts(shard_count, columns_per_device);

  size_t hash_table_size = OverlapsJoinHashTable::calculateHashTableSize(
      inverse_bucket_sizes_for_dimension_.size(), emitted_keys_count, entry_count);

  VLOG(1) << "Finalized range join hash table: entry count " << entry_count
          << " hash table size " << hash_table_size;

  std::vector<std::future<void>> init_threads;
  for (int device_id = 0; device_id < device_count_; ++device_id) {
    const auto fragments = query_info.fragments;
    init_threads.push_back(
        std::async(std::launch::async,
                   &RangeJoinHashTable::reifyForDevice,
                   this,
                   /* columns_for_device     */ columns_per_device[device_id],
                   /* layout_type            */ layout,
                   /* entry_count            */ entry_count,
                   /* emitted_keys_count     */ emitted_keys_count,
                   /* device_id              */ device_id,
                   /* parent_thread_id       */ logger::thread_id()));
  }
  for (auto& init_thread : init_threads) {
    init_thread.wait();
  }
  for (auto& init_thread : init_threads) {
    init_thread.get();
  }
}

void RangeJoinHashTable::reifyForDevice(const ColumnsForDevice& columns_for_device,
                                        const HashType layout,
                                        const size_t entry_count,
                                        const size_t emitted_keys_count,
                                        const int device_id,
                                        const logger::ThreadId parent_thread_id) {
  DEBUG_TIMER_NEW_THREAD(parent_thread_id);
  CHECK_EQ(getKeyComponentWidth(), size_t(8));
  CHECK(layoutRequiresAdditionalBuffers(layout));
  const auto effective_memory_level = getEffectiveMemoryLevel(inner_outer_pairs_);

  if (effective_memory_level == Data_Namespace::MemoryLevel::CPU_LEVEL) {
    VLOG(1) << "Building range join hash table on CPU.";
    auto hash_table = initHashTableOnCpu(columns_for_device.join_columns,
                                         columns_for_device.join_column_types,
                                         columns_for_device.join_buckets,
                                         layout,
                                         entry_count,
                                         emitted_keys_count);
    CHECK(hash_table);
    CHECK_EQ(Data_Namespace::CPU_LEVEL, effective_memory_level);
    CHECK_EQ(hash_tables_for_device_.size(), size_t(1));
    hash_tables_for_device_[0] = std::move(hash_table);
  } else {
    UNREACHABLE();
  }
}

std::shared_ptr<BaselineHashTable> RangeJoinHashTable::initHashTableOnCpu(
    const std::vector<JoinColumn>& join_columns,
    const std::vector<JoinColumnTypeInfo>& join_column_types,
    const std::vector<JoinBucketInfo>& join_bucket_info,
    const HashType layout,
    const size_t entry_count,
    const size_t emitted_keys_count) {
  auto timer = DEBUG_TIMER(__func__);
  decltype(std::chrono::steady_clock::now()) ts1, ts2;
  ts1 = std::chrono::steady_clock::now();
  const auto composite_key_info =
      HashJoin::getCompositeKeyInfo(inner_outer_pairs_, executor_);
  CHECK(!join_columns.empty());
  CHECK(!join_bucket_info.empty());

  setOverlapsHashtableMetaInfo(
      max_hashtable_size_, bucket_threshold_, inverse_bucket_sizes_for_dimension_);
  generateCacheKey(max_hashtable_size_, max_hashtable_size_);

  if ((query_plan_dag_.compare(EMPTY_QUERY_PLAN) == 0 ||
       hashtable_cache_key_ == EMPTY_HASHED_PLAN_DAG_KEY) &&
      inner_outer_pairs_.front().first->get_table_id() > 0) {
    // sometimes we cannot retrieve query plan dag, so try to recycler cache
    // with the old-passioned cache key if we deal with hashtable of non-temporary table
    AlternativeCacheKeyForOverlapsHashJoin cache_key{inner_outer_pairs_,
                                                     join_columns.front().num_elems,
                                                     composite_key_info_.cache_key_chunks,
                                                     condition_->get_optype(),
                                                     max_hashtable_size_,
                                                     bucket_threshold_,
                                                     inverse_bucket_sizes_for_dimension_};
    hashtable_cache_key_ = getAlternativeCacheKey(cache_key);
    VLOG(2) << "Use alternative hashtable cache key due to unavailable query plan dag "
               "extraction (hashtable_cache_key: "
            << hashtable_cache_key_ << ")";
  }

  std::lock_guard<std::mutex> cpu_hash_table_buff_lock(cpu_hash_table_buff_mutex_);
  if (auto generic_hash_table =
          initHashTableOnCpuFromCache(hashtable_cache_key_,
                                      CacheItemType::OVERLAPS_HT,
                                      DataRecyclerUtil::CPU_DEVICE_IDENTIFIER)) {
    if (auto hash_table =
            std::dynamic_pointer_cast<BaselineHashTable>(generic_hash_table)) {
      // See if a hash table of a different layout was returned.
      // If it was OneToMany, we can reuse it on ManyToMany.
      if (layout == HashType::ManyToMany &&
          hash_table->getLayout() == HashType::OneToMany) {
        // use the cached hash table
        layout_override_ = HashType::ManyToMany;
        return hash_table;
      }
    }
  }

  CHECK(layoutRequiresAdditionalBuffers(layout));
  const auto key_component_count =
      join_bucket_info[0].inverse_bucket_sizes_for_dimension.size();

  auto key_handler =
      RangeKeyHandler(false,
                      key_component_count,
                      &join_columns[0],
                      join_bucket_info[0].inverse_bucket_sizes_for_dimension.data());

  BaselineJoinHashTableBuilder builder;
  const auto err = builder.initHashTableOnCpu(&key_handler,
                                              composite_key_info,
                                              join_columns,
                                              join_column_types,
                                              join_bucket_info,
                                              entry_count,
                                              emitted_keys_count,
                                              layout,
                                              join_type_,
                                              getKeyComponentWidth(),
                                              getKeyComponentCount());
  ts2 = std::chrono::steady_clock::now();
  if (err) {
    CIDER_THROW(
        CiderHashJoinException,
        fmt::format("Unrecognized error when initializing CPU range join hash table ({})",
                    err));
  }
  std::shared_ptr<BaselineHashTable> hash_table = builder.getHashTable();
  auto hashtable_build_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(ts2 - ts1).count();
  putHashTableOnCpuToCache(hashtable_cache_key_,
                           CacheItemType::OVERLAPS_HT,
                           hash_table,
                           DataRecyclerUtil::CPU_DEVICE_IDENTIFIER,
                           hashtable_build_time);
  return hash_table;
}

std::pair<size_t, size_t> RangeJoinHashTable::computeRangeHashTableCounts(
    const size_t shard_count,
    std::vector<ColumnsForDevice>& columns_per_device) {
  CHECK(!inverse_bucket_sizes_for_dimension_.empty());
  const auto [tuple_count, emitted_keys_count] =
      approximateTupleCount(inverse_bucket_sizes_for_dimension_,
                            columns_per_device,
                            max_hashtable_size_,
                            bucket_threshold_);
  const auto entry_count = 2 * std::max(tuple_count, size_t(1));

  return std::make_pair(
      get_entries_per_device(entry_count, shard_count, device_count_, memory_level_),
      emitted_keys_count);
}

std::pair<size_t, size_t> RangeJoinHashTable::approximateTupleCount(
    const std::vector<double>& inverse_bucket_sizes_for_dimension,
    std::vector<ColumnsForDevice>& columns_per_device,
    const size_t chosen_max_hashtable_size,
    const double chosen_bucket_threshold) {
  const auto effective_memory_level = getEffectiveMemoryLevel(inner_outer_pairs_);
  CountDistinctDescriptor count_distinct_desc{
      .impl_type_ = CountDistinctImplType::Bitmap,
      .min_val = 0,
      .bitmap_sz_bits = 11,
      .approximate = true,
      .sub_bitmap_count = 1,
  };
  const auto padded_size_bytes = count_distinct_desc.bitmapPaddedSizeBytes();

  CHECK(!columns_per_device.empty() && !columns_per_device.front().join_columns.empty());
  if (columns_per_device.front().join_columns.front().num_elems == 0) {
    return std::make_pair(0, 0);
  }

  for (auto& columns_for_device : columns_per_device) {
    columns_for_device.setBucketInfo(inverse_bucket_sizes_for_dimension,
                                     inner_outer_pairs_);
  }

  // Number of keys must match dimension of buckets
  CHECK_EQ(columns_per_device.front().join_columns.size(),
           columns_per_device.front().join_buckets.size());
  if (effective_memory_level == Data_Namespace::MemoryLevel::CPU_LEVEL) {
    const auto composite_key_info =
        HashJoin::getCompositeKeyInfo(inner_outer_pairs_, executor_);

    const auto cached_count_info =
        getApproximateTupleCountFromCache(hashtable_cache_key_,
                                          CacheItemType::OVERLAPS_HT,
                                          DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
    if (cached_count_info.has_value() && cached_count_info.value().first) {
      VLOG(1) << "Using a cached tuple count: " << cached_count_info.value().first
              << ", emitted keys count: " << cached_count_info.value().second;
      return std::make_pair(cached_count_info.value().first,
                            cached_count_info.value().second);
    }
    int thread_count = 1;
    std::vector<uint8_t> hll_buffer_all_cpus(thread_count * padded_size_bytes);
    auto hll_result = &hll_buffer_all_cpus[0];

    std::vector<int32_t> num_keys_for_row;
    num_keys_for_row.resize(columns_per_device.front().join_columns[0].num_elems);

    approximate_distinct_tuples_range(hll_result,
                                      num_keys_for_row,
                                      count_distinct_desc.bitmap_sz_bits,
                                      padded_size_bytes,
                                      columns_per_device.front().join_columns,
                                      columns_per_device.front().join_column_types,
                                      columns_per_device.front().join_buckets,
                                      false,
                                      thread_count);

    for (int i = 1; i < thread_count; ++i) {
      hll_unify(hll_result,
                hll_result + i * padded_size_bytes,
                1 << count_distinct_desc.bitmap_sz_bits);
    }
    return std::make_pair(hll_size(hll_result, count_distinct_desc.bitmap_sz_bits),
                          num_keys_for_row.size() > 0 ? num_keys_for_row.back() : 0);
  }
  UNREACHABLE();
  return {0, 0};
}

#define LL_CONTEXT executor_->cgen_state_->context_
#define LL_BUILDER executor_->cgen_state_->ir_builder_
#define LL_INT(v) executor_->cgen_state_->llInt(v)
#define LL_FP(v) executor_->cgen_state_->llFp(v)
#define ROW_FUNC executor_->cgen_state_->row_func_

llvm::Value* RangeJoinHashTable::codegenKey(const CompilationOptions& co,
                                            llvm::Value* offset_ptr) {
  LOG(ERROR) << "Range join key currently only supported for geospatial types.";
  llvm::Value* key_buff_lv{nullptr};
  return key_buff_lv;
}

HashJoinMatchingSet RangeJoinHashTable::codegenMatchingSetWithOffset(
    const CompilationOptions& co,
    const size_t index,
    llvm::Value* range_offset) {
  const auto key_component_width = getKeyComponentWidth();
  CHECK(key_component_width == 4 || key_component_width == 8);

  auto key_buff_lv = codegenKey(co, range_offset);
  CHECK(getHashType() == HashType::OneToMany);

  auto hash_ptr = codegenHashTableLoad(index, executor_);
  const auto composite_dict_ptr_type =
      llvm::Type::getIntNPtrTy(LL_CONTEXT, key_component_width * 8);

  const auto composite_key_dict =
      hash_ptr->getType()->isPointerTy()
          ? LL_BUILDER.CreatePointerCast(hash_ptr, composite_dict_ptr_type)
          : LL_BUILDER.CreateIntToPtr(hash_ptr, composite_dict_ptr_type);

  const auto key_component_count = getKeyComponentCount();

  const auto funcName =
      "get_composite_key_index_" + std::to_string(key_component_width * 8);

  const auto key = executor_->cgen_state_->emitExternalCall(funcName,
                                                            get_int_type(64, LL_CONTEXT),
                                                            {key_buff_lv,
                                                             LL_INT(key_component_count),
                                                             composite_key_dict,
                                                             LL_INT(getEntryCount())});

  auto one_to_many_ptr = hash_ptr;
  if (one_to_many_ptr->getType()->isPointerTy()) {
    one_to_many_ptr =
        LL_BUILDER.CreatePtrToInt(hash_ptr, llvm::Type::getInt64Ty(LL_CONTEXT));
  } else {
    CHECK(one_to_many_ptr->getType()->isIntegerTy(64));
  }
  const auto composite_key_dict_size = offsetBufferOff();
  one_to_many_ptr =
      LL_BUILDER.CreateAdd(one_to_many_ptr, LL_INT(composite_key_dict_size));

  return HashJoin::codegenMatchingSet(
      /* hash_join_idx_args_in */ {one_to_many_ptr,
                                   key,
                                   LL_INT(int64_t(0)),
                                   LL_INT(getEntryCount() - 1)},
      /* is_nullable           */ false,
      /* is_bw_eq              */ false,
      /* sub_buff_size         */ getComponentBufferSize(),
      /* executor              */ executor_);
}
