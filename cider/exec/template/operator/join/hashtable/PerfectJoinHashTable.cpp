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

#include "exec/template/operator/join/hashtable/PerfectJoinHashTable.h"

#include <atomic>
#include <future>
#include <numeric>
#include <thread>

#include "cider/CiderException.h"
#include "exec/template/CodeGenerator.h"
#include "exec/template/Execute.h"
#include "exec/template/ExpressionRewrite.h"
#include "exec/template/operator/join/hashtable/builders/PerfectHashTableBuilder.h"
#include "exec/template/operator/join/hashtable/runtime/HashJoinRuntime.h"
#include "function/scalar/RuntimeFunctions.h"
#include "util/Logger.h"

// let's only consider CPU hahstable recycler at this moment
std::unique_ptr<HashtableRecycler> PerfectJoinHashTable::hash_table_cache_ =
    std::make_unique<HashtableRecycler>(CacheItemType::PERFECT_HT);
std::unique_ptr<HashingSchemeRecycler> PerfectJoinHashTable::hash_table_layout_cache_ =
    std::make_unique<HashingSchemeRecycler>();

namespace {

InnerOuter get_cols(const Analyzer::BinOper* qual_bin_oper,
                    SchemaProviderPtr schema_provider,
                    const TemporaryTables* temporary_tables) {
  const auto lhs = qual_bin_oper->get_left_operand();
  const auto rhs = qual_bin_oper->get_right_operand();
  return HashJoin::normalizeColumnPair(lhs, rhs, schema_provider, temporary_tables);
}

HashEntryInfo get_bucketized_hash_entry_info(SQLTypeInfo const& context_ti,
                                             ExpressionRange const& col_range,
                                             bool const is_bw_eq) {
  using EmptyRangeSize = boost::optional<size_t>;
  auto empty_range_check = [](ExpressionRange const& col_range,
                              bool const is_bw_eq) -> EmptyRangeSize {
    if (col_range.getIntMin() > col_range.getIntMax()) {
      CHECK_EQ(col_range.getIntMin(), int64_t(0));
      CHECK_EQ(col_range.getIntMax(), int64_t(-1));
      if (is_bw_eq) {
        return size_t(1);
      }
      return size_t(0);
    }
    return EmptyRangeSize{};
  };

  auto empty_range = empty_range_check(col_range, is_bw_eq);
  if (empty_range) {
    return {size_t(*empty_range), 1};
  }

  // size_t is not big enough for maximum possible range.
  if (col_range.getIntMin() == std::numeric_limits<int64_t>::min() &&
      col_range.getIntMax() == std::numeric_limits<int64_t>::max()) {
    CIDER_THROW(CiderTooManyHashEntriesException,
                "Hash tables with more than 2B entries not supported yet");
  }

  int64_t bucket_normalization =
      context_ti.get_type() == kDATE ? col_range.getBucket() : 1;
  CHECK_GT(bucket_normalization, 0);
  return {size_t(col_range.getIntMax() - col_range.getIntMin() + 1 + (is_bw_eq ? 1 : 0)),
          bucket_normalization};
}

}  // namespace

//! Make hash table from an in-flight SQL query's parse tree etc.
std::shared_ptr<PerfectJoinHashTable> PerfectJoinHashTable::getInstance(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const JoinType join_type,
    const HashType preferred_hash_type,
    const int device_count,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    Executor* executor,
    const HashTableBuildDagMap& hashtable_build_dag_map,
    const TableIdToNodeMap& table_id_to_node_map) {
  CHECK(IS_EQUIVALENCE(qual_bin_oper->get_optype()));
  const auto cols = get_cols(
      qual_bin_oper.get(), executor->getSchemaProvider(), executor->temporary_tables_);
  const auto inner_col = cols.first;
  CHECK(inner_col);
  const auto& ti = inner_col->get_type_info();
  auto col_range =
      getExpressionRange(ti.is_string() ? cols.second : inner_col, query_infos, executor);
  if (col_range.getType() == ExpressionRangeType::Invalid) {
    CIDER_THROW(CiderHashJoinException,
                "Could not compute range for the expressions involved in the equijoin");
  }
  if (ti.is_string()) {
    // The nullable info must be the same as the source column.
    const auto source_col_range = getExpressionRange(inner_col, query_infos, executor);
    if (source_col_range.getType() == ExpressionRangeType::Invalid) {
      CIDER_THROW(CiderHashJoinException,
                  "Could not compute range for the expressions involved in the equijoin");
    }
    if (source_col_range.getIntMin() > source_col_range.getIntMax()) {
      // If the inner column expression range is empty, use the inner col range
      CHECK_EQ(source_col_range.getIntMin(), int64_t(0));
      CHECK_EQ(source_col_range.getIntMax(), int64_t(-1));
      col_range = source_col_range;
    } else {
      col_range = ExpressionRange::makeIntRange(
          std::min(source_col_range.getIntMin(), col_range.getIntMin()),
          std::max(source_col_range.getIntMax(), col_range.getIntMax()),
          0,
          source_col_range.hasNulls());
    }
  }
  const auto max_hash_entry_count =
      static_cast<size_t>(std::numeric_limits<int32_t>::max());

  auto bucketized_entry_count_info = get_bucketized_hash_entry_info(
      ti, col_range, qual_bin_oper->get_optype() == kBW_EQ);
  auto bucketized_entry_count = bucketized_entry_count_info.getNormalizedHashEntryCount();

  if (bucketized_entry_count > max_hash_entry_count) {
    CIDER_THROW(CiderTooManyHashEntriesException,
                "Hash tables with more than 2B entries not supported yet");
  }

  // We don't want to build huge and very sparse tables
  // to consume lots of memory.
  if (bucketized_entry_count > 1000000) {
    const auto& query_info =
        get_inner_query_info(inner_col->get_table_id(), query_infos).info;
    if (query_info.getNumTuplesUpperBound() * 100 <
        huge_join_hash_min_load_ * bucketized_entry_count) {
      CIDER_THROW(CiderTooManyHashEntriesException,
                  "Hash tables with more than 2B entries not supported yet");
    }
  }

  if (qual_bin_oper->get_optype() == kBW_EQ &&
      col_range.getIntMax() >= std::numeric_limits<int64_t>::max()) {
    CIDER_THROW(CiderHashJoinException, "Cannot translate null value for kBW_EQ");
  }
  std::vector<InnerOuter> inner_outer_pairs;
  inner_outer_pairs.emplace_back(inner_col, cols.second);
  auto hashtable_cache_key =
      HashtableRecycler::getHashtableCacheKey(inner_outer_pairs,
                                              qual_bin_oper->get_optype(),
                                              join_type,
                                              hashtable_build_dag_map,
                                              executor);
  auto hash_key = hashtable_cache_key.first;
  decltype(std::chrono::steady_clock::now()) ts1, ts2;
  if (VLOGGING(1)) {
    ts1 = std::chrono::steady_clock::now();
  }
  auto join_hash_table = std::shared_ptr<PerfectJoinHashTable>(
      new PerfectJoinHashTable(qual_bin_oper,
                               inner_col,
                               query_infos,
                               memory_level,
                               join_type,
                               preferred_hash_type,
                               col_range,
                               data_provider,
                               column_cache,
                               executor,
                               device_count,
                               hash_key,
                               hashtable_cache_key.second,
                               table_id_to_node_map));
  try {
    join_hash_table->reify();
  } catch (const CiderHashJoinException& e) {
    join_hash_table->freeHashBufferMemory();
    throw;
  }
  if (VLOGGING(1)) {
    ts2 = std::chrono::steady_clock::now();
    VLOG(1) << "Built perfect hash table "
            << getHashTypeString(join_hash_table->getHashType()) << " in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(ts2 - ts1).count()
            << " ms";
  }
  return join_hash_table;
}

bool needs_dictionary_translation(const Analyzer::ColumnVar* inner_col,
                                  const Analyzer::Expr* outer_col_expr,
                                  const Executor* executor) {
  auto schema_provider = executor->getSchemaProvider();
  CHECK(schema_provider);
  const auto inner_col_info =
      schema_provider->getColumnInfo(*inner_col->get_column_info());
  const auto& inner_ti = get_column_type(inner_col->get_column_id(),
                                         inner_col->get_table_id(),
                                         inner_col_info,
                                         executor->getTemporaryTables());
  // Only strings may need dictionary translation.
  if (!inner_ti.is_string()) {
    return false;
  }
  const auto outer_col = dynamic_cast<const Analyzer::ColumnVar*>(outer_col_expr);
  CHECK(outer_col);
  const auto outer_col_info =
      schema_provider->getColumnInfo(*outer_col->get_column_info());
  // Don't want to deal with temporary tables for now, require translation.
  if (!inner_col_info || !outer_col_info) {
    return true;
  }
  const auto& outer_ti = get_column_type(outer_col->get_column_id(),
                                         outer_col->get_table_id(),
                                         outer_col_info,
                                         executor->getTemporaryTables());
  CHECK_EQ(inner_ti.is_string(), outer_ti.is_string());
  // If the two columns don't share the dictionary, translation is needed.
  if (outer_ti.get_comp_param() != inner_ti.get_comp_param()) {
    return true;
  }
  const auto inner_str_dict_proxy =
      executor->getStringDictionaryProxy(inner_col->get_comp_param(), true);
  CHECK(inner_str_dict_proxy);
  const auto outer_str_dict_proxy =
      executor->getStringDictionaryProxy(inner_col->get_comp_param(), true);
  CHECK(outer_str_dict_proxy);

  return *inner_str_dict_proxy != *outer_str_dict_proxy;
}

void PerfectJoinHashTable::reify() {
  auto timer = DEBUG_TIMER(__func__);
  CHECK_LT(0, device_count_);
  const auto cols = get_cols(
      qual_bin_oper_.get(), executor_->getSchemaProvider(), executor_->temporary_tables_);
  const auto inner_col = cols.first;
  const auto& query_info = getInnerQueryInfo(inner_col).info;
  if (query_info.fragments.empty()) {
    return;
  }
  if (query_info.getNumTuplesUpperBound() >
      static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    CIDER_THROW(CiderTooManyHashEntriesException,
                "Hash tables with more than 2B entries not supported yet");
  }
  std::vector<std::future<void>> init_threads;

  inner_outer_pairs_.push_back(cols);
  CHECK_EQ(inner_outer_pairs_.size(), size_t(1));

  std::vector<ColumnsForDevice> columns_per_device;
  try {
    for (int device_id = 0; device_id < device_count_; ++device_id) {
      const auto fragments = query_info.fragments;
      const auto columns_for_device = fetchColumnsForDevice(fragments, device_id);
      columns_per_device.push_back(columns_for_device);
      const auto chunk_key = genChunkKey(
          fragments, inner_outer_pairs_.front().second, inner_outer_pairs_.front().first);
      if (device_id == 0 && hashtable_cache_key_ == EMPTY_HASHED_PLAN_DAG_KEY &&
          getInnerTableId() > 0) {
        // sometimes we cannot retrieve query plan dag, so try to recycler cache
        // with the old-passioned cache key if we deal with hashtable of non-temporary
        // table
        auto outer_col =
            dynamic_cast<const Analyzer::ColumnVar*>(inner_outer_pairs_.front().second);
        AlternativeCacheKeyForPerfectHashJoin cache_key{
            col_range_,
            inner_col,
            outer_col ? outer_col : inner_col,
            chunk_key,
            columns_per_device[device_id].join_columns.front().num_elems,
            qual_bin_oper_->get_optype(),
            join_type_};
        hashtable_cache_key_ = getAlternativeCacheKey(cache_key);
        VLOG(2) << "Use alternative hashtable cache key due to unavailable query plan "
                   "dag extraction";
      }
      init_threads.push_back(std::async(std::launch::async,
                                        &PerfectJoinHashTable::reifyForDevice,
                                        this,
                                        chunk_key,
                                        columns_per_device[device_id],
                                        hash_type_,
                                        device_id,
                                        logger::thread_id()));
    }
    for (auto& init_thread : init_threads) {
      init_thread.wait();
    }
    for (auto& init_thread : init_threads) {
      init_thread.get();
    }
  } catch (const CiderOneToMoreHashException& e) {
    hash_type_ = HashType::OneToMany;
    freeHashBufferMemory();
    init_threads.clear();
    CHECK_EQ(columns_per_device.size(), size_t(device_count_));
    for (int device_id = 0; device_id < device_count_; ++device_id) {
      const auto fragments = query_info.fragments;
      const auto chunk_key = genChunkKey(
          fragments, inner_outer_pairs_.front().second, inner_outer_pairs_.front().first);
      init_threads.push_back(std::async(std::launch::async,
                                        &PerfectJoinHashTable::reifyForDevice,
                                        this,
                                        chunk_key,
                                        columns_per_device[device_id],
                                        hash_type_,
                                        device_id,
                                        logger::thread_id()));
    }
    for (auto& init_thread : init_threads) {
      init_thread.wait();
    }
    for (auto& init_thread : init_threads) {
      init_thread.get();
    }
  }
}

Data_Namespace::MemoryLevel PerfectJoinHashTable::getEffectiveMemoryLevel(
    const std::vector<InnerOuter>& inner_outer_pairs) const {
  for (const auto& inner_outer_pair : inner_outer_pairs) {
    if (needs_dictionary_translation(
            inner_outer_pair.first, inner_outer_pair.second, executor_)) {
      needs_dict_translation_ = true;
      return Data_Namespace::CPU_LEVEL;
    }
  }
  return memory_level_;
}

ColumnsForDevice PerfectJoinHashTable::fetchColumnsForDevice(
    const std::vector<Fragmenter_Namespace::FragmentInfo>& fragments,
    const int device_id) {
  const auto effective_memory_level = getEffectiveMemoryLevel(inner_outer_pairs_);
  std::vector<JoinColumn> join_columns;
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner;
  std::vector<JoinColumnTypeInfo> join_column_types;
  std::vector<JoinBucketInfo> join_bucket_info;
  std::vector<std::shared_ptr<void>> malloc_owner;
  for (const auto& inner_outer_pair : inner_outer_pairs_) {
    const auto inner_col = inner_outer_pair.first;
    if (inner_col->is_virtual()) {
      CIDER_THROW(CiderHashJoinException, "Cannot join on rowid");
    }
    join_columns.emplace_back(fetchJoinColumn(inner_col,
                                              fragments,
                                              effective_memory_level,
                                              device_id,
                                              chunks_owner,
                                              malloc_owner,
                                              executor_,
                                              &column_cache_));
    const auto& ti = inner_col->get_type_info();
    join_column_types.emplace_back(JoinColumnTypeInfo{static_cast<size_t>(ti.get_size()),
                                                      0,
                                                      0,
                                                      inline_fixed_encoding_null_val(ti),
                                                      isBitwiseEq(),
                                                      0,
                                                      get_join_column_type_kind(ti)});
  }
  return {join_columns, join_column_types, chunks_owner, join_bucket_info, malloc_owner};
}

void PerfectJoinHashTable::reifyForDevice(const ChunkKey& chunk_key,
                                          const ColumnsForDevice& columns_for_device,
                                          const HashType layout,
                                          const int device_id,
                                          const logger::ThreadId parent_thread_id) {
  DEBUG_TIMER_NEW_THREAD(parent_thread_id);
  const auto effective_memory_level = getEffectiveMemoryLevel(inner_outer_pairs_);

  CHECK_EQ(columns_for_device.join_columns.size(), size_t(1));
  CHECK_EQ(inner_outer_pairs_.size(), size_t(1));
  auto& join_column = columns_for_device.join_columns.front();
  if (layout == HashType::OneToOne) {
    const auto err = initHashTableForDevice(chunk_key,
                                            join_column,
                                            inner_outer_pairs_.front(),
                                            layout,
                                            effective_memory_level,
                                            device_id);
    if (err) {
      CIDER_THROW(CiderOneToMoreHashException, "Needs one to many hash");
    }
  } else {
    const auto err = initHashTableForDevice(chunk_key,
                                            join_column,
                                            inner_outer_pairs_.front(),
                                            HashType::OneToMany,
                                            effective_memory_level,
                                            device_id);
    if (err) {
      CIDER_THROW(
          CiderCompileException,
          fmt::format("Unexpected error building one to many hash table: {}", err));
    }
  }
}

int PerfectJoinHashTable::initHashTableForDevice(
    const ChunkKey& chunk_key,
    const JoinColumn& join_column,
    const InnerOuter& cols,
    const HashType layout,
    const Data_Namespace::MemoryLevel effective_memory_level,
    const int device_id) {
  auto timer = DEBUG_TIMER(__func__);
  const auto inner_col = cols.first;
  CHECK(inner_col);

  auto hash_entry_info = get_bucketized_hash_entry_info(
      inner_col->get_type_info(), col_range_, isBitwiseEq());
  if (!hash_entry_info && layout == HashType::OneToOne) {
    // TODO: what is this for?
    return 0;
  }
  int err{0};
  const int32_t hash_join_invalid_val{-1};
  auto hashtable_layout = layout;
  auto allow_hashtable_recycling = HashtableRecycler::isSafeToCacheHashtable(
      table_id_to_node_map_, needs_dict_translation_, inner_col->get_table_id());
  if (allow_hashtable_recycling) {
    auto cached_hashtable_layout_type = hash_table_layout_cache_->getItemFromCache(
        hashtable_cache_key_,
        CacheItemType::HT_HASHING_SCHEME,
        DataRecyclerUtil::CPU_DEVICE_IDENTIFIER,
        {});
    if (cached_hashtable_layout_type) {
      hash_type_ = *cached_hashtable_layout_type;
      hashtable_layout = hash_type_;
      VLOG(1) << "Recycle hashtable layout: " << getHashTypeString(hashtable_layout);
    }
  }
  if (effective_memory_level == Data_Namespace::CPU_LEVEL) {
    CHECK(!chunk_key.empty());
    std::shared_ptr<PerfectHashTable> hash_table{nullptr};
    if (allow_hashtable_recycling) {
      hash_table = initHashTableOnCpuFromCache(hashtable_cache_key_,
                                               CacheItemType::PERFECT_HT,
                                               DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
    }
    decltype(std::chrono::steady_clock::now()) ts1, ts2;
    ts1 = std::chrono::steady_clock::now();
    {
      std::lock_guard<std::mutex> cpu_hash_table_buff_lock(cpu_hash_table_buff_mutex_);
      if (!hash_table) {
        PerfectJoinHashTableBuilder builder;
        if (hashtable_layout == HashType::OneToOne) {
          builder.initOneToOneHashTableOnCpu(join_column,
                                             col_range_,
                                             isBitwiseEq(),
                                             cols,
                                             join_type_,
                                             hashtable_layout,
                                             hash_entry_info,
                                             hash_join_invalid_val,
                                             executor_);
          hash_table = builder.getHashTable();
        } else {
          builder.initOneToManyHashTableOnCpu(join_column,
                                              col_range_,
                                              isBitwiseEq(),
                                              cols,
                                              hash_entry_info,
                                              hash_join_invalid_val,
                                              executor_);
          hash_table = builder.getHashTable();
        }
        ts2 = std::chrono::steady_clock::now();
        auto build_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(ts2 - ts1).count();
        if (allow_hashtable_recycling) {
          hash_table_layout_cache_->putItemToCache(
              hashtable_cache_key_,
              hashtable_layout,
              CacheItemType::HT_HASHING_SCHEME,
              DataRecyclerUtil::CPU_DEVICE_IDENTIFIER,
              0,
              0,
              {});
        }
      }
    }
    CHECK(hash_table);
    CHECK_LT(size_t(device_id), hash_tables_for_device_.size());
    hash_tables_for_device_[device_id] = hash_table;
  } else {
    UNREACHABLE();
  }
  return err;
}

ChunkKey PerfectJoinHashTable::genChunkKey(
    const std::vector<Fragmenter_Namespace::FragmentInfo>& fragments,
    const Analyzer::Expr* outer_col_expr,
    const Analyzer::ColumnVar* inner_col) const {
  ChunkKey chunk_key{
      executor_->getDatabaseId(), inner_col->get_table_id(), inner_col->get_column_id()};
  const auto& ti = inner_col->get_type_info();
  if (ti.is_string()) {
    CHECK_EQ(kENCODING_DICT, ti.get_compression());
    size_t outer_elem_count = 0;
    const auto outer_col = dynamic_cast<const Analyzer::ColumnVar*>(outer_col_expr);
    CHECK(outer_col);
    const auto& outer_query_info = getInnerQueryInfo(outer_col).info;
    for (auto& frag : outer_query_info.fragments) {
      outer_elem_count = frag.getNumTuples();
    }
    chunk_key.push_back(outer_elem_count);
  }
  if (fragments.size() < 2) {
    chunk_key.push_back(fragments.front().fragmentId);
  }
  return chunk_key;
}

std::shared_ptr<PerfectHashTable> PerfectJoinHashTable::initHashTableOnCpuFromCache(
    QueryPlanHash key,
    CacheItemType item_type,
    DeviceIdentifier device_identifier) {
  CHECK(hash_table_cache_);
  auto timer = DEBUG_TIMER(__func__);
  auto hashtable_ptr =
      hash_table_cache_->getItemFromCache(key, item_type, device_identifier);
  if (hashtable_ptr) {
    return std::dynamic_pointer_cast<PerfectHashTable>(hashtable_ptr);
  }
  return nullptr;
}

llvm::Value* PerfectJoinHashTable::codegenHashTableLoad(const size_t table_idx) {
  AUTOMATIC_IR_METADATA(executor_->cgen_state_.get());
  const auto hash_ptr = HashJoin::codegenHashTableLoad(table_idx, executor_);
  if (hash_ptr->getType()->isIntegerTy(64)) {
    return hash_ptr;
  }
  CHECK(hash_ptr->getType()->isPointerTy());
  return executor_->cgen_state_->ir_builder_.CreatePtrToInt(
      get_arg_by_name(executor_->cgen_state_->row_func_, "join_hash_tables"),
      llvm::Type::getInt64Ty(executor_->cgen_state_->context_));
}

std::vector<llvm::Value*> PerfectJoinHashTable::getHashJoinArgs(
    llvm::Value* hash_ptr,
    const Analyzer::Expr* key_col,
    const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(executor_->cgen_state_.get());
  CodeGenerator code_generator(executor_);
  std::vector<llvm::Value*> key_lvs_tmp;
  llvm::Value* null_value = nullptr;
  if(co.use_cider_data_format) {
    auto key_lvs_cider = code_generator.codegen(key_col, co, true);
    auto key_lvs_cider_fixed_size = dynamic_cast<FixedSizeColValues*>(key_lvs_cider.get());
    key_lvs_tmp.push_back(key_lvs_cider_fixed_size->getValue());
    null_value = key_lvs_cider_fixed_size->getNull();
  }
  else {
    key_lvs_tmp = code_generator.codegen(key_col, true, co);
  }
  const auto key_lvs = key_lvs_tmp;
  CHECK_EQ(size_t(1), key_lvs.size());
  auto const& key_col_ti = key_col->get_type_info();
  auto hash_entry_info =
      get_bucketized_hash_entry_info(key_col_ti, col_range_, isBitwiseEq());

  std::vector<llvm::Value*> hash_join_idx_args{
      hash_ptr,
      executor_->cgen_state_->castToTypeIn(key_lvs.front(), 64),
      executor_->cgen_state_->llInt(col_range_.getIntMin()),
      executor_->cgen_state_->llInt(col_range_.getIntMax())};
  auto key_col_logical_ti = get_logical_type_info(key_col->get_type_info());
  if (!key_col_logical_ti.get_notnull() || isBitwiseEq()) {
    if(co.use_cider_data_format) {
        hash_join_idx_args.push_back(null_value ? null_value : llvm::ConstantInt::getFalse(executor_->cgen_state_->context_));
      }
    else{
    hash_join_idx_args.push_back(executor_->cgen_state_->llInt(
        inline_fixed_encoding_null_val(key_col_logical_ti)));
      }
  }
  auto special_date_bucketization_case = key_col_ti.get_type() == kDATE;
  if (isBitwiseEq()) {
    if (special_date_bucketization_case) {
      hash_join_idx_args.push_back(executor_->cgen_state_->llInt(
          col_range_.getIntMax() / hash_entry_info.bucket_normalization + 1));
    } else {
      hash_join_idx_args.push_back(
          executor_->cgen_state_->llInt(col_range_.getIntMax() + 1));
    }
  }

  if (special_date_bucketization_case) {
    hash_join_idx_args.emplace_back(
        executor_->cgen_state_->llInt(hash_entry_info.bucket_normalization));
  }

  return hash_join_idx_args;
}

HashJoinMatchingSet PerfectJoinHashTable::codegenMatchingSet(const CompilationOptions& co,
                                                             const size_t index) {
  AUTOMATIC_IR_METADATA(executor_->cgen_state_.get());
  const auto cols = get_cols(
      qual_bin_oper_.get(), executor_->getSchemaProvider(), executor_->temporary_tables_);
  auto key_col = cols.second;
  CHECK(key_col);
  auto val_col = cols.first;
  CHECK(val_col);
  auto pos_ptr = codegenHashTableLoad(index);
  CHECK(pos_ptr);
  const auto key_col_var = dynamic_cast<const Analyzer::ColumnVar*>(key_col);
  const auto val_col_var = dynamic_cast<const Analyzer::ColumnVar*>(val_col);
  if (key_col_var && val_col_var &&
      self_join_not_covered_by_left_deep_tree(
          key_col_var,
          val_col_var,
          get_max_rte_scan_table(executor_->cgen_state_->scan_idx_to_hash_pos_))) {
    CIDER_THROW(
        CiderCompileException,
        "Query execution fails because the query contains not supported self-join "
        "pattern. We suspect the query requires multiple left-deep join tree due to "
        "the "
        "join condition of the self-join and is not supported for now. Please consider "
        "rewriting table order in "
        "FROM clause.");
  }
  auto hash_join_idx_args = getHashJoinArgs(pos_ptr, key_col, co);
  const int64_t sub_buff_size = getComponentBufferSize();
  const auto& key_col_ti = key_col->get_type_info();

  auto bucketize = (key_col_ti.get_type() == kDATE);
  return HashJoin::codegenMatchingSet(hash_join_idx_args,
                                      !key_col_ti.get_notnull(),
                                      isBitwiseEq(),
                                      sub_buff_size,
                                      executor_,
                                      bucketize,
                                      co.use_cider_data_format);
}

size_t PerfectJoinHashTable::offsetBufferOff() const noexcept {
  return 0;
}

size_t PerfectJoinHashTable::countBufferOff() const noexcept {
  return getComponentBufferSize();
}

size_t PerfectJoinHashTable::payloadBufferOff() const noexcept {
  return 2 * getComponentBufferSize();
}

size_t PerfectJoinHashTable::getComponentBufferSize() const noexcept {
  if (hash_tables_for_device_.empty()) {
    return 0;
  }
  auto hash_table = hash_tables_for_device_.front();
  if (hash_table && hash_table->getLayout() == HashType::OneToMany) {
    return hash_table->getEntryCount() * sizeof(int32_t);
  } else {
    return 0;
  }
}

HashTable* PerfectJoinHashTable::getHashTableForDevice(const size_t device_id) const {
  CHECK_LT(device_id, hash_tables_for_device_.size());
  return hash_tables_for_device_[device_id].get();
}

std::string PerfectJoinHashTable::toString(const int device_id, bool raw) const {
  auto buffer = getJoinHashBuffer(device_id);
  auto buffer_size = getJoinHashBufferSize(device_id);
  auto hash_table = getHashTableForDevice(device_id);
  auto ptr1 = reinterpret_cast<const int8_t*>(buffer);
  auto ptr2 = ptr1 + offsetBufferOff();
  auto ptr3 = ptr1 + countBufferOff();
  auto ptr4 = ptr1 + payloadBufferOff();
  return HashTable::toString("perfect",
                             getHashTypeString(hash_type_),
                             0,
                             0,
                             hash_table ? hash_table->getEntryCount() : 0,
                             ptr1,
                             ptr2,
                             ptr3,
                             ptr4,
                             buffer_size,
                             raw);
}

std::set<DecodedJoinHashBufferEntry> PerfectJoinHashTable::toSet(
    const int device_id) const {
  auto buffer = getJoinHashBuffer(device_id);
  auto buffer_size = getJoinHashBufferSize(device_id);
  auto hash_table = getHashTableForDevice(device_id);
  auto ptr1 = reinterpret_cast<const int8_t*>(buffer);
  auto ptr2 = ptr1 + offsetBufferOff();
  auto ptr3 = ptr1 + countBufferOff();
  auto ptr4 = ptr1 + payloadBufferOff();
  return HashTable::toSet(0,
                          0,
                          hash_table ? hash_table->getEntryCount() : 0,
                          ptr1,
                          ptr2,
                          ptr3,
                          ptr4,
                          buffer_size);
}

llvm::Value* PerfectJoinHashTable::codegenSlot(const CompilationOptions& co,
                                               const size_t index) {
  AUTOMATIC_IR_METADATA(executor_->cgen_state_.get());
  using namespace std::string_literals;

  CHECK(getHashType() == HashType::OneToOne);
  const auto cols = get_cols(
      qual_bin_oper_.get(), executor_->getSchemaProvider(), executor_->temporary_tables_);
  auto key_col = cols.second;
  CHECK(key_col);
  auto val_col = cols.first;
  CHECK(val_col);
  CodeGenerator code_generator(executor_);
  const auto key_col_var = dynamic_cast<const Analyzer::ColumnVar*>(key_col);
  const auto val_col_var = dynamic_cast<const Analyzer::ColumnVar*>(val_col);
  if (key_col_var && val_col_var &&
      self_join_not_covered_by_left_deep_tree(
          key_col_var,
          val_col_var,
          get_max_rte_scan_table(executor_->cgen_state_->scan_idx_to_hash_pos_))) {
    CIDER_THROW(
        CiderCompileException,
        "Query execution fails because the query contains not supported self-join "
        "pattern. We suspect the query requires multiple left-deep join tree due to "
        "the "
        "join condition of the self-join and is not supported for now. Please consider "
        "rewriting table order in "
        "FROM clause.");
  }
  std::vector<llvm::Value*> key_lvs_tmp;
  if(co.use_cider_data_format) {
    auto key_lvs_col_values = code_generator.codegen(key_col, co, true);
    auto key_lvs_col_values_fixed_size = dynamic_cast<FixedSizeColValues*>(key_lvs_col_values.get());
    key_lvs_tmp.push_back(key_lvs_col_values_fixed_size->getValue());
  }
  else {
    key_lvs_tmp = code_generator.codegen(key_col, true, co);
  }
  const auto key_lvs = key_lvs_tmp;
  // const auto key_lvs = code_generator.codegen(key_col, true, co);
  CHECK_EQ(size_t(1), key_lvs.size());
  auto hash_ptr = codegenHashTableLoad(index);
  CHECK(hash_ptr);
  const auto hash_join_idx_args = getHashJoinArgs(hash_ptr, key_col, co);

  const auto& key_col_ti = key_col->get_type_info();
  std::string fname((key_col_ti.get_type() == kDATE) ? "bucketized_hash_join_idx"s
                                                     : "hash_join_idx"s);

  if (isBitwiseEq()) {
    fname += "_bitwise";
  }

  if (!isBitwiseEq() && !key_col_ti.get_notnull()) {
    if(co.use_cider_data_format) {
      fname += "_nullable_cider";
    } else {
      fname += "_nullable";
    }
  }
  return executor_->cgen_state_->emitCall(fname, hash_join_idx_args);
}

const InputTableInfo& PerfectJoinHashTable::getInnerQueryInfo(
    const Analyzer::ColumnVar* inner_col) const {
  return get_inner_query_info(inner_col->get_table_id(), query_infos_);
}

const InputTableInfo& get_inner_query_info(
    const int inner_table_id,
    const std::vector<InputTableInfo>& query_infos) {
  std::optional<size_t> ti_idx;
  for (size_t i = 0; i < query_infos.size(); ++i) {
    if (inner_table_id == query_infos[i].table_id) {
      ti_idx = i;
      break;
    }
  }
  CHECK(ti_idx);
  return query_infos[*ti_idx];
}

size_t get_entries_per_device(const size_t total_entries,
                              const size_t shard_count,
                              const size_t device_count,
                              const Data_Namespace::MemoryLevel memory_level) {
  const auto entries_per_shard =
      shard_count ? (total_entries + shard_count - 1) / shard_count : total_entries;
  size_t entries_per_device = entries_per_shard;
  return entries_per_device;
}

size_t PerfectJoinHashTable::shardCount() const {
  return 0;
}

bool PerfectJoinHashTable::isBitwiseEq() const {
  return qual_bin_oper_->get_optype() == kBW_EQ;
}
