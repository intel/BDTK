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

#include <llvm/IR/Value.h>
#include <cstdint>
#include <set>
#include <string>

#include "exec/template/ColumnarResults.h"
#include "exec/template/CompilationOptions.h"
#include "exec/template/InputMetadata.h"
#include "exec/template/common/descriptors/RowSetMemoryOwner.h"
#include "exec/template/operator/join/hashtable/HashTable.h"
#include "exec/template/operator/join/hashtable/runtime/HashJoinRuntime.h"
#include "type/plan/Analyzer.h"
#include "type/schema/SchemaProvider.h"
#include "util/memory/DataProvider.h"

using InnerOuter = std::pair<const Analyzer::ColumnVar*, const Analyzer::Expr*>;

struct ColumnsForDevice {
  const std::vector<JoinColumn> join_columns;
  const std::vector<JoinColumnTypeInfo> join_column_types;
  const std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner;
  std::vector<JoinBucketInfo> join_buckets;
  const std::vector<std::shared_ptr<void>> malloc_owner;

  void setBucketInfo(const std::vector<double>& bucket_sizes_for_dimension,
                     const std::vector<InnerOuter> inner_outer_pairs);
};

struct HashJoinMatchingSet {
  llvm::Value* elements;
  llvm::Value* count;
  llvm::Value* slot;
};

struct CompositeKeyInfo {
  std::vector<const void*> sd_inner_proxy_per_key;
  std::vector<const void*> sd_outer_proxy_per_key;
  std::vector<ChunkKey> cache_key_chunks;  // used for the cache key
};
class HashJoin {
 public:
  virtual std::string toString(const int device_id = 0, bool raw = false) const = 0;

  virtual std::string toStringFlat64(const int device_id) const;

  virtual std::string toStringFlat32(const int device_id) const;

  virtual DecodedJoinHashBufferSet toSet(const int device_id) const = 0;

  virtual llvm::Value* codegenSlot(const CompilationOptions&, const size_t) = 0;

  virtual HashJoinMatchingSet codegenMatchingSet(const CompilationOptions&,
                                                 const size_t) = 0;

  virtual int getInnerTableId() const noexcept = 0;

  virtual int getInnerTableRteIdx() const noexcept = 0;

  virtual HashType getHashType() const noexcept = 0;

  static bool layoutRequiresAdditionalBuffers(HashType layout) noexcept {
    return (layout == HashType::ManyToMany || layout == HashType::OneToMany);
  }

  static std::string getHashTypeString(HashType ht) noexcept {
    const char* HashTypeStrings[3] = {"OneToOne", "OneToMany", "ManyToMany"};
    return HashTypeStrings[static_cast<int>(ht)];
  };

  static HashJoinMatchingSet codegenMatchingSet(
      const std::vector<llvm::Value*>& hash_join_idx_args_in,
      const bool col_is_nullable,
      const bool is_bw_eq,
      const int64_t sub_buff_size,
      Executor* executor,
      const bool is_bucketized = false,
      const bool is_cider_format = false);

  static llvm::Value* codegenHashTableLoad(const size_t table_idx, Executor* executor);

  virtual Data_Namespace::MemoryLevel getMemoryLevel() const noexcept = 0;

  virtual int getDeviceCount() const noexcept = 0;

  virtual size_t offsetBufferOff() const noexcept = 0;

  virtual size_t countBufferOff() const noexcept = 0;

  virtual size_t payloadBufferOff() const noexcept = 0;

  virtual std::string getHashJoinType() const = 0;

  JoinColumn fetchJoinColumn(
      const Analyzer::ColumnVar* hash_col,
      const std::vector<Fragmenter_Namespace::FragmentInfo>& fragment_info,
      const Data_Namespace::MemoryLevel effective_memory_level,
      const int device_id,
      std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
      std::vector<std::shared_ptr<void>>& malloc_owner,
      Executor* executor,
      ColumnCacheMap* column_cache);

  //! Make hash table from an in-flight SQL query's parse tree etc.
  static std::shared_ptr<HashJoin> getInstance(
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
      const RegisteredQueryHint& query_hint,
      const TableIdToNodeMap& table_id_to_node_map);

  //! Make hash table from named tables and columns (such as for testing).
  static std::shared_ptr<HashJoin> getSyntheticInstance(
      std::string_view table1,
      std::string_view column1,
      std::string_view table2,
      std::string_view column2,
      const Data_Namespace::MemoryLevel memory_level,
      const HashType preferred_hash_type,
      const int device_count,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache,
      Executor* executor);

  //! Make hash table from named tables and columns (such as for testing).
  static std::shared_ptr<HashJoin> getSyntheticInstance(
      const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
      const Data_Namespace::MemoryLevel memory_level,
      const HashType preferred_hash_type,
      const int device_count,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache,
      Executor* executor);

  static std::pair<std::string, std::shared_ptr<HashJoin>> getSyntheticInstance(
      std::vector<std::shared_ptr<Analyzer::BinOper>>,
      const Data_Namespace::MemoryLevel memory_level,
      const HashType preferred_hash_type,
      const int device_count,
      DataProvider* data_provider,
      ColumnCacheMap& column_cache,
      Executor* executor);

  static int getInnerTableId(const std::vector<InnerOuter>& inner_outer_pairs) {
    CHECK(!inner_outer_pairs.empty());
    const auto first_inner_col = inner_outer_pairs.front().first;
    return first_inner_col->get_table_id();
  }

  // Swap the columns if needed and make the inner column the first component.
  static InnerOuter normalizeColumnPair(const Analyzer::Expr* lhs,
                                        const Analyzer::Expr* rhs,
                                        SchemaProviderPtr schema_provider,
                                        const TemporaryTables* temporary_tables);

  // Normalize each expression tuple
  static std::vector<InnerOuter> normalizeColumnPairs(
      const Analyzer::BinOper* condition,
      SchemaProviderPtr schema_provider,
      const TemporaryTables* temporary_tables);

  HashTable* getHashTableForDevice(const size_t device_id) const {
    CHECK_LT(device_id, hash_tables_for_device_.size());
    return hash_tables_for_device_[device_id].get();
  }

  size_t getJoinHashBufferSize() { return getJoinHashBufferSize(0); }

  size_t getJoinHashBufferSize(const int device_id) const {
    auto hash_table = getHashTableForDevice(device_id);
    if (!hash_table) {
      return 0;
    }
    return hash_table->getHashTableBufferSize();
  }

  int64_t getJoinHashBuffer(const int device_id) const {
    // TODO: just make device_id a size_t
    CHECK_LT(size_t(device_id), hash_tables_for_device_.size());
    if (!hash_tables_for_device_[device_id]) {
      return 0;
    }
    CHECK(hash_tables_for_device_[device_id]);
    auto hash_table = hash_tables_for_device_[device_id].get();
    return reinterpret_cast<int64_t>(hash_table->getCpuBuffer());
  }

  void freeHashBufferMemory() {
    auto empty_hash_tables =
        decltype(hash_tables_for_device_)(hash_tables_for_device_.size());
    hash_tables_for_device_.swap(empty_hash_tables);
  }

  static CompositeKeyInfo getCompositeKeyInfo(
      const std::vector<InnerOuter>& inner_outer_pairs,
      const Executor* executor);

 protected:
  HashJoin(DataProvider* data_provider) : data_provider_(data_provider) {}

  virtual size_t getComponentBufferSize() const noexcept = 0;

  std::vector<std::shared_ptr<HashTable>> hash_tables_for_device_;
  DataProvider* data_provider_;
};

std::ostream& operator<<(std::ostream& os, const DecodedJoinHashBufferEntry& e);

std::ostream& operator<<(std::ostream& os, const DecodedJoinHashBufferSet& s);

std::shared_ptr<Analyzer::ColumnVar> getSyntheticColumnVar(std::string_view table,
                                                           std::string_view column,
                                                           int rte_idx,
                                                           Executor* executor);
