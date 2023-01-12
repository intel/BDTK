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

#include "exec/template/operator/join/hashtable/HashJoin.h"

#include "cider/CiderException.h"
#include "exec/template/ColumnFetcher.h"
#include "exec/template/Execute.h"
#include "exec/template/RangeTableIndexVisitor.h"
#include "exec/template/ScalarExprVisitor.h"
#include "exec/template/operator/join/EquiJoinCondition.h"
#include "exec/template/operator/join/hashtable/BaselineJoinHashTable.h"
#include "exec/template/operator/join/hashtable/OverlapsJoinHashTable.h"
#include "exec/template/operator/join/hashtable/PerfectJoinHashTable.h"
#include "function/scalar/RuntimeFunctions.h"

extern bool g_enable_overlaps_hashjoin;

void ColumnsForDevice::setBucketInfo(
    const std::vector<double>& inverse_bucket_sizes_for_dimension,
    const std::vector<InnerOuter> inner_outer_pairs) {
  join_buckets.clear();

  CHECK_EQ(inner_outer_pairs.size(), join_columns.size());
  CHECK_EQ(join_columns.size(), join_column_types.size());
  for (size_t i = 0; i < join_columns.size(); i++) {
    const auto& inner_outer_pair = inner_outer_pairs[i];
    const auto inner_col = inner_outer_pair.first;
    const auto& ti = inner_col->get_type_info();
    const auto elem_ti = ti.get_elem_type();
    // CHECK(elem_ti.is_fp());

    join_buckets.emplace_back(JoinBucketInfo{inverse_bucket_sizes_for_dimension,
                                             elem_ti.get_type() == kDOUBLE});
  }
}

JoinColumn HashJoin::fetchJoinColumn(
    const Analyzer::ColumnVar* hash_col,
    const std::vector<Fragmenter_Namespace::FragmentInfo>& fragment_info,
    const Data_Namespace::MemoryLevel effective_memory_level,
    const int device_id,
    std::vector<std::shared_ptr<Chunk_NS::Chunk>>& chunks_owner,
    std::vector<std::shared_ptr<void>>& malloc_owner,
    Executor* executor,
    ColumnCacheMap* column_cache) {
  static std::mutex fragment_fetch_mutex;
  std::lock_guard<std::mutex> fragment_fetch_lock(fragment_fetch_mutex);
  try {
    JoinColumn join_column = ColumnFetcher::makeJoinColumn(executor,
                                                           *hash_col,
                                                           fragment_info,
                                                           effective_memory_level,
                                                           device_id,
                                                           /*thread_idx=*/0,
                                                           chunks_owner,
                                                           malloc_owner,
                                                           data_provider_,
                                                           *column_cache);
    return join_column;
  } catch (...) {
    CIDER_THROW(CiderOutOfMemoryException,
                "Not enough memory for columns involved in join");
  }
}

namespace {

template <typename T>
std::string toStringFlat(const HashJoin* hash_table, const int device_id) {
  auto mem = reinterpret_cast<const T*>(hash_table->getJoinHashBuffer(device_id));
  auto memsz = hash_table->getJoinHashBufferSize(device_id) / sizeof(T);
  std::string txt;
  for (size_t i = 0; i < memsz; ++i) {
    if (i > 0) {
      txt += ", ";
    }
    txt += std::to_string(mem[i]);
  }
  return txt;
}

}  // anonymous namespace

std::string HashJoin::toStringFlat64(const int device_id) const {
  return toStringFlat<int64_t>(this, device_id);
}

std::string HashJoin::toStringFlat32(const int device_id) const {
  return toStringFlat<int32_t>(this, device_id);
}

std::ostream& operator<<(std::ostream& os, const DecodedJoinHashBufferEntry& e) {
  os << "  {{";
  bool first = true;
  for (auto k : e.key) {
    if (!first) {
      os << ",";
    } else {
      first = false;
    }
    os << k;
  }
  os << "}, ";
  os << "{";
  first = true;
  for (auto p : e.payload) {
    if (!first) {
      os << ", ";
    } else {
      first = false;
    }
    os << p;
  }
  os << "}}";
  return os;
}

std::ostream& operator<<(std::ostream& os, const DecodedJoinHashBufferSet& s) {
  os << "{\n";
  bool first = true;
  for (auto e : s) {
    if (!first) {
      os << ",\n";
    } else {
      first = false;
    }
    os << e;
  }
  if (!s.empty()) {
    os << "\n";
  }
  os << "}\n";
  return os;
}

HashJoinMatchingSet HashJoin::codegenMatchingSet(
    const std::vector<llvm::Value*>& hash_join_idx_args_in,
    const bool col_is_nullable,
    const bool is_bw_eq,
    const int64_t sub_buff_size,
    Executor* executor,
    bool is_bucketized,
    bool is_cider_format) {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  using namespace std::string_literals;

  std::string fname(is_bucketized ? "bucketized_hash_join_idx"s : "hash_join_idx"s);

  if (is_bw_eq) {
    fname += "_bitwise";
  }
  if (!is_bw_eq && col_is_nullable) {
    if (is_cider_format) {
      fname += "_nullable_cider";
    } else {
      fname += "_nullable";
    }
  }

  const auto slot_lv = executor->cgen_state_->emitCall(fname, hash_join_idx_args_in);
  const auto slot_valid_lv = executor->cgen_state_->ir_builder_.CreateICmpSGE(
      slot_lv, executor->cgen_state_->llInt(int64_t(0)));

  auto pos_ptr = hash_join_idx_args_in[0];
  CHECK(pos_ptr);

  auto count_ptr = executor->cgen_state_->ir_builder_.CreateAdd(
      pos_ptr, executor->cgen_state_->llInt(sub_buff_size));
  auto hash_join_idx_args = hash_join_idx_args_in;
  hash_join_idx_args[0] = executor->cgen_state_->ir_builder_.CreatePtrToInt(
      count_ptr, llvm::Type::getInt64Ty(executor->cgen_state_->context_));

  const auto row_count_lv = executor->cgen_state_->ir_builder_.CreateSelect(
      slot_valid_lv,
      executor->cgen_state_->emitCall(fname, hash_join_idx_args),
      executor->cgen_state_->llInt(int64_t(0)));
  auto rowid_base_i32 = executor->cgen_state_->ir_builder_.CreateIntToPtr(
      executor->cgen_state_->ir_builder_.CreateAdd(
          pos_ptr, executor->cgen_state_->llInt(2 * sub_buff_size)),
      llvm::Type::getInt32PtrTy(executor->cgen_state_->context_));
  auto rowid_ptr_i32 =
      executor->cgen_state_->ir_builder_.CreateGEP(rowid_base_i32, slot_lv);
  return {rowid_ptr_i32, row_count_lv, slot_lv};
}

llvm::Value* HashJoin::codegenHashTableLoad(const size_t table_idx, Executor* executor) {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  llvm::Value* hash_ptr = nullptr;
  const auto total_table_count =
      executor->plan_state_->join_info_.join_hash_tables_.size();
  CHECK_LT(table_idx, total_table_count);
  if (total_table_count > 1) {
    auto hash_tables_ptr =
        get_arg_by_name(executor->cgen_state_->row_func_, "join_hash_tables");
    auto hash_pptr =
        table_idx > 0 ? executor->cgen_state_->ir_builder_.CreateGEP(
                            hash_tables_ptr,
                            executor->cgen_state_->llInt(static_cast<int64_t>(table_idx)))
                      : hash_tables_ptr;
    hash_ptr = executor->cgen_state_->ir_builder_.CreateLoad(hash_pptr);
  } else {
    hash_ptr = get_arg_by_name(executor->cgen_state_->row_func_, "join_hash_tables");
  }
  CHECK(hash_ptr);
  return hash_ptr;
}

//! Make hash table from an in-flight SQL query's parse tree etc.
std::shared_ptr<HashJoin> HashJoin::getInstance(
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
    const TableIdToNodeMap& table_id_to_node_map) {
  auto timer = DEBUG_TIMER(__func__);
  std::shared_ptr<HashJoin> join_hash_table;
  CHECK_GT(device_count, 0);
  if (dynamic_cast<const Analyzer::ExpressionTuple*>(qual_bin_oper->get_left_operand())) {
    VLOG(1) << "Trying to build keyed hash table:";
    join_hash_table = BaselineJoinHashTable::getInstance(qual_bin_oper,
                                                         query_infos,
                                                         memory_level,
                                                         join_type,
                                                         preferred_hash_type,
                                                         device_count,
                                                         data_provider,
                                                         column_cache,
                                                         executor,
                                                         hashtable_build_dag_map,
                                                         table_id_to_node_map);
  } else {
    try {
      VLOG(1) << "Trying to build perfect hash table:";
      join_hash_table = PerfectJoinHashTable::getInstance(qual_bin_oper,
                                                          query_infos,
                                                          memory_level,
                                                          join_type,
                                                          preferred_hash_type,
                                                          device_count,
                                                          data_provider,
                                                          column_cache,
                                                          executor,
                                                          hashtable_build_dag_map,
                                                          table_id_to_node_map);
    } catch (CiderTooManyHashEntriesException&) {
      const auto join_quals = coalesce_singleton_equi_join(qual_bin_oper);
      CHECK_EQ(join_quals.size(), size_t(1));
      const auto join_qual =
          std::dynamic_pointer_cast<Analyzer::BinOper>(join_quals.front());
      VLOG(1) << "Trying to build keyed hash table after perfect hash table:";
      join_hash_table = BaselineJoinHashTable::getInstance(join_qual,
                                                           query_infos,
                                                           memory_level,
                                                           join_type,
                                                           preferred_hash_type,
                                                           device_count,
                                                           data_provider,
                                                           column_cache,
                                                           executor,
                                                           hashtable_build_dag_map,
                                                           table_id_to_node_map);
    }
  }
  CHECK(join_hash_table);
  {
    if (join_hash_table->getJoinHashBufferSize() <= 1000) {
      VLOG(2) << "Built CPU hash table: " << join_hash_table->toString();
    }
  }
  return join_hash_table;
}

CompositeKeyInfo HashJoin::getCompositeKeyInfo(
    const std::vector<InnerOuter>& inner_outer_pairs,
    const Executor* executor) {
  CHECK(executor);
  std::vector<const void*> sd_inner_proxy_per_key;
  std::vector<const void*> sd_outer_proxy_per_key;
  std::vector<ChunkKey> cache_key_chunks;  // used for the cache key
  const auto db_id = executor->getDatabaseId();
  for (const auto& inner_outer_pair : inner_outer_pairs) {
    const auto inner_col = inner_outer_pair.first;
    const auto outer_col = inner_outer_pair.second;
    const auto& inner_ti = inner_col->get_type_info();
    const auto& outer_ti = outer_col->get_type_info();
    ChunkKey cache_key_chunks_for_column{
        db_id, inner_col->get_table_id(), inner_col->get_column_id()};
    if (inner_ti.is_string() &&
        !(inner_ti.get_comp_param() == outer_ti.get_comp_param())) {
      CHECK(outer_ti.is_string());
      CHECK(inner_ti.get_compression() == kENCODING_DICT &&
            outer_ti.get_compression() == kENCODING_DICT);
      const auto sd_inner_proxy = executor->getStringDictionaryProxy(
          inner_ti.get_comp_param(), executor->getRowSetMemoryOwner(), true);
      const auto sd_outer_proxy = executor->getStringDictionaryProxy(
          outer_ti.get_comp_param(), executor->getRowSetMemoryOwner(), true);
      CHECK(sd_inner_proxy && sd_outer_proxy);
      sd_inner_proxy_per_key.push_back(sd_inner_proxy);
      sd_outer_proxy_per_key.push_back(sd_outer_proxy);
      cache_key_chunks_for_column.push_back(sd_outer_proxy->getGeneration());
    } else {
      sd_inner_proxy_per_key.emplace_back();
      sd_outer_proxy_per_key.emplace_back();
    }
    cache_key_chunks.push_back(cache_key_chunks_for_column);
  }
  return {sd_inner_proxy_per_key, sd_outer_proxy_per_key, cache_key_chunks};
}

std::shared_ptr<Analyzer::ColumnVar> getSyntheticColumnVar(std::string_view table,
                                                           std::string_view column,
                                                           int rte_idx,
                                                           Executor* executor) {
  auto schema_provider = executor->getSchemaProvider();
  auto table_info =
      schema_provider->getTableInfo(executor->getDatabaseId(), std::string(table));
  CHECK(table_info);
  auto col_info = schema_provider->getColumnInfo(*table_info, std::string(column));
  CHECK(col_info);

  auto cv = std::make_shared<Analyzer::ColumnVar>(col_info, rte_idx);
  return cv;
}

class AllColumnVarsVisitor
    : public ScalarExprVisitor<std::set<const Analyzer::ColumnVar*>> {
 protected:
  std::set<const Analyzer::ColumnVar*> visitColumnVar(
      const Analyzer::ColumnVar* column) const override {
    return {column};
  }

  std::set<const Analyzer::ColumnVar*> visitColumnVarTuple(
      const Analyzer::ExpressionTuple* expr_tuple) const override {
    AllColumnVarsVisitor visitor;
    std::set<const Analyzer::ColumnVar*> result;
    for (const auto& expr_component : expr_tuple->getTuple()) {
      const auto component_rte_set = visitor.visit(expr_component.get());
      result.insert(component_rte_set.begin(), component_rte_set.end());
    }
    return result;
  }

  std::set<const Analyzer::ColumnVar*> aggregateResult(
      const std::set<const Analyzer::ColumnVar*>& aggregate,
      const std::set<const Analyzer::ColumnVar*>& next_result) const override {
    auto result = aggregate;
    result.insert(next_result.begin(), next_result.end());
    return result;
  }
};

void setupSyntheticCaching(DataProvider* data_provider,
                           std::set<const Analyzer::ColumnVar*> cvs,
                           Executor* executor) {
  std::unordered_set<int> phys_table_ids;
  for (auto cv : cvs) {
    phys_table_ids.insert(cv->get_table_id());
  }

  std::unordered_set<InputColDescriptor> col_descs;
  for (auto cv : cvs) {
    col_descs.emplace(InputColDescriptor{cv->get_column_info(), cv->get_rte_idx()});
  }

  executor->setupCaching(data_provider, col_descs, phys_table_ids);
}

std::vector<InputTableInfo> getSyntheticInputTableInfo(
    std::set<const Analyzer::ColumnVar*> cvs,
    Executor* executor) {
  std::unordered_set<int> phys_table_ids;
  for (auto cv : cvs) {
    phys_table_ids.insert(cv->get_table_id());
  }

  // NOTE(sy): This vector ordering seems to work for now, but maybe we need to
  // review how rte_idx is assigned for ColumnVars. See for example Analyzer.h
  // and RelAlgExecutor.cpp and rte_idx there.
  std::vector<InputTableInfo> query_infos(phys_table_ids.size());
  size_t i = 0;
  for (auto id : phys_table_ids) {
    query_infos[i].table_id = id;
    query_infos[i].info =
        executor->getDataProvider()->getTableMetadata(executor->getDatabaseId(), id);
    ++i;
  }

  return query_infos;
}

//! Make hash table from named tables and columns (such as for testing).
std::shared_ptr<HashJoin> HashJoin::getSyntheticInstance(
    std::string_view table1,
    std::string_view column1,
    std::string_view table2,
    std::string_view column2,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    Executor* executor) {
  auto a1 = getSyntheticColumnVar(table1, column1, 0, executor);
  auto a2 = getSyntheticColumnVar(table2, column2, 1, executor);

  auto qual_bin_oper = std::make_shared<Analyzer::BinOper>(kBOOLEAN, kEQ, kONE, a1, a2);

  std::set<const Analyzer::ColumnVar*> cvs =
      AllColumnVarsVisitor().visit(qual_bin_oper.get());
  auto query_infos = getSyntheticInputTableInfo(cvs, executor);
  setupSyntheticCaching(data_provider, cvs, executor);
  RegisteredQueryHint query_hint = RegisteredQueryHint::defaults();

  auto hash_table = HashJoin::getInstance(qual_bin_oper,
                                          query_infos,
                                          memory_level,
                                          JoinType::INNER,
                                          preferred_hash_type,
                                          device_count,
                                          data_provider,
                                          column_cache,
                                          executor,
                                          {},
                                          query_hint,
                                          {});
  return hash_table;
}

//! Make hash table from named tables and columns (such as for testing).
std::shared_ptr<HashJoin> HashJoin::getSyntheticInstance(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    Executor* executor) {
  std::set<const Analyzer::ColumnVar*> cvs =
      AllColumnVarsVisitor().visit(qual_bin_oper.get());
  auto query_infos = getSyntheticInputTableInfo(cvs, executor);
  setupSyntheticCaching(data_provider, cvs, executor);
  RegisteredQueryHint query_hint = RegisteredQueryHint::defaults();

  auto hash_table = HashJoin::getInstance(qual_bin_oper,
                                          query_infos,
                                          memory_level,
                                          JoinType::INNER,
                                          preferred_hash_type,
                                          device_count,
                                          data_provider,
                                          column_cache,
                                          executor,
                                          {},
                                          query_hint,
                                          {});
  return hash_table;
}

std::pair<std::string, std::shared_ptr<HashJoin>> HashJoin::getSyntheticInstance(
    std::vector<std::shared_ptr<Analyzer::BinOper>> qual_bin_opers,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    DataProvider* data_provider,
    ColumnCacheMap& column_cache,
    Executor* executor) {
  std::set<const Analyzer::ColumnVar*> cvs;
  for (auto& qual : qual_bin_opers) {
    auto cv = AllColumnVarsVisitor().visit(qual.get());
    cvs.insert(cv.begin(), cv.end());
  }
  auto query_infos = getSyntheticInputTableInfo(cvs, executor);
  setupSyntheticCaching(data_provider, cvs, executor);
  RegisteredQueryHint query_hint = RegisteredQueryHint::defaults();
  std::shared_ptr<HashJoin> hash_table;
  std::string error_msg;
  for (auto& qual : qual_bin_opers) {
    try {
      auto candidate_hash_table = HashJoin::getInstance(qual,
                                                        query_infos,
                                                        memory_level,
                                                        JoinType::INNER,
                                                        preferred_hash_type,
                                                        device_count,
                                                        data_provider,
                                                        column_cache,
                                                        executor,
                                                        {},
                                                        query_hint,
                                                        {});
      if (candidate_hash_table) {
        hash_table = candidate_hash_table;
      }
    } catch (CiderHashJoinException& e) {
      error_msg = e.what();
      continue;
    }
  }
  return std::make_pair(error_msg, hash_table);
}

InnerOuter HashJoin::normalizeColumnPair(const Analyzer::Expr* lhs,
                                         const Analyzer::Expr* rhs,
                                         SchemaProviderPtr schema_provider,
                                         const TemporaryTables* temporary_tables) {
  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();
  if (lhs_ti.get_type() != rhs_ti.get_type()) {
    CIDER_THROW(CiderHashJoinException,
                "Equijoin types must be identical, found: " + lhs_ti.get_type_name() +
                    ", " + rhs_ti.get_type_name());
  }
  if (!lhs_ti.is_integer() && !lhs_ti.is_time() && !lhs_ti.is_string() &&
      !lhs_ti.is_decimal()) {
    CIDER_THROW(CiderHashJoinException,
                "Cannot apply hash join to inner column type " + lhs_ti.get_type_name());
  }
  // Decimal types should be identical.
  if (lhs_ti.is_decimal() && (lhs_ti.get_scale() != rhs_ti.get_scale() ||
                              lhs_ti.get_precision() != rhs_ti.get_precision())) {
    CIDER_THROW(CiderHashJoinException, "Equijoin with different decimal types");
  }

  const auto lhs_cast = dynamic_cast<const Analyzer::UOper*>(lhs);
  const auto rhs_cast = dynamic_cast<const Analyzer::UOper*>(rhs);
  if (lhs_ti.is_string() && (static_cast<bool>(lhs_cast) != static_cast<bool>(rhs_cast) ||
                             (lhs_cast && lhs_cast->get_optype() != kCAST) ||
                             (rhs_cast && rhs_cast->get_optype() != kCAST))) {
    CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
  }
  // Casts to decimal are not suported.
  if (lhs_ti.is_decimal() && (lhs_cast || rhs_cast)) {
    CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
  }
  const auto lhs_col =
      lhs_cast ? dynamic_cast<const Analyzer::ColumnVar*>(lhs_cast->get_operand())
               : dynamic_cast<const Analyzer::ColumnVar*>(lhs);
  const auto rhs_col =
      rhs_cast ? dynamic_cast<const Analyzer::ColumnVar*>(rhs_cast->get_operand())
               : dynamic_cast<const Analyzer::ColumnVar*>(rhs);
  if (!lhs_col && !rhs_col) {
    CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
  }
  const Analyzer::ColumnVar* inner_col{nullptr};
  const Analyzer::ColumnVar* outer_col{nullptr};
  auto outer_ti = lhs_ti;
  auto inner_ti = rhs_ti;
  const Analyzer::Expr* outer_expr{lhs};
  if (!lhs_col || (rhs_col && lhs_col->get_rte_idx() < rhs_col->get_rte_idx())) {
    inner_col = rhs_col;
    outer_col = lhs_col;
  } else {
    if (lhs_col && lhs_col->get_rte_idx() == 0) {
      CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
    }
    inner_col = lhs_col;
    outer_col = rhs_col;
    std::swap(outer_ti, inner_ti);
    outer_expr = rhs;
  }
  if (!inner_col) {
    CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
  }
  if (!outer_col) {
    // check whether outer_col is a constant, i.e., inner_col = K;
    const auto outer_constant_col = dynamic_cast<const Analyzer::Constant*>(outer_expr);
    if (outer_constant_col) {
      CIDER_THROW(
          CiderHashJoinException,
          "Cannot use hash join for given expression: try to join with a constant value");
    }
    MaxRangeTableIndexVisitor rte_idx_visitor;
    int outer_rte_idx = rte_idx_visitor.visit(outer_expr);
    // The inner column candidate is not actually inner; the outer
    // expression contains columns which are at least as deep.
    if (inner_col->get_rte_idx() <= outer_rte_idx) {
      CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
    }
  }
  // We need to fetch the actual type information from the schema provider since Analyzer
  // always reports nullable as true for inner table columns in left joins.
  const auto inner_col_info =
      schema_provider->getColumnInfo(*inner_col->get_column_info());
  const auto inner_col_real_ti = get_column_type(inner_col->get_column_id(),
                                                 inner_col->get_table_id(),
                                                 inner_col_info,
                                                 temporary_tables);
  const auto& outer_col_ti =
      !(dynamic_cast<const Analyzer::FunctionOper*>(lhs)) && outer_col
          ? outer_col->get_type_info()
          : outer_ti;
  // Casts from decimal are not supported.
  if ((inner_col_real_ti.is_decimal() || outer_col_ti.is_decimal()) &&
      (lhs_cast || rhs_cast)) {
    CIDER_THROW(CiderHashJoinException, "Cannot use hash join for given expression");
  }

  if (!(inner_col_real_ti.is_integer() || inner_col_real_ti.is_time() ||
        inner_col_real_ti.is_decimal() ||
        (inner_col_real_ti.is_string() &&
         inner_col_real_ti.get_compression() == kENCODING_DICT))) {
    CIDER_THROW(
        CiderHashJoinException,
        "Can only apply hash join to integer-like types and dictionary encoded strings");
  }

  auto normalized_inner_col = inner_col;
  auto normalized_outer_col = outer_col ? outer_col : outer_expr;

  const auto& normalized_inner_ti = normalized_inner_col->get_type_info();
  const auto& normalized_outer_ti = normalized_outer_col->get_type_info();

  if (normalized_inner_ti.is_string() != normalized_outer_ti.is_string()) {
    CIDER_THROW(CiderHashJoinException,
                "Could not build hash tables for incompatible types " +
                    normalized_inner_ti.get_type_name() + " and " +
                    normalized_outer_ti.get_type_name());
  }

  return {normalized_inner_col, normalized_outer_col};
}

std::vector<InnerOuter> HashJoin::normalizeColumnPairs(
    const Analyzer::BinOper* condition,
    SchemaProviderPtr schema_provider,
    const TemporaryTables* temporary_tables) {
  std::vector<InnerOuter> result;
  const auto lhs_tuple_expr =
      dynamic_cast<const Analyzer::ExpressionTuple*>(condition->get_left_operand());
  const auto rhs_tuple_expr =
      dynamic_cast<const Analyzer::ExpressionTuple*>(condition->get_right_operand());

  CHECK_EQ(static_cast<bool>(lhs_tuple_expr), static_cast<bool>(rhs_tuple_expr));
  if (lhs_tuple_expr) {
    const auto& lhs_tuple = lhs_tuple_expr->getTuple();
    const auto& rhs_tuple = rhs_tuple_expr->getTuple();
    CHECK_EQ(lhs_tuple.size(), rhs_tuple.size());
    for (size_t i = 0; i < lhs_tuple.size(); ++i) {
      result.push_back(normalizeColumnPair(
          lhs_tuple[i].get(), rhs_tuple[i].get(), schema_provider, temporary_tables));
    }
  } else {
    CHECK(!lhs_tuple_expr && !rhs_tuple_expr);
    result.push_back(normalizeColumnPair(condition->get_left_operand(),
                                         condition->get_right_operand(),
                                         schema_provider,
                                         temporary_tables));
  }

  return result;
}
