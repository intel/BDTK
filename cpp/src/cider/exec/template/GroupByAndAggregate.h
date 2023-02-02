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

#ifndef QUERYENGINE_GROUPBYANDAGGREGATE_H
#define QUERYENGINE_GROUPBYANDAGGREGATE_H

#include "CodegenColValues.h"
#include "ColumnarResults.h"
#include "CompilationOptions.h"
#include "InputMetadata.h"
#include "QueryExecutionContext.h"
#include "exec/template/BufferCompaction.h"
#include "function/scalar/RuntimeFunctions.h"

#include "exec/template/common/DiamondCodegen.h"

#include "type/data/sqltypes.h"
#include "type/schema/SchemaProvider.h"
#include "util/Logger.h"

#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Value.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/make_unique.hpp>

#include <stack>
#include <vector>

extern bool g_enable_smem_group_by;
extern bool g_bigint_count;

struct ColRangeInfo {
  QueryDescriptionType hash_type_;
  int64_t min;
  int64_t max;
  int64_t bucket;
  bool has_nulls;
  bool isEmpty() { return min == 0 && max == -1; }
};

struct KeylessInfo {
  const bool keyless;
  const int32_t target_index;
};

class GroupByAndAggregate {
 public:
  GroupByAndAggregate(Executor* executor,
                      const RelAlgExecutionUnit& ra_exe_unit,
                      const std::vector<InputTableInfo>& query_infos,
                      std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                      const std::optional<int64_t>& group_cardinality_estimation);

  // returns true iff checking the error code after every row
  // is required -- slow path group by queries for now
  bool codegen(llvm::Value* filter_result,
               llvm::BasicBlock* sc_false,
               const QueryMemoryDescriptor& query_mem_desc,
               const CompilationOptions& co);

  static size_t shard_count_for_top_groups(const RelAlgExecutionUnit& ra_exe_unit,
                                           const SchemaProvider& schema_provider);

 private:
  std::unique_ptr<QueryMemoryDescriptor> initQueryMemoryDescriptor(
      const bool allow_multifrag,
      const size_t max_groups_buffer_entry_count,
      const int8_t crt_min_byte_width,
      const bool output_columnar_hint,
      const CompilationOptions& co);

  std::unique_ptr<QueryMemoryDescriptor> initQueryMemoryDescriptorImpl(
      const bool allow_multifrag,
      const size_t max_groups_buffer_entry_count,
      const int8_t crt_min_byte_width,
      const bool must_use_baseline_sort,
      const bool output_columnar_hint,
      const CompilationOptions& co);

  int64_t getShardedTopBucket(const ColRangeInfo& col_range_info,
                              const size_t shard_count) const;

  llvm::Value* codegenOutputSlot(llvm::Value* groups_buffer,
                                 const QueryMemoryDescriptor& query_mem_desc,
                                 const CompilationOptions& co,
                                 DiamondCodegen& diamond_codegen);

  std::tuple<llvm::Value*, llvm::Value*> codegenGroupBy(
      const QueryMemoryDescriptor& query_mem_desc,
      const CompilationOptions& co,
      DiamondCodegen& codegen);

  llvm::Value* codegenVarlenOutputBuffer(const QueryMemoryDescriptor& query_mem_desc);

  std::tuple<llvm::Value*, llvm::Value*> codegenSingleColumnPerfectHash(
      const QueryMemoryDescriptor& query_mem_desc,
      const CompilationOptions& co,
      llvm::Value* groups_buffer,
      llvm::Value* group_expr_lv_translated,
      llvm::Value* group_expr_lv_original,
      const int32_t row_size_quad);

  std::tuple<llvm::Value*, llvm::Value*> codegenMultiColumnPerfectHash(
      llvm::Value* groups_buffer,
      llvm::Value* group_key,
      llvm::Value* key_size_lv,
      const QueryMemoryDescriptor& query_mem_desc,
      const int32_t row_size_quad);
  llvm::Function* codegenPerfectHashFunction();

  std::tuple<llvm::Value*, llvm::Value*> codegenMultiColumnBaselineHash(
      const CompilationOptions& co,
      llvm::Value* groups_buffer,
      llvm::Value* group_key,
      llvm::Value* key_size_lv,
      const QueryMemoryDescriptor& query_mem_desc,
      const size_t key_width,
      const int32_t row_size_quad);

  ColRangeInfo getColRangeInfo();

  static int64_t getBucketedCardinality(const ColRangeInfo& col_range_info);

  llvm::Value* convertNullIfAny(const SQLTypeInfo& arg_type,
                                const TargetInfo& agg_info,
                                llvm::Value* target);

  bool codegenAggCalls(const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
                       llvm::Value* varlen_output_buffer,
                       const std::vector<llvm::Value*>& agg_out_vec,
                       const QueryMemoryDescriptor& query_mem_desc,
                       const CompilationOptions& co,
                       DiamondCodegen& diamond_codegen);

  llvm::Value* codegenWindowRowPointer(const Analyzer::WindowFunction* window_func,
                                       const QueryMemoryDescriptor& query_mem_desc,
                                       const CompilationOptions& co,
                                       DiamondCodegen& diamond_codegen);

  llvm::Value* codegenAggColumnPtr(
      llvm::Value* output_buffer_byte_stream,
      llvm::Value* out_row_idx,
      const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
      const QueryMemoryDescriptor& query_mem_desc,
      const size_t chosen_bytes,
      const size_t agg_out_off,
      const size_t target_idx);

  void codegenEstimator(std::stack<llvm::BasicBlock*>& array_loops,
                        DiamondCodegen& diamond_codegen,
                        const QueryMemoryDescriptor& query_mem_desc,
                        const CompilationOptions&);

  void codegenCountDistinct(const size_t target_idx,
                            const Analyzer::Expr* target_expr,
                            std::vector<llvm::Value*>& agg_args,
                            const QueryMemoryDescriptor&);

  void codegenApproxQuantile(const size_t target_idx,
                             const Analyzer::Expr* target_expr,
                             std::vector<llvm::Value*>& agg_args,
                             const QueryMemoryDescriptor& query_mem_desc);

  llvm::Value* getAdditionalLiteral(const int32_t off);

  std::vector<llvm::Value*> codegenAggArg(const Analyzer::Expr* target_expr,
                                          const CompilationOptions& co);

  std::unique_ptr<CodegenColValues> codegenAggArgCider(const Analyzer::Expr* target_expr,
                                                       const CompilationOptions& co);

  llvm::Value* emitCall(const std::string& fname, const std::vector<llvm::Value*>& args);

  void checkErrorCode(llvm::Value* retCode);

  Executor* executor_;
  const RelAlgExecutionUnit& ra_exe_unit_;
  const std::vector<InputTableInfo>& query_infos_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  bool output_columnar_;

  const std::optional<int64_t> group_cardinality_estimation_;

  friend class Executor;
  friend class QueryMemoryDescriptor;
  friend class CodeGenerator;
  friend struct TargetExprCodegen;
  friend struct TargetExprCodegenBuilder;
};

inline int64_t extract_from_datum(const Datum datum, const SQLTypeInfo& ti) {
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();
  switch (type) {
    case kBOOLEAN:
      return datum.tinyintval;
    case kTINYINT:
      return datum.tinyintval;
    case kSMALLINT:
      return datum.smallintval;
    case kCHAR:
    case kVARCHAR:
    case kTEXT:
      CHECK_EQ(kENCODING_DICT, ti.get_compression());
    case kINT:
      return datum.intval;
    case kBIGINT:
      return datum.bigintval;
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
      return datum.bigintval;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("type is {}", type));
  }
}

inline int64_t extract_min_stat(const ChunkStats& stats, const SQLTypeInfo& ti) {
  return extract_from_datum(stats.min, ti);
}

inline int64_t extract_max_stat(const ChunkStats& stats, const SQLTypeInfo& ti) {
  return extract_from_datum(stats.max, ti);
}

#endif  // QUERYENGINE_GROUPBYANDAGGREGATE_H
