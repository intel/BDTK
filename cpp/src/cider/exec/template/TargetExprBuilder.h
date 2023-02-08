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

/**
 * @file    TargetExprBuilder.h
 * @brief   Helpers for codegen of target expressions
 */

#pragma once

#include "type/plan/Analyzer.h"
#include "util/TargetInfo.h"

#include "GroupByAndAggregate.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"

#include <vector>

struct TargetExprCodegen {
  TargetExprCodegen(const Analyzer::Expr* target_expr,
                    TargetInfo& target_info,
                    const int32_t base_slot_index,
                    const size_t target_idx,
                    const bool is_group_by)
      : target_expr(target_expr)
      , target_info(target_info)
      , base_slot_index(base_slot_index)
      , target_idx(target_idx)
      , is_group_by(is_group_by) {}

  void codegen(GroupByAndAggregate* group_by_and_agg,
               Executor* executor,
               const QueryMemoryDescriptor& query_mem_desc,
               const CompilationOptions& co,
               const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
               const std::vector<llvm::Value*>& agg_out_vec,
               llvm::Value* output_buffer_byte_stream,
               llvm::Value* out_row_idx,
               llvm::Value* varlen_output_buffer,
               DiamondCodegen& diamond_codegen,
               DiamondCodegen* sample_cfg = nullptr) const;

  void codegen(GroupByAndAggregate* group_by_and_agg,
               Executor* executor,
               const QueryMemoryDescriptor& query_mem_desc,
               const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
               const std::vector<llvm::Value*>& agg_out_vec,
               const CompilationOptions& co) const;

  void codegenAggregate(GroupByAndAggregate* group_by_and_agg,
                        Executor* executor,
                        const QueryMemoryDescriptor& query_mem_desc,
                        const CompilationOptions& co,
                        const std::vector<llvm::Value*>& target_lvs,
                        const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
                        const std::vector<llvm::Value*>& agg_out_vec,
                        llvm::Value* output_buffer_byte_stream,
                        llvm::Value* out_row_idx,
                        llvm::Value* varlen_output_buffer,
                        int32_t slot_index) const;

  void codegenAggregate(Executor* executor,
                        const QueryMemoryDescriptor& query_mem_desc,
                        CodegenColValues* agg_input_data,
                        const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
                        const std::vector<llvm::Value*>& agg_out_vec,
                        int32_t slot_index,
                        const CompilationOptions& co) const;

  const Analyzer::Expr* target_expr;
  TargetInfo target_info;

  int32_t base_slot_index;
  size_t target_idx;
  bool is_group_by;
};

struct TargetExprCodegenBuilder {
  TargetExprCodegenBuilder(const QueryMemoryDescriptor& query_mem_desc,
                           const RelAlgExecutionUnit& ra_exe_unit,
                           const bool is_group_by)
      : query_mem_desc(query_mem_desc)
      , ra_exe_unit(ra_exe_unit)
      , is_group_by(is_group_by) {}

  void operator()(const Analyzer::Expr* target_expr,
                  const Executor* executor,
                  const CompilationOptions& co);

  void codegen(GroupByAndAggregate* group_by_and_agg,
               Executor* executor,
               const QueryMemoryDescriptor& query_mem_desc,
               const CompilationOptions& co,
               const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
               const std::vector<llvm::Value*>& agg_out_vec,
               llvm::Value* output_buffer_byte_stream,
               llvm::Value* out_row_idx,
               llvm::Value* varlen_output_buffer,
               DiamondCodegen& diamond_codegen) const;

  size_t target_index_counter{0};
  size_t slot_index_counter{0};

  const QueryMemoryDescriptor& query_mem_desc;
  const RelAlgExecutionUnit& ra_exe_unit;

  std::vector<TargetExprCodegen> target_exprs_to_codegen;
  bool is_group_by;
};
