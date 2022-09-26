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

/**
 * @file    TargetExprBuilder.cpp
 * @brief   Helpers for codegen of target expressions
 */

#include "TargetExprBuilder.h"

#include "CodeGenerator.h"
#include "Execute.h"
#include "GroupByAndAggregate.h"
#include "OutputBufferInitialization.h"
#include "util/Logger.h"

#include "exec/template/CodegenColValues.h"
#include "exec/template/operator/aggregate/CiderAggregateCodeGenerator.h"

#define LL_CONTEXT executor->cgen_state_->context_
#define LL_BUILDER executor->cgen_state_->ir_builder_
#define LL_BOOL(v) executor->ll_bool(v)
#define LL_INT(v) executor->cgen_state_->llInt(v)
#define LL_FP(v) executor->cgen_state_->llFp(v)
#define ROW_FUNC executor->cgen_state_->row_func_

namespace {

inline bool is_varlen_projection(const Analyzer::Expr* target_expr,
                                 const SQLTypeInfo& ti) {
  return false;
}

std::vector<std::string> agg_fn_base_names(const TargetInfo& target_info,
                                           const bool is_varlen_projection) {
  const auto& chosen_type = get_compact_type(target_info);
  if (is_varlen_projection) {
    UNREACHABLE();
    return {"agg_id_varlen"};
  }
  if (!target_info.is_agg || target_info.agg_kind == kSAMPLE) {
    if (chosen_type.is_varlen()) {
      // not a varlen projection (not creating new varlen outputs). Just store the pointer
      // and offset into the input buffer in the output slots.
      return {"agg_id", "agg_id"};
    }
    return {"agg_id"};
  }
  switch (target_info.agg_kind) {
    case kAVG:
      return {"agg_sum", "agg_count"};
    case kCOUNT:
      return {target_info.is_distinct ? "agg_count_distinct" : "agg_count"};
    case kMAX:
      return {"agg_max"};
    case kMIN:
      return {"agg_min"};
    case kSUM:
      return {"agg_sum"};
    case kAPPROX_COUNT_DISTINCT:
      return {"agg_approximate_count_distinct"};
    case kAPPROX_QUANTILE:
      return {"agg_approx_quantile"};
    case kSINGLE_VALUE:
      return {"checked_single_agg_id"};
    case kSAMPLE:
      return {"agg_id"};
    default:
      UNREACHABLE() << "Unrecognized agg kind: " << std::to_string(target_info.agg_kind);
  }
  return {};
}

inline bool is_columnar_projection(const QueryMemoryDescriptor& query_mem_desc) {
  return query_mem_desc.getQueryDescriptionType() == QueryDescriptionType::Projection &&
         query_mem_desc.didOutputColumnar();
}

bool is_simple_count(const TargetInfo& target_info) {
  return target_info.is_agg && target_info.agg_kind == kCOUNT && !target_info.is_distinct;
}

}  // namespace

void TargetExprCodegen::codegen(
    GroupByAndAggregate* group_by_and_agg,
    Executor* executor,
    const QueryMemoryDescriptor& query_mem_desc,
    const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
    const std::vector<llvm::Value*>& agg_out_vec,
    const CompilationOptions& co) const {
  CHECK(group_by_and_agg);
  CHECK(executor);
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());

  // TBD: Window functions support

  auto target_arg = group_by_and_agg->codegenAggArgCider(target_expr, co);

  int32_t slot_index = base_slot_index;
  CHECK_GE(slot_index, 0);
  CHECK(is_group_by || static_cast<size_t>(slot_index) < agg_out_vec.size());

  codegenAggregate(executor,
                   query_mem_desc,
                   target_arg.get(),
                   agg_out_ptr_w_idx,
                   agg_out_vec,
                   slot_index,
                   co);
}

void TargetExprCodegen::codegen(
    GroupByAndAggregate* group_by_and_agg,
    Executor* executor,
    const QueryMemoryDescriptor& query_mem_desc,
    const CompilationOptions& co,
    const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx_in,
    const std::vector<llvm::Value*>& agg_out_vec,
    llvm::Value* output_buffer_byte_stream,
    llvm::Value* out_row_idx,
    llvm::Value* varlen_output_buffer,
    DiamondCodegen& diamond_codegen,
    DiamondCodegen* sample_cfg) const {
  CHECK(group_by_and_agg);
  CHECK(executor);
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());

  auto agg_out_ptr_w_idx = agg_out_ptr_w_idx_in;
  const auto arg_expr = agg_arg(target_expr);

  const bool varlen_projection = is_varlen_projection(target_expr, target_info.sql_type);
  const auto agg_fn_names = agg_fn_base_names(target_info, varlen_projection);
  const auto window_func = dynamic_cast<const Analyzer::WindowFunction*>(target_expr);
  WindowProjectNodeContext::resetWindowFunctionContext(executor);
  auto target_lvs =
      window_func
          ? std::vector<llvm::Value*>{executor->codegenWindowFunction(target_idx, co)}
          : group_by_and_agg->codegenAggArg(target_expr, co);
  const auto window_row_ptr = window_func
                                  ? group_by_and_agg->codegenWindowRowPointer(
                                        window_func, query_mem_desc, co, diamond_codegen)
                                  : nullptr;
  if (window_row_ptr) {
    agg_out_ptr_w_idx =
        std::make_tuple(window_row_ptr, std::get<1>(agg_out_ptr_w_idx_in));
    if (window_function_is_aggregate(window_func->getKind())) {
      out_row_idx = window_row_ptr;
    }
  }

  llvm::Value* str_target_lv{nullptr};
  if (target_lvs.size() == 3) {
    // none encoding string, pop the packed pointer + length since
    // it's only useful for IS NULL checks and assumed to be only
    // two components (pointer and length) for the purpose of projection
    str_target_lv = target_lvs.front();
    target_lvs.erase(target_lvs.begin());
  }
  if (target_lvs.size() < agg_fn_names.size()) {
    CHECK_EQ(size_t(1), target_lvs.size());
    CHECK_EQ(size_t(2), agg_fn_names.size());
    for (size_t i = 1; i < agg_fn_names.size(); ++i) {
      target_lvs.push_back(target_lvs.front());
    }
  } else {
    CHECK(str_target_lv || (agg_fn_names.size() == target_lvs.size()));
    CHECK(target_lvs.size() == 1 || target_lvs.size() == 2);
  }

  int32_t slot_index = base_slot_index;
  CHECK_GE(slot_index, 0);
  CHECK(is_group_by || static_cast<size_t>(slot_index) < agg_out_vec.size());

  uint32_t col_off{0};
  codegenAggregate(group_by_and_agg,
                   executor,
                   query_mem_desc,
                   co,
                   target_lvs,
                   agg_out_ptr_w_idx,
                   agg_out_vec,
                   output_buffer_byte_stream,
                   out_row_idx,
                   varlen_output_buffer,
                   slot_index);
}

void TargetExprCodegen::codegenAggregate(
    Executor* executor,
    const QueryMemoryDescriptor& query_mem_desc,
    CodegenColValues* agg_input_data,
    const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
    const std::vector<llvm::Value*>& agg_out_vec,
    int32_t slot_index,
    const CompilationOptions& co) const {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  size_t target_lv_idx = 0;
  const bool lazy_fetched{executor->plan_state_->isLazyFetchColumn(target_expr)};
  const bool is_non_groupby_agg = query_mem_desc.getQueryDescriptionType() ==
                                  QueryDescriptionType::NonGroupedAggregate;

  CodeGenerator code_generator(executor);

  auto& context = getGlobalLLVMContext();

  const auto agg_fn_names = agg_fn_base_names(
      target_info, is_varlen_projection(target_expr, target_info.sql_type));
  auto arg_expr = agg_arg(target_expr);

  for (const auto& agg_base_name : agg_fn_names) {
    // TBD: Distinct Count support.
    // TBD: Array type support.
    // TBD: String type support.
    // TBD: Varlen Projection support.
    // TBD: Projection support.
    // TBD: Window Function support.
    // TBD: Lazy fetch support.

    // TODO: Non-Groupby Agg support.
    // TODO: Projection support.

    if (auto window_func = dynamic_cast<const Analyzer::WindowFunction*>(target_expr)) {
      CIDER_THROW(CiderCompileException,
                  "TargetExpr codegen is not support window function now.");
    }
    if (!is_group_by && !is_non_groupby_agg) {
      CIDER_THROW(CiderCompileException,
                  "TargetExpr codegen is only support group-by aggregation now.");
    }

    const auto chosen_bytes =
        static_cast<size_t>(query_mem_desc.getPaddedSlotWidthBytes(slot_index));

    std::unique_ptr<AggregateCodeGenerator> generator;
    if (query_mem_desc.getQueryDescriptionType() == QueryDescriptionType::Projection) {
      generator = ProjectIDCodeGenerator::Make(
          agg_base_name, target_info, executor->cgen_state_.get());
    } else {
      switch (target_info.agg_kind) {
        case kMAX:
        case kMIN:
        case kSUM:
        case kSAMPLE:
          generator = SimpleAggregateCodeGenerator::Make(
              agg_base_name, target_info, chosen_bytes, executor->cgen_state_.get());
          break;
        case kAVG:
          generator =
              (0 == target_lv_idx
                   ? SimpleAggregateCodeGenerator::Make(agg_base_name,
                                                        target_info,
                                                        chosen_bytes,
                                                        executor->cgen_state_.get())
                   : CountAggregateCodeGenerator::Make(agg_base_name,
                                                       target_info,
                                                       chosen_bytes,
                                                       executor->cgen_state_.get(),
                                                       true));
          break;
        case kCOUNT:
          generator = CountAggregateCodeGenerator::Make(agg_base_name,
                                                        target_info,
                                                        chosen_bytes,
                                                        executor->cgen_state_.get(),
                                                        arg_expr);
          break;
        default:
          CIDER_THROW(CiderCompileException,
                      "Unsupported aggregation type in TargetExprBuilder.");
      }
      CHECK(generator);

      if (is_group_by) {
        if (query_mem_desc.getQueryDescriptionType() ==
            QueryDescriptionType::Projection) {
          // Projection
          llvm::Value *project_arraies_vec = std::get<0>(agg_out_ptr_w_idx),
                      *row_num = std::get<1>(agg_out_ptr_w_idx);
          llvm::Value* project_arraies_ptr =
              LL_BUILDER.CreateGEP(project_arraies_vec, LL_INT(slot_index));
          llvm::Value* project_arraies_i8 =
              LL_BUILDER.CreateIntToPtr(LL_BUILDER.CreateLoad(project_arraies_ptr, false),
                                        llvm::Type::getInt8PtrTy(context));
          llvm::Value* col_data =
              executor->cgen_state_->emitCall("cider_ColDecoder_extractArrowBuffersAt",
                                              {project_arraies_i8, LL_INT((uint64_t)1)});
          if (target_info.skip_null_val) {
            llvm::Value* col_null = executor->cgen_state_->emitCall(
                "cider_ColDecoder_extractArrowBuffersAt",
                {project_arraies_i8, LL_INT((uint64_t)0)});
            generator->codegen(agg_input_data, col_data, row_num, col_null);
          } else {
            generator->codegen(agg_input_data, col_data, row_num);
          }
        } else {
          // Fetch output buffers.
          llvm::Value *agg_col_ptr = nullptr, *agg_null_ptr = nullptr, *index = nullptr;
          agg_col_ptr = std::get<0>(agg_out_ptr_w_idx);
          agg_col_ptr->setName("agg_col_ptr");
          agg_null_ptr = LL_BUILDER.CreateGEP(
              LL_BUILDER.CreateBitCast(agg_col_ptr, llvm::Type::getInt8PtrTy(context)),
              LL_INT(query_mem_desc.getNullVectorOffsetOfGroupTargets()));
          index = LL_INT(query_mem_desc.getColOnlyOffInBytes(slot_index) / chosen_bytes);
          generator->codegen(agg_input_data, agg_col_ptr, index, agg_null_ptr);
        }
      } else if (is_non_groupby_agg) {
        generator->codegen(agg_input_data,
                           agg_out_vec[slot_index],
                           LL_INT(uint64_t(0)),
                           agg_out_vec[slot_index + agg_out_vec.size() / 2]);
        // TODO(qi): Move to query_template
        if (!target_info.skip_null_val) {
          std::vector<llvm::Value*> func_args = {
              agg_out_vec[slot_index + agg_out_vec.size() / 2], LL_INT(uint64_t(1))};
          executor->cgen_state_->emitCall("agg_id", func_args);
        }
      }

      ++slot_index;
      ++target_lv_idx;
    }
  }
}

void TargetExprCodegen::codegenAggregate(
    GroupByAndAggregate* group_by_and_agg,
    Executor* executor,
    const QueryMemoryDescriptor& query_mem_desc,
    const CompilationOptions& co,
    const std::vector<llvm::Value*>& target_lvs,
    const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
    const std::vector<llvm::Value*>& agg_out_vec,
    llvm::Value* output_buffer_byte_stream,
    llvm::Value* out_row_idx,
    llvm::Value* varlen_output_buffer,
    int32_t slot_index) const {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  size_t target_lv_idx = 0;
  const bool lazy_fetched{executor->plan_state_->isLazyFetchColumn(target_expr)};

  CodeGenerator code_generator(executor);

  const auto agg_fn_names = agg_fn_base_names(
      target_info, is_varlen_projection(target_expr, target_info.sql_type));
  auto arg_expr = agg_arg(target_expr);

  for (const auto& agg_base_name : agg_fn_names) {
    if (target_info.is_distinct && arg_expr->get_type_info().is_array()) {
      CHECK_EQ(static_cast<size_t>(query_mem_desc.getLogicalSlotWidthBytes(slot_index)),
               sizeof(int64_t));
      // TODO(miyu): check if buffer may be columnar here
      CHECK(!query_mem_desc.didOutputColumnar());
      const auto& elem_ti = arg_expr->get_type_info().get_elem_type();
      uint32_t col_off{0};
      if (is_group_by) {
        const auto col_off_in_bytes = query_mem_desc.getColOnlyOffInBytes(slot_index);
        CHECK_EQ(size_t(0), col_off_in_bytes % sizeof(int64_t));
        col_off /= sizeof(int64_t);
      }
      executor->cgen_state_->emitExternalCall(
          "agg_count_distinct_array_" + numeric_type_name(elem_ti),
          llvm::Type::getVoidTy(LL_CONTEXT),
          {is_group_by
               ? LL_BUILDER.CreateGEP(std::get<0>(agg_out_ptr_w_idx), LL_INT(col_off))
               : agg_out_vec[slot_index],
           target_lvs[target_lv_idx],
           code_generator.posArg(arg_expr),
           elem_ti.is_fp()
               ? static_cast<llvm::Value*>(executor->cgen_state_->inlineFpNull(elem_ti))
               : static_cast<llvm::Value*>(
                     executor->cgen_state_->inlineIntNull(elem_ti))});
      ++slot_index;
      ++target_lv_idx;
      continue;
    }

    llvm::Value* agg_col_ptr{nullptr};
    const auto chosen_bytes =
        static_cast<size_t>(query_mem_desc.getPaddedSlotWidthBytes(slot_index));
    const auto& chosen_type = get_compact_type(target_info);
    const auto& arg_type =
        ((arg_expr && arg_expr->get_type_info().get_type() != kNULLT) &&
         !target_info.is_distinct)
            ? target_info.agg_arg_type
            : target_info.sql_type;
    const bool is_fp_arg =
        !lazy_fetched && arg_type.get_type() != kNULLT && arg_type.is_fp();
    if (is_group_by) {
      agg_col_ptr = group_by_and_agg->codegenAggColumnPtr(output_buffer_byte_stream,
                                                          out_row_idx,
                                                          agg_out_ptr_w_idx,
                                                          query_mem_desc,
                                                          chosen_bytes,
                                                          slot_index,
                                                          target_idx);
      CHECK(agg_col_ptr);
      agg_col_ptr->setName("agg_col_ptr");
    }

    if (is_varlen_projection(target_expr, target_info.sql_type)) {
      UNREACHABLE();
    }

    const bool float_argument_input = takes_float_argument(target_info);
    const bool is_count_in_avg = target_info.agg_kind == kAVG && target_lv_idx == 1;
    // The count component of an average should never be compacted.
    const auto agg_chosen_bytes =
        float_argument_input && !is_count_in_avg ? sizeof(float) : chosen_bytes;
    if (float_argument_input) {
      CHECK_GE(chosen_bytes, sizeof(float));
    }

    auto target_lv = target_lvs[target_lv_idx];

    const auto need_skip_null = target_info.skip_null_val;

    if (need_skip_null && !is_agg_domain_range_equivalent(target_info.agg_kind)) {
      target_lv = group_by_and_agg->convertNullIfAny(arg_type, target_info, target_lv);
    } else if (is_fp_arg) {
      target_lv = executor->castToFP(target_lv, arg_type, target_info.sql_type);
    }
    if (!dynamic_cast<const Analyzer::AggExpr*>(target_expr) || arg_expr) {
      target_lv = executor->cgen_state_->castToTypeIn(target_lv, (agg_chosen_bytes << 3));
    }

    const bool is_simple_count_target = is_simple_count(target_info);
    llvm::Value* str_target_lv{nullptr};
    if (target_lvs.size() == 3) {
      // none encoding string
      str_target_lv = target_lvs.front();
    }
    std::vector<llvm::Value*> agg_args{
        executor->castToIntPtrTyIn((is_group_by ? agg_col_ptr : agg_out_vec[slot_index]),
                                   (agg_chosen_bytes << 3)),
        (is_simple_count_target && !arg_expr)
            ? (agg_chosen_bytes == sizeof(int32_t) ? LL_INT(int32_t(0))
                                                   : LL_INT(int64_t(0)))
            : (is_simple_count_target && arg_expr && str_target_lv ? str_target_lv
                                                                   : target_lv)};
    if (query_mem_desc.isLogicalSizedColumnsAllowed()) {
      if (is_simple_count_target && arg_expr && str_target_lv) {
        agg_args[1] =
            agg_chosen_bytes == sizeof(int32_t) ? LL_INT(int32_t(0)) : LL_INT(int64_t(0));
      }
    }
    std::string agg_fname{agg_base_name};
    if (is_fp_arg) {
      if (!lazy_fetched) {
        if (agg_chosen_bytes == sizeof(float)) {
          CHECK_EQ(arg_type.get_type(), kFLOAT);
          agg_fname += "_float";
        } else {
          CHECK_EQ(agg_chosen_bytes, sizeof(double));
          agg_fname += "_double";
        }
      }
    } else if (agg_chosen_bytes == sizeof(int32_t)) {
      agg_fname += "_int32";
    } else if (agg_chosen_bytes == sizeof(int16_t) &&
               query_mem_desc.didOutputColumnar()) {
      agg_fname += "_int16";
    } else if (agg_chosen_bytes == sizeof(int8_t) && query_mem_desc.didOutputColumnar()) {
      agg_fname += "_int8";
    }

    if (is_distinct_target(target_info)) {
      CHECK_EQ(agg_chosen_bytes, sizeof(int64_t));
      CHECK(!chosen_type.is_fp());
      group_by_and_agg->codegenCountDistinct(
          target_idx, target_expr, agg_args, query_mem_desc);
    } else if (target_info.agg_kind == kAPPROX_QUANTILE) {
      CHECK_EQ(agg_chosen_bytes, sizeof(int64_t));
      group_by_and_agg->codegenApproxQuantile(
          target_idx, target_expr, agg_args, query_mem_desc);
    } else {
      const auto& arg_ti = target_info.agg_arg_type;
      if (need_skip_null) {
        agg_fname += "_skip_val";
      }

      if (target_info.agg_kind == kSINGLE_VALUE || need_skip_null) {
        llvm::Value* null_in_lv{nullptr};
        if (arg_ti.is_fp()) {
          null_in_lv =
              static_cast<llvm::Value*>(executor->cgen_state_->inlineFpNull(arg_ti));
        } else {
          null_in_lv = static_cast<llvm::Value*>(executor->cgen_state_->inlineIntNull(
              is_agg_domain_range_equivalent(target_info.agg_kind)
                  ? arg_ti
                  : target_info.sql_type));
        }
        CHECK(null_in_lv);
        auto null_lv =
            executor->cgen_state_->castToTypeIn(null_in_lv, (agg_chosen_bytes << 3));
        agg_args.push_back(null_lv);
      }
      if (!target_info.is_distinct) {
        auto agg_fname_call_ret_lv = group_by_and_agg->emitCall(agg_fname, agg_args);

        if (agg_fname.find("checked") != std::string::npos) {
          group_by_and_agg->checkErrorCode(agg_fname_call_ret_lv);
        }
      }
    }
    const auto window_func = dynamic_cast<const Analyzer::WindowFunction*>(target_expr);
    if (window_func && window_function_requires_peer_handling(window_func)) {
      const auto window_func_context =
          WindowProjectNodeContext::getActiveWindowFunctionContext(executor);
      const auto pending_outputs =
          LL_INT(window_func_context->aggregateStatePendingOutputs());
      executor->cgen_state_->emitExternalCall("add_window_pending_output",
                                              llvm::Type::getVoidTy(LL_CONTEXT),
                                              {agg_args.front(), pending_outputs});
      const auto& window_func_ti = window_func->get_type_info();
      std::string apply_window_pending_outputs_name = "apply_window_pending_outputs";
      switch (window_func_ti.get_type()) {
        case kFLOAT: {
          apply_window_pending_outputs_name += "_float";
          if (query_mem_desc.didOutputColumnar()) {
            apply_window_pending_outputs_name += "_columnar";
          }
          break;
        }
        case kDOUBLE: {
          apply_window_pending_outputs_name += "_double";
          break;
        }
        default: {
          apply_window_pending_outputs_name += "_int";
          if (query_mem_desc.didOutputColumnar()) {
            apply_window_pending_outputs_name +=
                std::to_string(window_func_ti.get_size() * 8);
          } else {
            apply_window_pending_outputs_name += "64";
          }
          break;
        }
      }
      const auto partition_end =
          LL_INT(reinterpret_cast<int64_t>(window_func_context->partitionEnd()));
      executor->cgen_state_->emitExternalCall(apply_window_pending_outputs_name,
                                              llvm::Type::getVoidTy(LL_CONTEXT),
                                              {pending_outputs,
                                               target_lvs.front(),
                                               partition_end,
                                               code_generator.posArg(nullptr)});
    }

    ++slot_index;
    ++target_lv_idx;
  }
}

void TargetExprCodegenBuilder::operator()(const Analyzer::Expr* target_expr,
                                          const Executor* executor,
                                          const CompilationOptions& co) {
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());
  if (query_mem_desc.getPaddedSlotWidthBytes(slot_index_counter) == 0) {
    CHECK(!dynamic_cast<const Analyzer::AggExpr*>(target_expr));
    ++slot_index_counter;
    ++target_index_counter;
    return;
  }
  if (dynamic_cast<const Analyzer::UOper*>(target_expr) &&
      static_cast<const Analyzer::UOper*>(target_expr)->get_optype() == kUNNEST) {
    CIDER_THROW(CiderCompileException,
                "UNNEST not supported in the projection list yet.");
  }
  if ((executor->plan_state_->isLazyFetchColumn(target_expr) || !is_group_by) &&
      (static_cast<size_t>(query_mem_desc.getPaddedSlotWidthBytes(slot_index_counter)) <
       sizeof(int64_t)) &&
      !is_columnar_projection(query_mem_desc)) {
    // TODO(miyu): enable different byte width in the layout w/o padding
    CIDER_THROW(CiderCompileException, "Retry query compilation with no compaction.");
  }

  auto target_info = get_target_info(target_expr, g_bigint_count);
  auto arg_expr = agg_arg(target_expr);
  if (arg_expr) {
    if (target_info.agg_kind == kSINGLE_VALUE || target_info.agg_kind == kSAMPLE ||
        target_info.agg_kind == kAPPROX_QUANTILE) {
      target_info.skip_null_val = false;
    } else if (query_mem_desc.getQueryDescriptionType() ==
                   QueryDescriptionType::NonGroupedAggregate &&
               !arg_expr->get_type_info().is_varlen() && !co.use_cider_data_format) {
      // TODO: COUNT is currently not null-aware for varlen types. Need to add proper code
      // generation for handling varlen nulls.
      target_info.skip_null_val = true;
    } else if (constrained_not_null(arg_expr, ra_exe_unit.quals)) {
      target_info.skip_null_val = false;
    }
  } else if (co.use_cider_data_format) {
    // Keep notnull information for Cider data format.
    target_info.skip_null_val = !target_expr->get_type_info().get_notnull();
  }

  target_exprs_to_codegen.emplace_back(
      target_expr, target_info, slot_index_counter, target_index_counter++, is_group_by);

  const auto agg_fn_names = agg_fn_base_names(
      target_info, is_varlen_projection(target_expr, target_info.sql_type));
  slot_index_counter += agg_fn_names.size();
}

void TargetExprCodegenBuilder::codegen(
    GroupByAndAggregate* group_by_and_agg,
    Executor* executor,
    const QueryMemoryDescriptor& query_mem_desc,
    const CompilationOptions& co,
    const std::tuple<llvm::Value*, llvm::Value*>& agg_out_ptr_w_idx,
    const std::vector<llvm::Value*>& agg_out_vec,
    llvm::Value* output_buffer_byte_stream,
    llvm::Value* out_row_idx,
    llvm::Value* varlen_output_buffer,
    DiamondCodegen& diamond_codegen) const {
  CHECK(group_by_and_agg);
  CHECK(executor);
  AUTOMATIC_IR_METADATA(executor->cgen_state_.get());

  for (const auto& target_expr_codegen : target_exprs_to_codegen) {
    if (co.use_cider_data_format) {
      target_expr_codegen.codegen(
          group_by_and_agg, executor, query_mem_desc, agg_out_ptr_w_idx, agg_out_vec, co);
    } else {
      target_expr_codegen.codegen(group_by_and_agg,
                                  executor,
                                  query_mem_desc,
                                  co,
                                  agg_out_ptr_w_idx,
                                  agg_out_vec,
                                  output_buffer_byte_stream,
                                  out_row_idx,
                                  varlen_output_buffer,
                                  diamond_codegen);
    }
  }
}
