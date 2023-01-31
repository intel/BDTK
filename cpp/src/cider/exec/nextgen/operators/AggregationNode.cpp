/*
 * Copyright (c) 2022 Intel Corporation.
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

#include "exec/nextgen/operators/AggregationNode.h"

namespace cider::exec::nextgen::operators {
TranslatorPtr AggNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<AggTranslator>(shared_from_this(), succ);
}

void AggTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void outputNullableCheck(const Analyzer::AggExpr* agg_expr, context::AggExprsInfo& info) {
  if (info.agg_type_ == SQLAgg::kCOUNT) {
    info.setNotNull(true);
    return;
  }

  bool input_notnull = agg_expr->get_arg()->get_type_info().get_notnull();
  if (input_notnull != info.sql_type_info_.get_notnull()) {
    LOG(WARNING) << "output nullable info is not same with input, so enforce consistency "
                    "with input, which is: "
                 << (input_notnull ? "not null." : "nullable.");
    info.setNotNull(input_notnull);
  }
}

context::AggExprsInfoVector initExpersInfo(ExprPtrVector& exprs) {
  context::AggExprsInfoVector infos;
  int8_t start_addr = 0;
  for (const auto& expr : exprs) {
    auto agg_expr = dynamic_cast<const Analyzer::AggExpr*>(expr.get());
    infos.emplace_back(agg_expr->get_type_info(), agg_expr->get_aggtype(), start_addr);
    outputNullableCheck(agg_expr, infos.back());
    start_addr += expr->get_type_info().get_size();
  }
  return infos;
}

template <typename TYPE>
void makeSumInitialValue(int8_t* value_addr, int8_t offset) {
  auto cast_memory = reinterpret_cast<TYPE*>(value_addr + offset);
  *cast_memory = 0;
}

template <typename TYPE>
void makeMinInitialValue(int8_t* value_addr, int8_t offset) {
  auto cast_memory = reinterpret_cast<TYPE*>(value_addr + offset);
  *cast_memory = std::numeric_limits<TYPE>::max();
}

template <typename TYPE>
void makeMaxInitialValue(int8_t* value_addr, int8_t offset) {
  auto cast_memory = reinterpret_cast<TYPE*>(value_addr + offset);
  *cast_memory = std::numeric_limits<TYPE>::min();
}

std::vector<int8_t> initOriginValue(context::AggExprsInfoVector& exprs_info) {
  std::vector<int8_t> origin_vector(exprs_info.back().start_offset_ +
                                    exprs_info.back().sql_type_info_.get_size() +
                                    exprs_info.size());
  int8_t* raw_memory = origin_vector.data();
  for (const auto& info : exprs_info) {
    switch (info.agg_type_) {
      case SQLAgg::kSUM:
      case SQLAgg::kCOUNT: {
        switch (info.sql_type_info_.get_size()) {
          case 1:
            makeSumInitialValue<int8_t>(raw_memory, info.start_offset_);
            break;
          case 2:
            makeSumInitialValue<int16_t>(raw_memory, info.start_offset_);
            break;
          case 4:
            makeSumInitialValue<int32_t>(raw_memory, info.start_offset_);
            break;
          case 8:
            makeSumInitialValue<int64_t>(raw_memory, info.start_offset_);
            break;
          default:
            LOG(ERROR) << info.sql_type_info_.get_size()
                       << " size is not support for sum/count yet";
            break;
        }
        break;
      }
      case SQLAgg::kMIN: {
        switch (info.sql_type_info_.get_size()) {
          case 1:
            makeMinInitialValue<int8_t>(raw_memory, info.start_offset_);
            break;
          case 2:
            makeMinInitialValue<int16_t>(raw_memory, info.start_offset_);
            break;
          case 4:
            makeMinInitialValue<int32_t>(raw_memory, info.start_offset_);
            break;
          case 8:
            makeMinInitialValue<int64_t>(raw_memory, info.start_offset_);
            break;
          default:
            LOG(ERROR) << info.sql_type_info_.get_size()
                       << " size is not support for min yet";
            break;
        }
        break;
      }
      case SQLAgg::kMAX: {
        switch (info.sql_type_info_.get_size()) {
          case 1:
            makeMaxInitialValue<int8_t>(raw_memory, info.start_offset_);
            break;
          case 2:
            makeMaxInitialValue<int16_t>(raw_memory, info.start_offset_);
            break;
          case 4:
            makeMaxInitialValue<int32_t>(raw_memory, info.start_offset_);
            break;
          case 8:
            makeMaxInitialValue<int64_t>(raw_memory, info.start_offset_);
            break;
          default:
            LOG(ERROR) << info.sql_type_info_.get_size()
                       << " size is not support for max yet";
            break;
        }
        break;
      }
      default:
        LOG(ERROR) << "Agg function is not supported yet";
        break;
    }
  }
  // init null value (1--null, 0--not null)
  auto null_buffer_offset =
      exprs_info.back().start_offset_ + exprs_info.back().sql_type_info_.get_size();
  for (size_t i = 0; i < exprs_info.size(); i++) {
    exprs_info[i].null_offset_ = null_buffer_offset + i;
    auto null_value = reinterpret_cast<int8_t*>(raw_memory + exprs_info[i].null_offset_);
    *null_value = exprs_info[i].sql_type_info_.get_notnull() ? 0 : 1;
  }
  return origin_vector;
}

void AggTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();

  auto&& [_, exprs] = node_->getOutputExprs();

  // arrange buffer initail info
  context::AggExprsInfoVector exprs_info = initExpersInfo(exprs);

  std::vector<int8_t> origin_value = initOriginValue(exprs_info);

  // Groupby
  // TODO(Yanting): support group-by
  auto agg_node = dynamic_cast<AggNode*>(node_.get());
  auto& groupby_exprs = agg_node->getGroupByExprs();
  if (groupby_exprs.size() != 0) {
    LOG(ERROR) << "group-by is not supported now.";
  }

  ExprPtrVector& output_exprs = exprs;
  auto batch = context.registerBatch(SQLTypeInfo(kSTRUCT, false, [&output_exprs]() {
    std::vector<SQLTypeInfo> output_types;
    output_types.reserve(output_exprs.size());
    for (auto& expr : output_exprs) {
      output_types.emplace_back(expr->get_type_info());
    }
    return output_types;
  }()));

  // non-groupby Agg
  auto buffer =
      context.registerBuffer(origin_value.size(),
                             exprs_info,
                             "output_buffer",
                             [origin_value](context::Buffer* buf) {
                               auto raw_buf = buf->getBuffer();
                               memcpy(raw_buf, origin_value.data(), buf->getCapacity());
                             });

  int32_t current_expr_idx = 0;
  for (auto& expr : exprs) {
    auto agg_expr = dynamic_cast<Analyzer::AggExpr*>(expr.get());

    auto cast_buffer = buffer->castPointerSubType(jitlib::JITTypeTag::INT8);
    auto val_addr_initial = cast_buffer + exprs_info[current_expr_idx].start_offset_;
    auto val_addr = val_addr_initial->castPointerSubType(
        exprs_info[current_expr_idx].jit_value_type_);

    // count(*/1) and count(col) are different
    // The former will count all input rows and dont have argument in expression,
    // But the latter only counts not-null rows and need to refer argument info.
    if (exprs_info[current_expr_idx].agg_type_ == SQLAgg::kCOUNT) {
      if (!agg_expr->get_arg() || agg_expr->get_arg()->get_type_info().get_notnull()) {
        func->emitRuntimeFunctionCall(
            exprs_info[current_expr_idx].agg_name_,
            jitlib::JITFunctionEmitDescriptor{
                .ret_type = exprs_info[current_expr_idx].jit_value_type_,
                .params_vector = {val_addr.get()}});
      } else {
        utils::FixSizeJITExprValue values(agg_expr->get_arg()->get_expr_value());
        auto null_addr = cast_buffer + exprs_info[current_expr_idx].null_offset_;
        func->emitRuntimeFunctionCall(
            exprs_info[current_expr_idx].agg_name_ + "_nullable",
            jitlib::JITFunctionEmitDescriptor{
                .ret_type = exprs_info[current_expr_idx].jit_value_type_,
                .params_vector = {
                    val_addr.get(), null_addr.get(), values.getNull().get()}});
      }
      current_expr_idx += 1;
      continue;
    }

    // for other agg function
    utils::FixSizeJITExprValue values(agg_expr->get_arg()->codegen(context));

    if (agg_expr->get_arg()->get_type_info().get_notnull()) {
      func->emitRuntimeFunctionCall(
          exprs_info[current_expr_idx].agg_name_,
          jitlib::JITFunctionEmitDescriptor{
              .ret_type = exprs_info[current_expr_idx].jit_value_type_,
              .params_vector = {val_addr.get(), values.getValue().get()}});
    } else {
      auto null_addr = cast_buffer + exprs_info[current_expr_idx].null_offset_;
      func->emitRuntimeFunctionCall(
          exprs_info[current_expr_idx].agg_name_ + "_nullable",
          jitlib::JITFunctionEmitDescriptor{
              .ret_type = exprs_info[current_expr_idx].jit_value_type_,
              .params_vector = {val_addr.get(),
                                values.getValue().get(),
                                null_addr.get(),
                                values.getNull().get()}});
    }
    current_expr_idx += 1;
  }
}

}  // namespace cider::exec::nextgen::operators
