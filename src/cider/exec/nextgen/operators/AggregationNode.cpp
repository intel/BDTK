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
static std::unordered_map<SQLTypes, std::string> sql_type_name = {
    {kNULLT, "null"},
    {kTINYINT, "int8"},
    {kSMALLINT, "int16"},
    {kINT, "int32"},
    {kBIGINT, "int64"},
    // not supported
    //  {SQLTypes::kFLOAT, "float"},
    //  {SQLTypes::kDOUBLE, "double"},
    //  {SQLTypes::kDATE, "date"},
    //  {SQLTypes::kDECIMAL, "decimal"},
    //  {SQLTypes::kBOOLEAN, "bool"},
};

jitlib::JITTypeTag AggExprsInfo::getJitValueType(SQLTypes sql_type) {
  switch (sql_type) {
    case kTINYINT:
      return jitlib::JITTypeTag::INT8;
    case kSMALLINT:
      return jitlib::JITTypeTag::INT16;
    case kINT:
      return jitlib::JITTypeTag::INT32;
    case kBIGINT:
      return jitlib::JITTypeTag::INT64;
    case kFLOAT:
    case kDOUBLE:
    case kBOOLEAN:
    case kDATE:
    case kDECIMAL:
    default:
      return jitlib::JITTypeTag::INVALID;
  }
}

std::string AggExprsInfo::getAggName(SQLAgg agg_type, SQLTypes sql_type) {
  std::string agg_name = "nextgen_cider_agg";
  switch (agg_type) {
    case SQLAgg::kSUM: {
      agg_name = agg_name + "_sum_" + sql_type_name[sql_type];
      break;
    }
    default:
      LOG(FATAL) << "unsupport type";
      break;
  }
  return agg_name;
}

TranslatorPtr AggNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<AggTranslator>(shared_from_this(), succ);
}

void AggTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

AggExprsInfoVector initExpersInfo(ExprPtrVector& exprs) {
  AggExprsInfoVector infos;
  int32_t start_addr = 0;
  for (const auto& expr : exprs) {
    int32_t size = 0;
    auto agg_expr = dynamic_cast<const Analyzer::AggExpr*>(expr.get());
    // get value size in buffer
    switch (expr->get_type_info().get_size()) {
      case 1:
      case 2:
      case 4:
      case 8:
        size = 8;
        break;
      default:
        size = 8;
        break;
    }
    infos.emplace_back(
        agg_expr->get_type_info().get_type(), agg_expr->get_aggtype(), start_addr, size);
    start_addr += size;
  }
  return infos;
}

std::vector<int8_t> initOriginValue(AggExprsInfoVector& exprs_info) {
  std::vector<int8_t> origin_vector(exprs_info.back().start_offset_ +
                                    exprs_info.back().byte_size_);
  int8_t* raw_memory = origin_vector.data();
  for (const auto& info : exprs_info) {
    switch (info.agg_type_) {
      case SQLAgg::kSUM:
      case SQLAgg::kCOUNT: {
        switch (info.byte_size_) {
          case 8: {
            auto cast_memory =
                reinterpret_cast<int64_t*>(raw_memory + info.start_offset_);
            *cast_memory = 0;
            break;
          }
          default:
            LOG(FATAL) << info.byte_size_ << " size is not support for sum yet";
            break;
        }
        break;
      }
      default:
        LOG(FATAL) << "Agg function not support yet";
        break;
    }
  }
  return origin_vector;
}

void AggTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();

  auto&& [_, exprs] = node_->getOutputExprs();

  // arrange buffer initail info
  AggExprsInfoVector exprs_info = initExpersInfo(exprs);

  std::vector<int8_t> origin_value = initOriginValue(exprs_info);

  // Groupby
  // TODO(Yanting): support group-by
  auto agg_node = dynamic_cast<AggNode*>(node_.get());
  auto& groupby_exprs = agg_node->getGroupByExprs();
  if (groupby_exprs.size() != 0) {
    LOG(FATAL) << "group-by is not supported now.";
  }

  // non-groupby Agg
  auto buffer = context.registerBuffer(
      origin_value.size(), "output_buffer", [origin_value](context::Buffer* buf) {
        auto raw_buf = buf->getBuffer();
        memcpy(raw_buf, origin_value.data(), buf->getCapacity());
      });

  int32_t current_expr_idx = 0;
  for (const auto& expr : exprs) {
    auto agg_expr = dynamic_cast<const Analyzer::AggExpr*>(expr.get());
    utils::FixSizeJITExprValue values(agg_expr->get_arg()->get_expr_value());
    auto real_value = values.getValue();

    auto cast_buffer = buffer->castPointerSubType(jitlib::JITTypeTag::INT8);
    auto val_addr_initial = cast_buffer + exprs_info[current_expr_idx].start_offset_;
    auto val_addr = val_addr_initial->castPointerSubType(
        exprs_info[current_expr_idx].jit_value_type_);

    func->emitRuntimeFunctionCall(
        exprs_info[current_expr_idx].agg_name_,
        jitlib::JITFunctionEmitDescriptor{
            .ret_type = exprs_info[current_expr_idx].jit_value_type_,
            .params_vector = {val_addr.get(), real_value.get()}});
    current_expr_idx += 1;
  }
}

}  // namespace cider::exec::nextgen::operators
