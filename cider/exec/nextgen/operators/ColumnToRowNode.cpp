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

#include "exec/nextgen/operators/ColumnToRowNode.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/utils/TypeUtils.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace cider::jitlib;

class ColumnDecoder {
 public:
  ColumnDecoder(context::CodegenContext& ctx, ExprPtr& expr, JITValuePointer& index)
      : context_(ctx), expr_(expr), index_(index) {}

  void decode() {
    switch (expr_->get_type_info().get_type()) {
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
        decodeFixSizedType();
      default:
        LOG(FATAL) << "Unsupported data type in ColumnDecoder: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void decodeFixSizedType() {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::FixSizeJITExprValue fixsize_values(buffers);

    auto& func = batch->getParentJITFunction();
    JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
    // data buffer decoder
    auto data_pointer = fixsize_values.getValue()->castPointerSubType(tag);
    auto row_data = data_pointer[index_];

    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value<JITExprValueType::ROW>(nullptr, row_data);
    } else {
      // null buffer decoder
      // TODO: Difference between input array and intermediate array.
      // bool is_input_array = dynamic_cast<Analyzer::ColumnVar*>(expr_.get());
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_values.getNull().get(), index_.get()}}});

      expr_->set_expr_value<JITExprValueType::ROW>(row_null_data, row_data);
    }
  }

 private:
  context::CodegenContext& context_;
  ExprPtr& expr_;
  JITValuePointer& index_;
};

void ColumnToRowTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ColumnToRowTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = op_node_->getOutputExprs();
  ExprPtrVector& inputs = exprs;

  // for row loop
  // prototype:void func(CodegenContext* context, ArrowArray* in, ArrowArray* out);
  auto arrow_pointer = func->getArgument(1);
  auto index = func->createVariable(JITTypeTag::INT64, "index");
  index = func->createConstant(JITTypeTag::INT64, 0l);

  // len means rows length
  auto len = context::codegen_utils::getArrowArrayLength(
      context.getArrowArrayValues(inputs.front()->getLocalIndex()).first);

  func->createLoopBuilder()
      ->condition([&index, &len]() { return index < len; })
      ->loop([&]() {
        for (auto& input : inputs) {
          ColumnDecoder(context, input, index).decode();
        }
        successor_->consume(context);
      })
      ->update([&index]() { index = index + 1l; })
      ->build();
}
}  // namespace cider::exec::nextgen::operators
