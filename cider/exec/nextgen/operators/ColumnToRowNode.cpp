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

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/operators/OpNode.h"
#include "exec/nextgen/utils/TypeUtils.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace cider::jitlib;

class ColumnReader {
 public:
  ColumnReader(context::CodegenContext& ctx, ExprPtr& expr, JITValuePointer& index)
      : context_(ctx), expr_(expr), index_(index) {}

  void read() {
    switch (expr_->get_type_info().get_type()) {
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
        readFixSizedTypeCol();
        break;
      default:
        LOG(FATAL) << "Unsupported data type in ColumnReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void readFixSizedTypeCol() {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::FixSizeJITExprValue fixsize_values(buffers);

    auto& func = batch->getParentJITFunction();
    auto row_data = getFixSizeRowData(func, fixsize_values);
    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createConstant(JITTypeTag::BOOL, false), row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_values.getNull().get(), index_.get()}}});

      expr_->set_expr_value(row_null_data, row_data);
    }
  }

  JITValuePointer getFixSizeRowData(JITFunction& func,
                                    utils::FixSizeJITExprValue& fixsize_val) {
    if (expr_->get_type_info().get_type() == kBOOLEAN) {
      auto row_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_set",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_val.getValue().get(), index_.get()}}});
      return row_data;
    } else {
      JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
      // data buffer decoder
      auto data_pointer = fixsize_val.getValue()->castPointerSubType(tag);
      auto row_data = data_pointer[index_];
      return row_data;
    }
  }

 private:
  context::CodegenContext& context_;
  ExprPtr& expr_;
  JITValuePointer& index_;
};

TranslatorPtr ColumnToRowNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<ColumnToRowTranslator>(shared_from_this(), succ);
}

void ColumnToRowTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ColumnToRowTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = node_->getOutputExprs();
  ExprPtrVector& inputs = exprs;

  // for row loop
  auto index = func->createVariable(JITTypeTag::INT64, "index", 0);

  // input column rows num.
  auto len = func->createLocalJITValue([&context, &inputs]() {
    return context::codegen_utils::getArrowArrayLength(
        context.getArrowArrayValues(inputs.front()->getLocalIndex()).first);
  });
  static_cast<ColumnToRowNode*>(node_.get())->setColumnRowNum(len);

  func->createLoopBuilder()
      ->condition([&index, &len]() { return index < len; })
      ->loop([&]() {
        for (auto& input : inputs) {
          ColumnReader(context, input, index).read();
        }
        successor_->consume(context);
      })
      ->update([&index]() { index = index + 1l; })
      ->build();

  // Execute defer build functions.
  auto c2r_node = static_cast<ColumnToRowNode*>(node_.get());
  for (auto& defer_func : c2r_node->getDeferFunctions()) {
    defer_func();
  }
}
}  // namespace cider::exec::nextgen::operators
