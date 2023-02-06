/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "cider/CiderOptions.h"
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

  void read(bool for_null) {
    switch (expr_->get_type_info().get_type()) {
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
      case kDATE:
      case kTIME:
      case kTIMESTAMP:
      case kDECIMAL:
        readFixSizedTypeCol(for_null);
        break;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        readVariableSizeTypeCol(for_null);
        break;
      case kARRAY:
        readVariableSizeArrayCol(for_null);
      default:
        LOG(ERROR) << "Unsupported data type in ColumnReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void readVariableSizeTypeCol(bool for_null) {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::VarSizeJITExprValue varsize_values(buffers);

    auto& func = batch->getParentJITFunction();

    if (for_null) {
      if (expr_->get_type_info().get_notnull()) {
        expr_->set_expr_null(func.createLiteral(JITTypeTag::BOOL, false));
      } else {
        auto row_null_data = varsize_values.getNull()[index_];
        expr_->set_expr_null(row_null_data);
      }
      return;
    }

    // offset buffer
    auto offset_pointer =
        varsize_values.getLength()->castPointerSubType(JITTypeTag::INT32);
    auto len = offset_pointer[index_ + 1] - offset_pointer[index_];
    auto cur_offset = offset_pointer[index_];
    // data buffer
    auto value_pointer = varsize_values.getValue()->castPointerSubType(JITTypeTag::INT8);
    auto row_data = value_pointer + cur_offset;  // still char*

    if ((FLAGS_null_separate && !for_null) || expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), len, row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      std::string fname = "check_bit_vector_clear";
      if (context_.getCodegenOptions().check_bit_vector_clear_opt) {
        fname = "check_bit_vector_clear_opt";
      }
      auto row_null_data = func.emitRuntimeFunctionCall(
          fname,
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{varsize_values.getNull().get(), index_.get()}}});
      expr_->set_expr_value(row_null_data, len, row_data);
    }
  }

  void readVariableSizeArrayCol(bool for_null) {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::VarSizeArrayExprValue varsize_values(buffers);

    auto& func = batch->getParentJITFunction();

    if (for_null) {
      if (expr_->get_type_info().get_notnull()) {
        expr_->set_expr_null(func.createLiteral(JITTypeTag::BOOL, false));
      } else {
        auto row_null_data = varsize_values.getNull()[index_];
        expr_->set_expr_null(row_null_data);
      }
      return;
    }

    // offset buffer
    auto offset_pointer =
        varsize_values.getLength()->castPointerSubType(JITTypeTag::INT32);
    auto cur_offset = offset_pointer[index_];
    // data buffer
    auto value_pointer = JITValuePointer();
    switch (expr_->get_type_info().get_subtype()) {
      case kTINYINT:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::INT8));
        break;
      case kSMALLINT:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::INT16));
        break;
      case kINT:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::INT32));
        break;
      case kBIGINT:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::INT64));
        break;
      case kFLOAT:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::FLOAT));
        break;
      case kDOUBLE:
        value_pointer.replace(
            varsize_values.getValue()->castPointerSubType(JITTypeTag::DOUBLE));
        break;
      default:
        CIDER_THROW(CiderException, std::string("Unsupported list data type"));
        break;
    }
    auto row_data = value_pointer + cur_offset;
    // array elements null buffer
    auto elem_null_pointer =
        varsize_values.getElemNull()->castPointerSubType(JITTypeTag::INT8);

    if ((FLAGS_null_separate && !for_null) || expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false),
                            offset_pointer,
                            elem_null_pointer,
                            row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{varsize_values.getNull().get(), index_.get()}}});
      expr_->set_expr_value(row_null_data, offset_pointer, elem_null_pointer, row_data);
    }
  }

  void readFixSizedTypeCol(bool for_null) {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::FixSizeJITExprValue fixsize_values(buffers);

    auto& func = batch->getParentJITFunction();

    if (for_null) {
      if (expr_->get_type_info().get_notnull()) {
        expr_->set_expr_null(func.createLiteral(JITTypeTag::BOOL, false));
      } else {
        auto row_null_data = fixsize_values.getNull()[index_];
        expr_->set_expr_value(row_null_data);
      }
      return;
    }

    auto row_data = getFixSizeRowData(func, fixsize_values);
    if ((FLAGS_null_separate && !for_null) || expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), row_data);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      std::string fname = "check_bit_vector_clear";
      if (context_.getCodegenOptions().check_bit_vector_clear_opt) {
        fname = "check_bit_vector_clear_opt";
      }
      auto row_null_data = func.emitRuntimeFunctionCall(
          fname,
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
  for_null_ = false;
  codegen(context,
          [this](context::CodegenContext& context) { successor_->consume(context); });
}

void ColumnToRowTranslator::consumeNull(context::CodegenContext& context) {
  for_null_ = true;
  codegen(context,
          [this](context::CodegenContext& context) { successor_->consumeNull(context); });
}

void ColumnToRowTranslator::codegenImpl(SuccessorEmitter successor_wrapper,
                                        context::CodegenContext& context,
                                        void* successor) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = node_->getOutputExprs();
  ExprPtrVector& inputs = exprs;

  // for row loop
  auto index = func->createVariable(JITTypeTag::INT64, "index", 0);

  // get input row num from input arrow array
  // assumes signature be like: query_func(context, array)
  auto input_array = func->getArgument(1);  // array
  auto len = func->createLocalJITValue([&input_array]() {
    return context::codegen_utils::getArrowArrayLength(input_array);
  });
  if (!for_null_) {
    static_cast<ColumnToRowNode*>(node_.get())->setColumnRowNum(len);
  }
  auto idx_upper = func->createVariable(JITTypeTag::INT64, "idx_upper", len);
  if (for_null_) {
    // pack 8 bit
    idx_upper = len / 8 + 1;
  }

  func->createLoopBuilder()
      ->condition([&index, &idx_upper]() { return index < idx_upper; })
      ->loop([&](LoopBuilder*) {
        for (auto& input : inputs) {
          ColumnReader(context, input, index).read(for_null_);
        }
        successor_wrapper(successor, context);
      })
      ->update([&index]() { index = index + 1l; })
      ->build();

  if (for_null_) {
    return;
  }
  // Execute defer build functions.
  auto c2r_node = static_cast<ColumnToRowNode*>(node_.get());
  for (auto& defer_func : c2r_node->getDeferFunctions()) {
    defer_func();
  }
}
}  // namespace cider::exec::nextgen::operators
