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

  void read() {
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
        readFixSizedTypeCol();
        break;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        readVariableSizeTypeCol();
        break;
      case kARRAY:
        readVariableSizeArrayCol();
        break;
      default:
        LOG(ERROR) << "Unsupported data type in ColumnReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void readVariableSizeTypeCol() {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::VarSizeJITExprValue varsize_values(buffers);
    auto& func = batch->getParentJITFunction();
    auto dictionary =
        varsize_values.getDictionary()->castPointerSubType(JITTypeTag::INT8);

    JITValuePointer len = func.emitRuntimeFunctionCall(
        "get_str_length_from_dictionary_or_buffer",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT32,
            .params_vector = {{dictionary.get(),
                               index_.get(),
                               varsize_values.getLength()
                                   ->castPointerSubType(JITTypeTag::INT32)
                                   .get()}}});
    JITValuePointer row_data = func.emitRuntimeFunctionCall(
        "get_str_ptr_from_dictionary_or_buffer",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .params_vector = {
                {dictionary.get(),
                 index_.get(),
                 varsize_values.getLength()->castPointerSubType(JITTypeTag::INT32).get(),
                 varsize_values.getValue()
                     ->castPointerSubType(JITTypeTag::INT8)
                     .get()}}});

    if (expr_->get_type_info().get_notnull()) {
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

  //| BOOL     | INT64      | INT8*                |   C_TYPE*      |  INT64     |
  //|----------|------------|----------------------|----------------|------------|
  //| row_null | row_length | elem_null for column | values for row | row_offset |
  //
  // results for column like [[12, -7, 25], null, [0, -127, null, 50], []] are
  //|----------|------------|----------------------|----------------|------------|
  //| false    | 3          | 101111               | <12, -7, 25>   | 0          |
  //|----------|------------|----------------------|----------------|------------|
  //| true     | 0          | 101111               | <>             | 3          |
  //|----------|------------|----------------------|----------------|------------|
  //| false    | 4          | 101111               | <0, -127,0, 50>| 3          |
  //|----------|------------|----------------------|----------------|------------|
  //| false    | 0          | 101111               | <>             | 7          |
  void readVariableSizeArrayCol() {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::VarSizeArrayExprValue varsize_values(buffers);

    auto& func = batch->getParentJITFunction();

    // offset buffer
    auto offset_pointer =
        varsize_values.getLength()->castPointerSubType(JITTypeTag::INT32);
    auto cur_offset = offset_pointer[index_];
    auto len = offset_pointer[index_ + 1] - offset_pointer[index_];
    // data buffer
    auto value_pointer = varsize_values.getValue()->castPointerSubType(
        utils::getJITTypeTag(expr_->get_type_info().getChildAt(0).get_type()));
    auto row_data = value_pointer + cur_offset;
    // array elements null buffer
    auto col_null_pointer =
        varsize_values.getElemNull()->castPointerSubType(JITTypeTag::INT8);

    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false),
                            len,
                            col_null_pointer,
                            row_data,
                            cur_offset);
    } else {
      // null buffer decoder
      // TBD: Null representation, bit-array or bool-array.
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{varsize_values.getNull().get(), index_.get()}}});
      expr_->set_expr_value(row_null_data, len, col_null_pointer, row_data, cur_offset);
    }
  }

  void readFixSizedTypeCol() {
    auto&& [batch, buffers] = context_.getArrowArrayValues(expr_->getLocalIndex());
    utils::FixSizeJITExprValue fixsize_values(buffers);

    auto& func = batch->getParentJITFunction();

    auto row_data = getFixSizeRowData(func, fixsize_values);
    if (expr_->get_type_info().get_notnull()) {
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
  codegen(context,
          [this](context::CodegenContext& context) { successor_->consume(context); });
}

void ColumnToRowTranslator::codegenImpl(SuccessorEmitter successor_wrapper,
                                        context::CodegenContext& context,
                                        void* successor) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = node_->getOutputExprs();
  ExprPtrVector& inputs = exprs;

  // for row loop
  auto index = func->createVariable(JITTypeTag::INT64, "index", 0);
  auto len = context.getInputLength();
  static_cast<ColumnToRowNode*>(node_.get())->setColumnRowNum(len);

  func->createLoopBuilder()
      ->condition([&index, &len]() { return index < len; })
      ->loop([&](LoopBuilder*) {
        for (auto& input : inputs) {
          ColumnReader(context, input, index).read();
        }
        successor_wrapper(successor, context);
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
