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

#include "exec/nextgen/operators/RowToColumnNode.h"

#include "cider/CiderOptions.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/utils/JITExprValue.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;
using namespace context;

class ColumnWriter {
 public:
  ColumnWriter(context::CodegenContext& ctx,
               ExprPtr& expr,
               JITValuePointer& index,
               JITValuePointer& arrow_array_len)
      : context_(ctx)
      , expr_(expr)
      , arrow_array_(getArrowArrayFromCTX())
      , buffers_(getArrowArrayBuffersFromCTX())
      , index_(index)
      , arrow_array_len_(arrow_array_len) {}

  void write(bool for_null) {
    switch (expr_->get_type_info().get_type()) {
      case kBOOLEAN:
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
      case kFLOAT:
      case kDOUBLE:
      case kDATE:
      case kTIMESTAMP:
      case kTIME:
      case kDECIMAL:
        writeFixSizedTypeCol(for_null);
        break;
      case kVARCHAR:
      case kCHAR:
      case kTEXT:
        writeVariableSizeTypeCol(for_null);
        break;
      default:
        LOG(ERROR) << "Unsupported data type in ColumnWriter: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void writeVariableSizeTypeCol(bool for_null) {
    // Get values need to write
    utils::VarSizeJITExprValue values(expr_->get_expr_value());

    if (for_null) {
      auto null = setNullBuffer(values.getNull(), for_null);
      Analyzer::JITExprValueAdaptor(
          context_.getArrowArrayValues(expr_->getLocalIndex()).second)
          .setNull(null);
      return;
    }

    // Allocate buffer
    // offset, need array_len + 1 element.
    auto raw_length_buffer = context_.getJITFunction()->createLocalJITValue([this]() {
      auto bytes = (arrow_array_len_ + 1) *
                   context_.getJITFunction()->createLiteral(JITTypeTag::INT64, 4);
      return allocateRawDataBuffer(1, bytes);
    });

    auto actual_raw_length_buffer =
        raw_length_buffer->castPointerSubType(JITTypeTag::INT32);
    auto ifBuilder = context_.getJITFunction()->createIfBuilder();
    ifBuilder->condition([&values]() { return values.getNull(); })
        ->ifTrue([&]() {
          actual_raw_length_buffer[index_ + 1] = actual_raw_length_buffer[index_];
        })
        ->ifFalse([&]() {
          actual_raw_length_buffer[index_ + 1] =
              actual_raw_length_buffer[index_] + *values.getLength();
        })
        ->build();

    // get latest pointer to data buffer, will allocate data buffer on first call,
    // and will reallocate buffer if more capacity is needed
    auto raw_data_buffer = context_.getJITFunction()->emitRuntimeFunctionCall(
        "get_data_buffer_with_realloc_on_demand",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .ret_sub_type = JITTypeTag::INT8,
            .params_vector = {arrow_array_.get(),
                              actual_raw_length_buffer[index_].get()}});
    auto actual_raw_data_buffer = raw_data_buffer->castPointerSubType(JITTypeTag::INT8);
    auto cur_pointer = actual_raw_data_buffer + actual_raw_length_buffer[index_];

    context_.getJITFunction()->emitRuntimeFunctionCall(
        "do_memcpy",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::VOID,
            .params_vector = {
                cur_pointer.get(), values.getValue().get(), values.getLength().get()}});

    // Save JITValues of output buffers to corresponding exprs.
    buffers_.clear();
    buffers_.append(
        setNullBuffer(values.getNull(), for_null), raw_length_buffer, raw_data_buffer);
  }

  void writeFixSizedTypeCol(bool for_null) {
    // Get values need to write
    utils::FixSizeJITExprValue values(expr_->get_expr_value());

    if (for_null) {
      auto null = setNullBuffer(values.getNull(), for_null);
      Analyzer::JITExprValueAdaptor(
          context_.getArrowArrayValues(expr_->getLocalIndex()).second)
          .setNull(null);
      return;
    }

    // Allocate buffer.
    // Write value
    auto raw_data_buffer = setFixSizeRawData(values);

    // Save JITValues of output buffers to corresponding exprs.
    buffers_.clear();
    buffers_.append(setNullBuffer(values.getNull(), for_null), raw_data_buffer);
  }

  JITValuePointer setFixSizeRawData(utils::FixSizeJITExprValue& fixsize_val) {
    if (expr_->get_type_info().get_type() == kBOOLEAN) {
      auto raw_data_buffer = context_.getJITFunction()->createLocalJITValue(
          [this]() { return allocateBitwiseBuffer(1); });
      // leverage existing set_null_vector but need opposite value as input
      // TODO: (yma11) need check in UT
      std::string fname = "set_null_vector_bit";
      if (context_.getCodegenOptions().set_null_bit_vector_opt) {
        fname = "set_null_vector_bit_opt";
      }
      context_.getJITFunction()->emitRuntimeFunctionCall(
          fname,
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::VOID,
              .params_vector = {{raw_data_buffer.get(),
                                 index_.get(),
                                 (!fixsize_val.getValue()).get()}}});
      return raw_data_buffer;
    } else {
      auto raw_data_buffer = context_.getJITFunction()->createLocalJITValue([this]() {
        return allocateRawDataBuffer(1, expr_->get_type_info().get_type());
      });
      // Write value
      auto actual_raw_data_buffer = raw_data_buffer->castPointerSubType(
          utils::getJITTypeTag(expr_->get_type_info().get_type()));
      actual_raw_data_buffer[index_] = *fixsize_val.getValue();
      return raw_data_buffer;
    }
  }

  JITValuePointer setNullBuffer(JITValuePointer& null_val, bool for_null) {
    // the null_buffer, raw_data_buffer not used anymore.
    // so it doesn't matter whether null_buffer is nullptr
    // or constant false.
    auto null_buffer = JITValuePointer(nullptr);
    if ((!FLAGS_null_separate || for_null) && !expr_->get_type_info().get_notnull()) {
      // TBD: Null representation, bit-array or bool-array.
      null_buffer.replace(context_.getJITFunction()->createLocalJITValue(
          [this]() { return allocateBitwiseBuffer(0); }));
      if (for_null) {
        *null_buffer[index_] = null_val;
        return null_buffer;
      }

      std::string fname = "set_null_vector_bit";
      if (context_.getCodegenOptions().set_null_bit_vector_opt) {
        fname = "set_null_vector_bit_opt";
      }
      context_.getJITFunction()->emitRuntimeFunctionCall(
          fname,
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::VOID,
              .params_vector = {{null_buffer.get(), index_.get(), null_val.get()}}});
    }
    return null_buffer;
  }

 private:
  JITValuePointer allocateBitwiseBuffer(int64_t index = 0) {
    return allocateRawDataBuffer(index, SQLTypes::kTINYINT);
  }

  JITValuePointer allocateRawDataBuffer(int64_t index, SQLTypes type) {
    return codegen_utils::allocateArrowArrayBuffer(
        arrow_array_, index, arrow_array_len_, type);
  }

  JITValuePointer allocateRawDataBuffer(int64_t index, jitlib::JITValuePointer& bytes) {
    return codegen_utils::allocateArrowArrayBuffer(arrow_array_, index, bytes);
  }

  JITValuePointer& getArrowArrayFromCTX() {
    size_t local_index = expr_->getLocalIndex();
    CHECK(local_index);
    auto& values = context_.getArrowArrayValues(local_index);
    return values.first;
  }

  utils::JITExprValue& getArrowArrayBuffersFromCTX() {
    size_t local_index = expr_->getLocalIndex();
    CHECK(local_index);
    auto& values = context_.getArrowArrayValues(local_index);
    return values.second;
  }

 private:
  context::CodegenContext& context_;
  ExprPtr& expr_;
  JITValuePointer& arrow_array_;
  utils::JITExprValue& buffers_;
  JITValuePointer& index_;
  JITValuePointer& arrow_array_len_;
};

TranslatorPtr RowToColumnNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<RowToColumnTranslator>(shared_from_this(), succ);
}

void RowToColumnTranslator::consume(context::CodegenContext& context) {
  for_null_ = false;
  codegen(context, [this](context::CodegenContext& context) {
    if (successor_) {
      successor_->consume(context);
    }
  });
}

void RowToColumnTranslator::consumeNull(context::CodegenContext& context) {
  for_null_ = true;
  codegen(context, [this](context::CodegenContext& context) {
    if (successor_) {
      successor_->consumeNull(context);
    }
  });
}

void RowToColumnTranslator::codegenImpl(SuccessorEmitter successor_wrapper,
                                        context::CodegenContext& context,
                                        void* successor) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = node_->getOutputExprs();
  ExprPtrVector& output_exprs = exprs;

  auto output_index = func->createVariable(JITTypeTag::INT64, "output_index", 0);

  // Get input ArrowArray length from previous C2RNode
  auto prev_c2r_node = static_cast<RowToColumnNode*>(node_.get())->getColumnToRowNode();
  auto input_array_len = prev_c2r_node->getColumnRowNum();

  for (int64_t i = 0; i < exprs.size(); ++i) {
    ExprPtr& expr = exprs[i];
    ColumnWriter writer(context, expr, output_index, input_array_len);
    writer.write(for_null_);
  }
  // Update index
  output_index = output_index + 1;

  successor_wrapper(successor, context);

  if (for_null_) {
    return;
  }

  // Execute length field updating build function after C2R loop finished.
  prev_c2r_node->registerDeferFunc([output_index, &output_exprs, &context]() mutable {
    for (auto& expr : output_exprs) {
      size_t local_offset = expr->getLocalIndex();
      CHECK_NE(local_offset, 0);

      auto&& [arrow_array, _] = context.getArrowArrayValues(local_offset);
      codegen_utils::setArrowArrayLength(arrow_array, output_index);
    }
  });
}

}  // namespace cider::exec::nextgen::operators
