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

#include "exec/nextgen/operators/RowToColumnNode.h"

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
               JITValuePointer& arrow_array,
               JITValuePointer& index,
               JITValuePointer& arrow_array_len)
      : context_(ctx)
      , expr_(expr)
      , arrow_array_(arrow_array)
      , index_(index)
      , arrow_array_len_(arrow_array_len) {}

  void write() {
    CHECK(0 == expr_->getLocalIndex());
    switch (expr_->get_type_info().get_type()) {
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
        writeFixSizedTypeCol();
        break;
      default:
        LOG(FATAL) << "Unsupported data type in ColumnWriter: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void writeFixSizedTypeCol() {
    // Get values need to write
    utils::FixSizeJITExprValue values(expr_->get_expr_value());

    // Allocate buffer.
    auto raw_data_buffer = context_.getJITFunction()->createLocalJITValue(
        [this]() { return allocateRawDataBuffer(1, expr_->get_type_info().get_type()); });
    auto null_buffer = JITValuePointer(nullptr);

    // Write value
    auto actual_raw_data_buffer = raw_data_buffer->castPointerSubType(
        utils::getJITTypeTag(expr_->get_type_info().get_type()));
    actual_raw_data_buffer[index_] = *values.getValue();

    if (!expr_->get_type_info().get_notnull()) {
      // TBD: Null representation, bit-array or bool-array.
      null_buffer.replace(context_.getJITFunction()->createLocalJITValue(
          [this]() { return allocateBitwiseBuffer(0); }));

      context_.getJITFunction()->emitRuntimeFunctionCall(
          "set_null_vector",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::VOID,
              .params_vector = {
                  {null_buffer.get(), index_.get(), values.getNull().get()}}});
    }

    // Register Output ArrowArray to CodegenContext
    size_t local_offset = context_.appendArrowArrayValues(
        arrow_array_,
        utils::JITExprValue(JITExprValueType::BATCH, null_buffer, raw_data_buffer));
    expr_->setLocalIndex(local_offset);
  }

 private:
  JITValuePointer allocateBitwiseBuffer(int64_t index = 0) {
    return allocateRawDataBuffer(index, SQLTypes::kTINYINT);
  }

  JITValuePointer allocateRawDataBuffer(int64_t index, SQLTypes type) {
    auto bytes = arrow_array_len_ * context_.getJITFunction()->createLiteral(
                                        JITTypeTag::INT64, utils::getTypeBytes(type));
    return codegen_utils::allocateArrowArrayBuffer(arrow_array_, index, bytes);
  }

 private:
  context::CodegenContext& context_;
  ExprPtr& expr_;
  JITValuePointer& arrow_array_;
  JITValuePointer& index_;
  JITValuePointer& arrow_array_len_;
};

TranslatorPtr RowToColumnNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<RowToColumnTranslator>(shared_from_this(), succ);
}

void RowToColumnTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void RowToColumnTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [type, exprs] = node_->getOutputExprs();
  ExprPtrVector& output_exprs = exprs;

  // construct output Batch
  auto output_arrow_array = context.registerBatch(
      SQLTypeInfo(kSTRUCT,
                  false,
                  [&output_exprs]() {
                    std::vector<SQLTypeInfo> output_types;
                    output_types.reserve(output_exprs.size());
                    for (auto& expr : output_exprs) {
                      output_types.emplace_back(expr->get_type_info());
                    }
                    return output_types;
                  }()),
      "output",
      true);

  // TODO (bigPYJ1151): Refactor after JITLib Refactor.
  auto output_index = func->createLocalJITValue([func]() {
    auto output_index = func->createVariable(JITTypeTag::INT64, "output_index");
    output_index = func->createLiteral(JITTypeTag::INT64, 0l);
    return output_index;
  });

  // Get input ArrowArray length from previous C2RNode
  auto prev_c2r_node = static_cast<RowToColumnNode*>(node_.get())->getColumnToRowNode();
  auto input_array_len = prev_c2r_node->getColumnRowNum();

  for (int64_t i = 0; i < exprs.size(); ++i) {
    ExprPtr& expr = exprs[i];

    auto child_arrow_array = func->createLocalJITValue([&output_arrow_array, &i]() {
      return codegen_utils::getArrowArrayChild(output_arrow_array, i);
    });

    // Write rows
    ColumnWriter writer(context, expr, child_arrow_array, output_index, input_array_len);
    writer.write();
  }
  // Update index
  output_index = output_index + 1;

  // Execute length field updating build function after C2R loop finished.
  prev_c2r_node->registerDeferFunc(
      [output_index, output_arrow_array, &output_exprs, &context]() mutable {
        codegen_utils::setArrowArrayLength(output_arrow_array, output_index);
        for (auto& expr : output_exprs) {
          size_t local_offset = expr->getLocalIndex();
          CHECK(local_offset != 0);

          auto&& [arrow_array, _] = context.getArrowArrayValues(local_offset);
          codegen_utils::setArrowArrayLength(arrow_array, output_index);
        }
      });

  if (successor_) {
    successor_->consume(context);
  }
}
}  // namespace cider::exec::nextgen::operators
