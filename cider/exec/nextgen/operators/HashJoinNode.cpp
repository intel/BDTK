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
#include "exec/nextgen/operators/HashJoinNode.h"

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/JITLib.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;
using namespace context;

class BuildTableReader {
 public:
  BuildTableReader(utils::JITExprValue& buffer_values,
                   ExprPtr& expr,
                   JITValuePointer& index)
      : buffer_values_(buffer_values), expr_(expr), index_(index) {}

  void read() {
    switch (expr_->get_type_info().get_type()) {
      case kTINYINT:
      case kSMALLINT:
      case kINT:
      case kBIGINT:
        readFixSizedTypeCol();
        break;
      default:
        LOG(FATAL) << "Unsupported data type in BuildTableReader: "
                   << expr_->get_type_info().get_type_name();
    }
  }

 private:
  void readFixSizedTypeCol() {
    utils::FixSizeJITExprValue fixsize_values(buffer_values_);
    auto data_buffer = fixsize_values.getValue();
    auto& func = data_buffer->getParentJITFunction();
    JITTypeTag tag = utils::getJITTypeTag(expr_->get_type_info().get_type());
    // data buffer decoder
    auto actual_raw_data_buffer = data_buffer->castPointerSubType(tag);
    auto row_data = actual_raw_data_buffer[index_];

    if (expr_->get_type_info().get_notnull()) {
      expr_->set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), row_data);
    } else {
      // null buffer decoder
      auto row_null_data = func.emitRuntimeFunctionCall(
          "check_bit_vector_clear",
          JITFunctionEmitDescriptor{
              .ret_type = JITTypeTag::BOOL,
              .params_vector = {{fixsize_values.getNull().get(), index_.get()}}});

      expr_->set_expr_value<JITExprValueType::ROW>(row_null_data, row_data);
    }
  }

 private:
  utils::JITExprValue& buffer_values_;
  ExprPtr& expr_;
  JITValuePointer& index_;
};

TranslatorPtr HashJoinNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<HashJoinTranslator>(shared_from_this(), succ);
}

void HashJoinTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void HashJoinTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto join_quals = static_cast<HashJoinNode*>(node_.get())->getJoinQuals();

  std::vector<JITValuePointer> params;
  for (int i = 0; i < join_quals.size(); ++i) {
    utils::FixSizeJITExprValue jit_value(join_quals[i]->get_expr_value());
    auto& expr = jit_value.getValue();
    params.emplace_back(expr);
  }

  auto keys = func->packJITValues(params);

  auto join_res = func->createLocalJITValue([&keys]() {
    // TODO get HashTable pointer
    return codegen_utils::lookUpValueByKey(keys, keys);
  });

  auto join_res_len = func->createLocalJITValue(
      [&join_res]() { return codegen_utils::getJoinResLength(join_res); });

  auto build_tables = static_cast<HashJoinNode*>(node_.get())->getBuildTables();
  auto row_index = func->createVariable(JITTypeTag::INT64, "index", 0l);
  func->createLoopBuilder()
      ->condition([&row_index, &join_res_len]() { return row_index < join_res_len; })
      ->loop([&] {
        auto data_idx = func->createLiteral(JITTypeTag::INT64, 1l);

        auto res_array = func->createLocalJITValue([&join_res, &row_index]() {
          return codegen_utils::getJoinResArray(join_res, row_index);
        });

        auto res_row_id = func->createLocalJITValue([&join_res, &row_index]() {
          return codegen_utils::getJoinResRowId(join_res, row_index);
        });

        for (int idx = 0; idx < build_tables.size(); idx++) {
          ExprPtr& expr = build_tables[idx];

          auto child_arrow_array = func->createLocalJITValue([&res_array, &idx]() {
            return codegen_utils::getArrowArrayChild(res_array, idx);
          });

          int64_t buffer_num = utils::getBufferNum(expr->get_type_info().get_type());
          utils::JITExprValue buffer_values(buffer_num, JITExprValueType::BATCH);
          for (int64_t i = 0; i < buffer_num; ++i) {
            auto buffer = func->createLocalJITValue([&child_arrow_array, i]() {
              return context::codegen_utils::getArrowArrayBuffer(child_arrow_array, i);
            });
            buffer_values.append(buffer);
          }

          BuildTableReader reader(buffer_values, expr, row_index);
          reader.read();
        }
        successor_->consume(context);
      })
      ->update([&row_index]() { row_index = row_index + 1l; })
      ->build();
}
}  // namespace cider::exec::nextgen::operators