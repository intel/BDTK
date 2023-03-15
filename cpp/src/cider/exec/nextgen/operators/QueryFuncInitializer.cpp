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
#include "exec/nextgen/operators/QueryFuncInitializer.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;

TranslatorPtr QueryFuncInitializer::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<QueryFuncInitializerTranslator>(shared_from_this(), succ);
}

void QueryFuncInitializerTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void QueryFuncInitializerTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [output_type, exprs] = node_->getOutputExprs();

  // get input ArrowArray pointer, generated query function signature likes
  // void query_func(RuntimeContext* ctx, ArrowArray* input).
  auto arrow_pointer = func->getArgument(1);
  auto input_len = func->createLocalJITValue([&arrow_pointer]() {
    return context::codegen_utils::getArrowArrayLength(arrow_pointer);
  });
  CHECK(input_len.get());
  context.setInputLength(input_len);

  // Load input columns
  for (int64_t index = 0; index < exprs.size(); ++index) {
    auto col_var_expr = dynamic_cast<Analyzer::ColumnVar*>(exprs[index].get());
    CHECK(col_var_expr);
    auto child_array = func->createLocalJITValue([&arrow_pointer, col_var_expr]() {
      return context::codegen_utils::getArrowArrayChild(arrow_pointer,
                                                        col_var_expr->get_column_id());
    });

    int64_t buffer_num = utils::getBufferNum(col_var_expr->get_type_info().get_type());
    utils::JITExprValue buffer_values(buffer_num, JITExprValueType::BATCH);

    for (int64_t i = 0; i < buffer_num; ++i) {
      auto buffer = func->createLocalJITValue([&child_array, i]() {
        return context::codegen_utils::getArrowArrayBuffer(child_array, i);
      });
      buffer_values.append(buffer);
    }

    if (col_var_expr->get_type_info().get_type() == kARRAY) {
      auto values_child_array = func->createLocalJITValue([&child_array]() {
        return context::codegen_utils::getArrowArrayChild(child_array, 0);
      });
      for (int64_t i = 0; i < 2; ++i) {
        auto buffer = func->createLocalJITValue([&values_child_array, i]() {
          return context::codegen_utils::getArrowArrayBuffer(values_child_array, i);
        });
        buffer_values.append(buffer);
      }
    }

    if (col_var_expr->get_type_info().get_type() == kTEXT ||
        col_var_expr->get_type_info().get_type() == kVARCHAR ||
        col_var_expr->get_type_info().get_type() == kCHAR) {
      // append dictionary buffer as last element if this is a string column, and if
      // string dictionary is valid, the second pointer will be index in dictionary, the
      // third pointer will be invalid.
      buffer_values.append(func->createLocalJITValue([&child_array]() {
        return context::codegen_utils::getArrowArrayDictionary(child_array);
      }));
    }

    // All ArrowArray related JITValues will be saved in CodegenContext, and associate
    // with exprs with local_offset.
    size_t local_offset =
        context.appendArrowArrayValues(child_array, std::move(buffer_values));
    exprs[index]->setLocalIndex(local_offset);
  }

  // Prepare target columns
  ExprPtrVector& target_exprs =
      static_cast<QueryFuncInitializer*>(node_.get())->getTargetExprs();
  // Target output ArrowArray will always be the first Batch in CodegenCTX.
  auto output_arrow_array = context.registerBatch(
      SQLTypeInfo(kSTRUCT,
                  false,
                  [&target_exprs]() {
                    std::vector<SQLTypeInfo> output_types;
                    output_types.reserve(target_exprs.size());
                    for (auto& expr : target_exprs) {
                      output_types.emplace_back(expr->get_type_info());
                    }
                    return output_types;
                  }()),
      "output",
      true);
  context.appendArrowArrayValues(output_arrow_array,
                                 utils::JITExprValue(1, JITExprValueType::BATCH));

  for (int64_t i = 0; i < target_exprs.size(); ++i) {
    ExprPtr& expr = target_exprs[i];
    auto child_arrow_array = func->createLocalJITValue([&output_arrow_array, i]() {
      return context::codegen_utils::getArrowArrayChild(output_arrow_array, i);
    });

    // Binding target exprs with JITValues.
    expr->setLocalIndex(context.appendArrowArrayValues(
        child_arrow_array, utils::JITExprValue(0, JITExprValueType::BATCH)));
  }

  successor_->consume(context);
}

}  // namespace cider::exec::nextgen::operators
