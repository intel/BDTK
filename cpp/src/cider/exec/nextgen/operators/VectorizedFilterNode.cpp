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
#include "exec/nextgen/operators/VectorizedFilterNode.h"

#include <cstddef>
#include <memory>

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/base/JITControlFlow.h"
#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/operators/OpNode.h"
#include "exec/nextgen/operators/VectorizedProjectNode.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "type/data/sqltypes.h"
#include "util/sqldefs.h"

namespace cider::exec::nextgen::operators {

using namespace context;
using namespace jitlib;

TranslatorPtr VectorizedFilterNode::toTranslator(const TranslatorPtr& successor) {
  return createOpTranslator<VectorizedFilterTranslator>(shared_from_this(), successor);
}

jitlib::JITValuePointer VectorizedFilterTranslator::generateFilterCondition(
    CodegenContext& context) {
  auto&& [_, exprs] = node_->getOutputExprs();
  // Combine conditions with AND.
  ExprPtr conditon = exprs.front();
  CHECK_EQ(conditon->get_type_info().get_type(), kBOOLEAN);
  for (size_t i = 1; i < exprs.size(); ++i) {
    CHECK_EQ(exprs[i]->get_type_info().get_type(), kBOOLEAN);
    conditon = std::make_shared<Analyzer::BinOper>(
        SQLTypeInfo(kBOOLEAN, !(conditon->getNullable() || exprs[i]->getNullable())),
        false,
        kAND,
        kONE,
        conditon,
        exprs[i]);
    conditon->setTrivialNullProcess(true);
  }

  // Register batch for the condition.
  auto condition_array = context.registerBatch(conditon->get_type_info(), "condition");
  conditon->setLocalIndex(context.appendArrowArrayValues(
      condition_array, utils::JITExprValue(0, JITExprValueType::BATCH)));

  // Generate the condition.
  ExprPtrVector condition_target{conditon};
  auto vectorized_proj_node = createOpNode<VectorizedProjectNode>(condition_target);
  auto vectorized_proj_translator = vectorized_proj_node->toTranslator();
  vectorized_proj_translator->codegen(context, [](CodegenContext&) {});

  // Remove null data
  // TODO bigPYJ1151): Support ISNULL NOTNULL vectorization.
  auto&& buffers = context.getArrowArrayValues(conditon->getLocalIndex()).second;
  utils::FixSizeJITExprValue bool_data(buffers);
  CHECK(bool_data.getValue().get());
  if (conditon->getNullable()) {
    CHECK(bool_data.getNull().get());
    context::codegen_utils::bitBufferAnd(bool_data.getValue(),
                                         bool_data.getValue(),
                                         bool_data.getNull(),
                                         context.getInputLength());
  }

  // Reset tail bits.
  context.getJITFunction()->emitRuntimeFunctionCall(
      "reset_tail_bits_64_align",
      JITFunctionEmitDescriptor{
          .params_vector = {bool_data.getValue().get(), context.getInputLength().get()}});

  return bool_data.getValue();
}

void VectorizedFilterTranslator::consume(context::CodegenContext& context) {
  codegen(context, [this](context::CodegenContext& context) {
    if (successor_) {
      successor_->consume(context);
    }
  });
}

void VectorizedFilterTranslator::codegenImpl(SuccessorEmitter successor_wrapper,
                                             context::CodegenContext& context,
                                             void* successor) {
  auto selected_row_mask = generateFilterCondition(context);

  auto&& func = context.getJITFunction();
  auto loop_builder = func->createLoopBuilder();

  auto mask_index = func->createVariable(JITTypeTag::INT64, "selected_row_mask_index", 0);
  auto row_index_start =
      func->createVariable(JITTypeTag::INT64, "selected_row_start_index", 0);
  auto upper = (context.getInputLength() + 63) / 64;

  loop_builder->condition([&upper, &mask_index]() { return mask_index < upper; })
      ->loop([&](LoopBuilder* builder) {
        auto&& selected_row_mask_i64 =
            selected_row_mask->castPointerSubType(JITTypeTag::INT64);
        auto current_selected_mask = selected_row_mask_i64[mask_index];

        // Set current i64 selected mask.
        context.setFilterMask(current_selected_mask, row_index_start);

        // All the rows are filtered.
        builder->loopContinue(current_selected_mask == 0);

        // TODO (bigPYJ1151): Support full execution when no rows are filtered.
        // Row-based execution.
        successor_wrapper(successor, context);
      })
      ->update([&mask_index, &row_index_start]() {
        mask_index = mask_index + 1;
        row_index_start = row_index_start + 64;
      })
      ->build();

  for (auto& defer_func : context.getDeferFunc()) {
    defer_func();
  }
  context.clearDeferFunc();
}
}  // namespace cider::exec::nextgen::operators
