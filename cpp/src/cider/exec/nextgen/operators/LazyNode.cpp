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

#include "exec/nextgen/operators/LazyNode.h"

namespace cider::exec::nextgen::operators {
TranslatorPtr LazyNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<LazyTranslator>(shared_from_this(), succ);
}

void LazyTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void LazyTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto input_array = func->getArgument(1);

  auto&& [output_type, exprs] = node_->getOutputExprs();
  for (size_t i = 0; i < exprs.size(); ++i) {
    auto output_expr = dynamic_cast<Analyzer::OutputColumnVar*>(exprs[i].get());
    if (!output_expr) {
      continue;
    }
    auto input_expr = dynamic_cast<Analyzer::ColumnVar*>(
        output_expr->get_children_reference()[0]->get());

    // input child array
    auto input_col_id = input_expr->get_column_id();
    // auto input_child_array = func->createLocalJITValue([&input_array, input_col_id]() {
    //   return context::codegen_utils::getArrowArrayChild(input_array, input_col_id);
    // });

    // // input child array move to output
    // auto output_array = context.getOutputBatch();
    // // context::codegen_utils::setArrowArrayChild(output_array, i, input_child_array);
    // context::codegen_utils::copyArrowArrayChild(output_array, i, input_child_array);

    // input set to nullptr to make sure not release this child
    context::codegen_utils::clearArrowArrayChild(input_array, input_col_id);

    context.bare_output_input_map_.emplace(i, input_col_id);
  }
  if (successor_) {
    successor_->consume(context);
  }
}

}  // namespace cider::exec::nextgen::operators
