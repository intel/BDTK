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
#include "exec/nextgen/operators/ArrowSourceNode.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;

TranslatorPtr ArrowSourceNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<ArrowSourceTranslator>(shared_from_this(), succ);
}

void ArrowSourceTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ArrowSourceTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [output_type, exprs] = op_node_->getOutputExprs();

  // get input ArrowArray pointer, generated query function signature likes void
  // query_func(RuntimeContext* ctx, ArrowArray* input).
  auto arrow_pointer = func->getArgument(1);

  for (int64_t index = 0; index < exprs.size(); ++index) {
    auto col_var_expr = dynamic_cast<Analyzer::ColumnVar*>(exprs[index].get());
    CHECK(col_var_expr);
    auto child_array = context::codegen_utils::getArrowArrayChild(arrow_pointer, index);

    int64_t buffer_num = utils::getBufferNum(col_var_expr->get_type_info().get_type());
    utils::JITExprValue buffer_values(buffer_num, JITExprValueType::BATCH);

    for (int64_t i = 0; i < buffer_num; ++i) {
      auto buffer = context::codegen_utils::getArrowArrayBuffer(child_array, i);
      buffer_values.append(buffer);
    }
    context.appendArrowArrayValues(child_array, std::move(buffer_values));
  }
  successor_->consume(context);
}

}  // namespace cider::exec::nextgen::operators