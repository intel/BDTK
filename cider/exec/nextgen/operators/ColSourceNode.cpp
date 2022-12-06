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
#include "exec/nextgen/operators/ColSourceNode.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;

TranslatorPtr ColSourceNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<ColSourceTranslator>(shared_from_this(), succ);
}

void ColSourceTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ColSourceTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [output_type, exprs] = node_->getOutputExprs();

  // void query_func(RuntimeContext* ctx, input..., output...)
  size_t arg_idx = 1;
  for (int64_t index = 0; index < exprs.size(); ++index) {
    auto expr = dynamic_cast<Analyzer::ColumnVar*>(exprs[index].get());
    CHECK(expr);

    if (expr->get_type_info().get_notnull()) {
      expr->set_expr_value(func->createConstant(JITTypeTag::BOOL, false),
                           func->getArgument(arg_idx++));
    } else {
      auto null = func->getArgument(arg_idx++);
      auto val = func->getArgument(arg_idx++);
      expr->set_expr_value(null, val);
    }
  }
  if (successor_) {
    successor_->consume(context);
  }
}

}  // namespace cider::exec::nextgen::operators
