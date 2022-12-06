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
#include "exec/nextgen/operators/ColSinkNode.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using namespace jitlib;

TranslatorPtr ColSinkNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<ColSinkTranslator>(shared_from_this(), succ);
}

void ColSinkTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ColSinkTranslator::codegen(context::CodegenContext& context) {
  auto func = context.getJITFunction();
  auto&& [output_type, exprs] = node_->getOutputExprs();

  // void query_func(RuntimeContext* ctx, input..., output...)
  size_t arg_idx = (static_cast<ColSinkNode*>(node_.get()))->out_arg_idx_;
  for (auto& expr : exprs) {
    utils::FixSizeJITExprValue values(expr->get_expr_value());
    if (expr->get_type_info().get_notnull()) {
      func->getArgument(arg_idx++)->dereference() = *values.getValue();
    } else {
      func->getArgument(arg_idx++)->dereference() = *values.getNull();
      func->getArgument(arg_idx++)->dereference() = *values.getValue();
    }
  }
  if (successor_) {
    successor_->consume(context);
  }
}

}  // namespace cider::exec::nextgen::operators
