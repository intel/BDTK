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

#include "exec/nextgen/translator/filter.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/translator/dummy.h"
#include "exec/nextgen/translator/expr.h"

namespace cider::exec::nextgen::translator {
void FilterTranslator::consume(Context& context) {
  auto next_input = codegen(context);
  if (successor_) {
    successor_->consume(context);
  }
}

JITValuePointer FilterTranslator::codegen(Context& context) {
  ExprGenerator gen(context.query_func_);
  for (const auto& expr : node_.exprs_) {
    gen.codegen(expr.get());
  }

  llvm::Value* cond = nullptr;
  TODO("MaJian", "extract cond from tuple");

  // build control flow
  TODO("MaJian", "move to control flow");
  //   auto* cond_true = llvm::BasicBlock::Create(
  //       *context.llvm_context_, "filter_true", context.query_func_);
  //   auto* cond_false = llvm::BasicBlock::Create(
  //       *context.llvm_context_, "filter_false", context.query_func_);
  //   context.ir_builder_.CreateCondBr(cond, cond_true, cond_false);

  //   return {cond_true, cond_false};
  return JITValuePointer(nullptr);
}

}  // namespace cider::exec::nextgen::translator
