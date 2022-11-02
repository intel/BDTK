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

#include "filter.h"
#include "exec/nextgen/translator/dummy.h"
#include "expr.h"

void FilterTranslator::consume(Context& context, const JITTuple& input) {
  auto next_input = codegen(context, input);
  if (successor_) {
    successor_->consume(context, next_input);
  }
}

JITTuple FilterTranslator::codegen(Context& context, const JITTuple& input) {
  ExprGenerator gen;
  JITTuple cond_tuple = gen.codegen(opNode_->exprs_, input);

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
  return JITTuple{};
}