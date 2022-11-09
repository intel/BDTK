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
#include "exec/nextgen/jitlib/base/JITValueOperations.h"
#include "exec/nextgen/translator/dummy.h"
#include "exec/nextgen/translator/expr.h"

namespace cider::exec::nextgen::translator {
void FilterTranslator::consume(Context& context) {
  auto next_input = codegen(context);
}

JITValuePointer FilterTranslator::codegen(Context& context) {
  auto& func_ = context.query_func_;
  auto if_builder = func_->createIfBuilder();
  if_builder
      ->condition([&]() {
        ExprGenerator gen(func_);

        auto bool_init = func_->createVariable(JITTypeTag::BOOL, "bool_init");
        bool_init = func_->createConstant(JITTypeTag::BOOL, true);
        for (const auto& expr : node_.exprs_) {
          auto& cond = gen.codegen(expr.get());
          bool_init = *bool_init && cond.get_value();
          TODO("MaJian", "support null in condition");
        }
        TODO("MaJian", "support short circuit logic operation");
        return bool_init;
      })
      ->ifTrue([&]() {
        CHECK(successor_);
        successor_->consume(context);
      })
      ->build();

  return nullptr;
}

}  // namespace cider::exec::nextgen::translator
