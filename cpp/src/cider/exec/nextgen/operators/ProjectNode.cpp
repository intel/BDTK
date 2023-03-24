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

#include "exec/nextgen/operators/ProjectNode.h"

namespace cider::exec::nextgen::operators {
TranslatorPtr ProjectNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<ProjectTranslator>(shared_from_this(), succ);
}

void ProjectTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void ProjectTranslator::codegen(context::CodegenContext& context) {
  auto&& [output_type, exprs] = node_->getOutputExprs();
  for (auto& expr : exprs) {
    if (context.hasLazyNode() && dynamic_cast<Analyzer::OutputColumnVar*>(expr.get())) {
      // ignore bare columns
      continue;
    }
    // set a flag for output string expr. Nested expression like `substring(concat(col_a,
    // col_b), 1, 5)` will have different process logical for output expression and
    // internal expression.
    if (auto strOper = std::dynamic_pointer_cast<Analyzer::StringOper>(expr)) {
      strOper->setIsOutput();
    }
    expr->codegen(context);
  }
  if (successor_) {
    successor_->consume(context);
  }
}

}  // namespace cider::exec::nextgen::operators
