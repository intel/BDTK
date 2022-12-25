
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
#include "type/plan/Expr.h"
#include "type/plan/UnaryExpr.h"

namespace Analyzer {
using namespace cider::jitlib;

// change this to pure virtual method after all subclasses support codegen.
JITExprValue& Expr::codegen(JITFunction& func, CodegenContext& context) {
  UNREACHABLE();
  return expr_var_;
}
}  // namespace Analyzer

std::shared_ptr<Analyzer::Expr> remove_cast(const std::shared_ptr<Analyzer::Expr>& expr) {
  const auto uoper = dynamic_cast<const Analyzer::UOper*>(expr.get());
  if (!uoper || uoper->get_optype() != kCAST) {
    return expr;
  }
  return uoper->get_own_operand();
}

const Analyzer::Expr* remove_cast(const Analyzer::Expr* expr) {
  const auto uoper = dynamic_cast<const Analyzer::UOper*>(expr);
  if (!uoper || uoper->get_optype() != kCAST) {
    return expr;
  }
  return uoper->get_operand();
}
