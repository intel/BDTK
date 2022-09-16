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

#pragma once

#include <cstdint>
#include "type/plan/Analyzer.h"
#include "velox/core/Expressions.h"

using namespace facebook::velox::core;

namespace facebook::velox::plugin {

class VeloxToCiderExprConverter {
 public:
  VeloxToCiderExprConverter() {}
  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const ITypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;
  // convert list of exprs
  std::vector<std::shared_ptr<const Analyzer::Expr>> toCiderExprs(
      std::vector<std::shared_ptr<const ITypedExpr>> v_exprs,
      std::unordered_map<std::string, int> col_info) const;

  std::shared_ptr<Analyzer::Expr> wrapExprWithCast(
      const std::shared_ptr<Analyzer::Expr> c_expr,
      std::shared_ptr<const Type> type) const;

 private:
  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const ConstantTypedExpr> v_expr) const;

  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const FieldAccessTypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;

  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const CallTypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;

  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const LambdaTypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;

  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const ConcatTypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;

  std::shared_ptr<Analyzer::Expr> toCiderExpr(
      std::shared_ptr<const CastTypedExpr> v_expr,
      std::unordered_map<std::string, int> col_info) const;
};
}  // namespace facebook::velox::plugin
