/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef TYPE_PLAN_LIKE_EXPR_H
#define TYPE_PLAN_LIKE_EXPR_H

#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "type/data/sqltypes.h"
#include "type/plan/Expr.h"
#include "type/schema/ColumnInfo.h"

namespace Analyzer {
/*
 * @type LikeExpr
 * @brief expression for the LIKE predicate.
 * arg must evaluate to char, varchar or text.
 */
class LikeExpr : public Expr {
 public:
  LikeExpr(std::shared_ptr<Analyzer::Expr> a,
           std::shared_ptr<Analyzer::Expr> l,
           std::shared_ptr<Analyzer::Expr> e,
           bool i,
           bool s)
      : Expr(kBOOLEAN, a->get_type_info().get_notnull())
      , arg(a)
      , like_expr(l)
      , escape_expr(e)
      , is_ilike(i)
      , is_simple(s) {}

  const Expr* get_arg() const { return arg.get(); }
  const Expr* get_like_expr() const { return like_expr.get(); }
  const Expr* get_escape_expr() const { return escape_expr.get(); }
  std::shared_ptr<Analyzer::Expr> get_shared_arg() { return arg; }
  std::shared_ptr<Analyzer::Expr> get_shared_like() { return like_expr; }
  std::shared_ptr<Analyzer::Expr> get_shared_escape() { return escape_expr; }

  bool get_is_ilike() const { return is_ilike; }
  bool get_is_simple() const { return is_simple; }

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

  [[deprecated]] const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  [[deprecated]] void group_predicates(
      std::list<const Expr*>& scan_predicates,
      std::list<const Expr*>& join_predicates,
      std::list<const Expr*>& const_predicates) const override;
  [[deprecated]] void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  [[deprecated]] void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_with_targetlist(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_with_child_targetlist(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_agg_to_var(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }

 private:
  std::shared_ptr<Analyzer::Expr> arg;        // the argument to the left of LIKE
  std::shared_ptr<Analyzer::Expr> like_expr;  // expression that evaluates to like string
  std::shared_ptr<Analyzer::Expr>
      escape_expr;  // expression that evaluates to escape string, can be nullptr
  bool is_ilike;    // is this ILIKE?
  bool is_simple;   // is this simple, meaning we can use fast path search (fits '%str%'
                    // pattern with no inner '%','_','[',']'

 public:
  ExprPtrRefVector get_children_reference() override {
    if (escape_expr) {
      return {&arg, &like_expr, &escape_expr};
    } else {
      return {&arg, &like_expr};
    }
  }

  JITExprValue& codegen(CodegenContext& context) override;
};
}  // namespace Analyzer

#endif
