/*
 * Copyright (c) 2022 Intel Corporation.
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

#ifndef TYPE_PLAN_CASE_EXPR_H
#define TYPE_PLAN_CASE_EXPR_H

#include "exec/nextgen/context/CodegenContext.h"
#include "type/data/sqltypes.h"
#include "type/plan/Expr.h"
#include "util/sqldefs.h"

namespace Analyzer {
/*
 * @type CaseExpr
 * @brief the CASE-WHEN-THEN-ELSE expression
 */
class CaseExpr : public Expr {
 public:
  CaseExpr(const SQLTypeInfo& ti,
           bool has_agg,
           const std::list<std::pair<std::shared_ptr<Analyzer::Expr>,
                                     std::shared_ptr<Analyzer::Expr>>>& w,
           std::shared_ptr<Analyzer::Expr> e)
      : Expr(ti, has_agg), expr_pair_list(w), else_expr(e) {}
  const std::list<
      std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>&
  get_expr_pair_list() const {
    return expr_pair_list;
  }
  const Expr* get_else_expr() const { return else_expr.get(); }
  const std::shared_ptr<Analyzer::Expr>& get_else_ref() const { return else_expr; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  [[deprecated]] void group_predicates(
      std::list<const Expr*>& scan_predicates,
      std::list<const Expr*>& join_predicates,
      std::list<const Expr*>& const_predicates) const override;
  [[deprecated]] void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  [[deprecated]] void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  [[deprecated]] std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info) override;

  JITExprValue& codegen(CodegenContext& context) override;
  ExprPtrRefVector get_children_reference() override;

 private:
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      expr_pair_list;  // a pair of expressions for each WHEN expr1 THEN expr2.  expr1
                       // must be of boolean type.  all expr2's must be of compatible
                       // types and will be promoted to the common type.
  std::shared_ptr<Analyzer::Expr> else_expr;  // expression for ELSE.  nullptr if omitted.
};
}  // namespace Analyzer

#endif
