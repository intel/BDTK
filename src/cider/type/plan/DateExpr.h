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

#ifndef TYPE_PLAN_DATE_EXPR_H
#define TYPE_PLAN_DATE_EXPR_H

#include "type/data/sqltypes.h"
#include "type/plan/Expr.h"
#include "util/sqldefs.h"

namespace Analyzer {
/*
 * @type DateaddExpr
 * @brief the DATEADD expression
 */
class DateaddExpr : public Expr {
 public:
  DateaddExpr(const SQLTypeInfo& ti,
              const DateaddField f,
              const std::shared_ptr<Analyzer::Expr> number,
              const std::shared_ptr<Analyzer::Expr> datetime)
      : Expr(ti, false), field_(f), number_(number), datetime_(datetime) {}
  DateaddField get_field() const { return field_; }
  const std::shared_ptr<Analyzer::Expr> get_number() const { return number_; }
  const std::shared_ptr<Analyzer::Expr> get_datetime() const { return datetime_; }
  Expr* get_number_expr() const { return number_.get(); }
  Expr* get_datetime_expr() const { return datetime_.get(); }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  [[deprecated]] void check_group_by(
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

 public:
  JITExprValue& codegen(JITFunction& func) override;
  ExprPtrRefVector get_children_reference() override {
    return {&(number_), &(datetime_)};
  }

 private:
  DateaddField field_;
  std::shared_ptr<Analyzer::Expr> number_;
  std::shared_ptr<Analyzer::Expr> datetime_;
};
}  // namespace Analyzer

#endif
