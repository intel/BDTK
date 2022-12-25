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
#ifndef TYPE_PLAN_UNARY_EXPR_H
#define TYPE_PLAN_UNARY_EXPR_H

#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "Expr.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "type/data/sqltypes.h"
#include "util/sqldefs.h"

namespace Analyzer {
/*
 * @type UOper
 * @brief represents unary operator expressions.  operator types include
 * kUMINUS, kISNULL, kEXISTS, kCAST
 */
class UOper : public Expr {
 public:
  UOper(const SQLTypeInfo& ti, bool has_agg, SQLOps o, std::shared_ptr<Analyzer::Expr> p)
      : Expr(ti, has_agg), optype(o), operand(p) {}
  UOper(SQLTypes t, SQLOps o, std::shared_ptr<Analyzer::Expr> p)
      : Expr(t, o == kISNULL ? true : p->get_type_info().get_notnull())
      , optype(o)
      , operand(p) {}
  SQLOps get_optype() const { return optype; }
  const Expr* get_operand() const { return operand.get(); }
  std::shared_ptr<Analyzer::Expr> get_non_const_own_operand() const { return operand; }
  const std::shared_ptr<Analyzer::Expr> get_own_operand() const { return operand; }
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    operand->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    operand->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<UOper>(
        type_info, contains_agg, optype, operand->rewrite_with_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<UOper>(
        type_info, contains_agg, optype, operand->rewrite_with_child_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<UOper>(
        type_info, contains_agg, optype, operand->rewrite_agg_to_var(tlist));
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info) override;

 public:
  ExprPtrRefVector get_children_reference() override { return {&operand}; }
  JITExprValue& codegen(JITFunction& func, CodegenContext& context) override;
  JITExprValue& codegenIsNull(JITFunction& func,
                              CodegenContext& context,
                              Analyzer::Expr* operand_expr_val,
                              SQLOps optype);
  JITExprValue& codegenNot(JITFunction& func,
                           CodegenContext& context,
                           Analyzer::Expr* operand_expr_val);
  JITExprValue& codegenCast(JITFunction& func,
                            CodegenContext& context,
                            Analyzer::Expr* operand_expr_val);
  JITExprValue& codegenUminus(JITFunction& func,
                              CodegenContext& context,
                              Analyzer::Expr* operand_expr_val);
  JITValuePointer codegenCastFunc(JITFunction& func, JITValue& lhs);

 protected:
  SQLOps optype;  // operator type, e.g., kUMINUS, kISNULL, kEXISTS
  std::shared_ptr<Analyzer::Expr> operand;  // operand expression
};
}  // namespace Analyzer

#endif
