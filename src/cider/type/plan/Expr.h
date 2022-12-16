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
#ifndef TYPE_PLAN_EXPR_H
#define TYPE_PLAN_EXPR_H

#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "exec/nextgen/jitlib/JITLib.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "exec/nextgen/utils/TypeUtils.h"
#include "type/data/sqltypes.h"

namespace Analyzer {
using namespace cider::jitlib;
using namespace cider::exec::nextgen::utils;

class Expr;
class ColumnVar;
class TargetEntry;
using DomainSet = std::list<const Expr*>;
using ExprPtr = std::shared_ptr<Expr>;
using ExprPtrRefVector = std::vector<ExprPtr*>;

/*
 * @type Expr
 * @brief super class for all expressions in parse trees and in query plans
 */
class Expr : public std::enable_shared_from_this<Expr> {
 public:
  Expr(SQLTypes t, bool notnull) : type_info(t, notnull), contains_agg(false) {}
  Expr(SQLTypes t, int d, bool notnull)
      : type_info(t, d, 0, notnull), contains_agg(false) {}
  Expr(SQLTypes t, int d, int s, bool notnull)
      : type_info(t, d, s, notnull), contains_agg(false) {}
  Expr(const SQLTypeInfo& ti, bool has_agg = false)
      : type_info(ti), contains_agg(has_agg) {}
  virtual ~Expr() {}
  std::shared_ptr<Analyzer::Expr> get_shared_ptr() { return shared_from_this(); }
  const SQLTypeInfo& get_type_info() const { return type_info; }
  virtual void set_type_info(const SQLTypeInfo& ti) { type_info = ti; }
  bool get_contains_agg() const { return contains_agg; }
  void set_contains_agg(bool a) { contains_agg = a; }
  virtual std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info);
  virtual void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const {};
  virtual std::shared_ptr<Analyzer::Expr> deep_copy()
      const = 0;  // make a deep copy of self
                  /*
                   * @brief normalize_simple_predicate only applies to boolean expressions.
                   * it checks if it is an expression comparing a column
                   * with a constant.  if so, it returns a normalized copy of the predicate with ColumnVar
                   * always as the left operand with rte_idx set to the rte_idx of the ColumnVar.
                   * it returns nullptr with rte_idx set to -1 otherwise.
                   */
  virtual std::shared_ptr<Analyzer::Expr> normalize_simple_predicate(int& rte_idx) const {
    rte_idx = -1;
    return nullptr;
  }
  /*
   * @brief seperate conjunctive predicates into scan predicates, join predicates and
   * constant predicates.
   */
  virtual void group_predicates(std::list<const Expr*>& scan_predicates,
                                std::list<const Expr*>& join_predicates,
                                std::list<const Expr*>& const_predicates) const {}
  /*
   * @brief collect_rte_idx collects the indices of all the range table
   * entries involved in an expression
   */
  virtual void collect_rte_idx(std::set<int>& rte_idx_set) const {}
  /*
   * @brief collect_column_var collects all unique ColumnVar nodes in an expression
   * If include_agg = false, it does not include to ColumnVar nodes inside
   * the argument to AggExpr's.  Otherwise, they are included.
   * It does not make copies of the ColumnVar
   */
  virtual void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const {}
  /*
   * @brief rewrite_with_targetlist rewrite ColumnVar's in expression with entries in a
   * targetlist. targetlist expressions are expected to be only Var's or AggExpr's.
   * returns a new expression copy
   */
  virtual std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
    return deep_copy();
  };
  /*
   * @brief rewrite_with_child_targetlist rewrite ColumnVar's in expression with entries
   * in a child plan's targetlist. targetlist expressions are expected to be only Var's or
   * ColumnVar's returns a new expression copy
   */
  virtual std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
    return deep_copy();
  };
  /*
   * @brief rewrite_agg_to_var rewrite ColumnVar's in expression with entries in an
   * AggPlan's targetlist. targetlist expressions are expected to be only Var's or
   * ColumnVar's or AggExpr's All AggExpr's are written into Var's. returns a new
   * expression copy
   */
  virtual std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
    return deep_copy();
  }
  virtual bool operator==(const Expr& rhs) const = 0;
  virtual std::string toString() const = 0;
  virtual void print() const { std::cout << toString(); }

  virtual void add_unique(std::list<const Expr*>& expr_list) const;
  /*
   * @brief find_expr traverse Expr hierarchy and adds the node pointer to
   * the expr_list if the function f returns true.
   * Duplicate Expr's are not added the list.
   * Cannot use std::set because we don't have an ordering function.
   */
  virtual void find_expr(bool (*f)(const Expr*),
                         std::list<const Expr*>& expr_list) const {
    if (f(this)) {
      add_unique(expr_list);
    }
  }
  /*
   * @brief decompress adds cast operator to decompress encoded result
   */
  std::shared_ptr<Analyzer::Expr> decompress();
  /*
   * @brief perform domain analysis on Expr and fill in domain
   * information in domain_set.  Empty domain_set means no information.
   */
  virtual void get_domain(DomainSet& domain_set) const { domain_set.clear(); }

 public:
  // change this to pure virtual method after all subclasses support codegen.
  virtual JITExprValue& codegen(JITFunction& func);

  // for {JITValuePointer, ...}
  template <JITExprValueType type = JITExprValueType::ROW, typename... T>
  JITExprValue& set_expr_value(T&&... ptrs) {
    expr_var_ = JITExprValue(type, std::forward<T>(ptrs)...);
    return expr_var_;
  }

  JITExprValue& get_expr_value() { return expr_var_; }

  // TODO (bigPYJ1151): to pure virtual.
  virtual ExprPtrRefVector get_children_reference() {
    UNREACHABLE();
    return {};
  }

  void setLocalIndex(size_t index) { local_index_ = index; }

  size_t getLocalIndex() { return local_index_; }

 protected:
  JITTypeTag getJITTag(const SQLTypes& st) {
    return cider::exec::nextgen::utils::getJITTypeTag(st);
  }

 protected:
  SQLTypeInfo type_info;  // SQLTypeInfo of the return result of this expression
  bool contains_agg;

  JITExprValue expr_var_;
  size_t local_index_{0};  // 1-based index of input column in CodegenContext.
};

using ExpressionPtr = std::shared_ptr<Analyzer::Expr>;
using ExpressionPtrList = std::list<ExpressionPtr>;
using ExpressionPtrVector = std::vector<ExpressionPtr>;

}  // namespace Analyzer

template <typename Tp, typename... Args>
inline typename std::enable_if<std::is_base_of<Analyzer::Expr, Tp>::value,
                               std::shared_ptr<Tp>>::type
makeExpr(Args&&... args) {
  return std::make_shared<Tp>(std::forward<Args>(args)...);
}

// Remove a cast operator if present.
std::shared_ptr<Analyzer::Expr> remove_cast(const std::shared_ptr<Analyzer::Expr>& expr);
const Analyzer::Expr* remove_cast(const Analyzer::Expr* expr);

#endif  // TYPE_PLAN_EXPR_H
