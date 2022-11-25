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

/**
 * @file    Analyzer.h
 * @brief   Defines data structures for the semantic analysis phase of query processing
 **/
#ifndef TYPE_PLAN_ANALYZER_H
#define TYPE_PLAN_ANALYZER_H

#include <cstdint>
#include <iostream>
#include <list>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "BinaryExpr.h"
#include "ColumnExpr.h"
#include "ConstantExpr.h"
#include "Expr.h"
#include "cider/CiderException.h"
#include "type/data/sqltypes.h"
#include "util/Logger.h"
#include "util/sqldefs.h"

enum OpSupportExprType {
  kCOLUMN_VAR,
  kEXPRESSION_TUPLE,
  kCONSTANT,
  kU_OPER,
  kBIN_OPER,
  kRANGE_OPER,
  kSUBQUERY,
  kIN_VALUES,
  kIN_INTEGER_SET,
  kCHAR_LENGTH_EXPR,
  kKEY_FOR_STRING_EXPR,
  kSAMPLE_RATIO_EXPR,
  kLOWER_EXPR,
  kCARDINALITY_EXPR,
  kLIKE_EXPR,
  kREGEXP_EXPR,
  kWIDTH_BUCKET_EXPR,
  kLIKELIHOOD_EXPR,
  kAGG_EXPR,
  kCASE_EXPR,
  kEXTRACT_EXPR,
  kDATEADD_EXPR,
  kDATEDIFF_EXPR,
  kDATETRUNC_EXPR,
  kSTRING_OPER,
  kLOWER_STRING_OPER,
  kSUBSTRING_STRING_OPER,
  kFUNCTION_OPER,
  kOFFSET_IN_FRAGMENT,
  kWINDOW_FUNCTION,
  kARRAY_EXPR,
  kUNDEFINED_EXPR
};

namespace Analyzer {

/*
 * @type ExpressionTuple
 * @brief A tuple of expressions on the side of an equi-join on multiple columns.
 * Not to be used in any other context.
 */
class ExpressionTuple : public Expr {
 public:
  ExpressionTuple(const std::vector<std::shared_ptr<Analyzer::Expr>>& tuple)
      : Expr(SQLTypeInfo()), tuple_(tuple){};

  const std::vector<std::shared_ptr<Analyzer::Expr>>& getTuple() const { return tuple_; }

  void collect_rte_idx(std::set<int>& rte_idx_set) const override;

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

 private:
  const std::vector<std::shared_ptr<Analyzer::Expr>> tuple_;
};

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

 protected:
  SQLOps optype;  // operator type, e.g., kUMINUS, kISNULL, kEXISTS
  std::shared_ptr<Analyzer::Expr> operand;  // operand expression
};

/**
 * @type RangeOper
 * @brief
 */
class RangeOper : public Expr {
 public:
  RangeOper(const bool l_inclusive,
            const bool r_inclusive,
            std::shared_ptr<Analyzer::Expr> l,
            std::shared_ptr<Analyzer::Expr> r)
      : Expr(SQLTypeInfo(kNULLT), /*not_null=*/false)
      , left_inclusive_(l_inclusive)
      , right_inclusive_(r_inclusive)
      , left_operand_(l)
      , right_operand_(r) {
    CHECK(left_operand_);
    CHECK(right_operand_);
  }

  const Expr* get_left_operand() const { return left_operand_.get(); }
  const Expr* get_right_operand() const { return right_operand_.get(); }

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    left_operand_->collect_rte_idx(rte_idx_set);
    right_operand_->collect_rte_idx(rte_idx_set);
  }

  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    left_operand_->collect_column_var(colvar_set, include_agg);
    right_operand_->collect_column_var(colvar_set, include_agg);
  }

 private:
  // build a range between these two operands
  bool left_inclusive_;
  bool right_inclusive_;
  std::shared_ptr<Analyzer::Expr> left_operand_;
  std::shared_ptr<Analyzer::Expr> right_operand_;
};

class Query;

/*
 * @type Subquery
 * @brief subquery expression.  Note that the type of the expression is the type of the
 * TargetEntry in the subquery instead of the set.
 */
class Subquery : public Expr {
 public:
  Subquery(const SQLTypeInfo& ti, Query* q)
      : Expr(ti), parsetree(q) /*, plan(nullptr)*/ {}
  ~Subquery() override;
  const Query* get_parsetree() const { return parsetree; }
  // const Plan *get_plan() const { return plan; }
  // void set_plan(Plan *p) { plan = p; } // subquery plan is set by the optimizer
  std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info) override;
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override {
    CHECK(false);
  }
  void collect_rte_idx(std::set<int>& rte_idx_set) const override { CHECK(false); }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    CHECK(false);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    CIDER_THROW(CiderUnsupportedException, fmt::format("tlist.size is {}", tlist.size()));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    CIDER_THROW(CiderUnsupportedException, fmt::format("tlist.size is {}", tlist.size()));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    CIDER_THROW(CiderUnsupportedException, fmt::format("tlist.size is {}", tlist.size()));
  }
  bool operator==(const Expr& rhs) const override {
    CHECK(false);
    return false;
  }
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override {
    CHECK(false);
  }

 private:
  Query* parsetree;  // parse tree of the subquery
};

/*
 * @type InValues
 * @brief represents predicate expr IN (v1, v2, ...)
 * v1, v2, ... are can be either Constant or Parameter.
 */
class InValues : public Expr {
 public:
  InValues(std::shared_ptr<Analyzer::Expr> a,
           const std::list<std::shared_ptr<Analyzer::Expr>>& l);
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  const std::list<std::shared_ptr<Analyzer::Expr>>& get_value_list() const {
    return value_list;
  }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;  // the argument left of IN
  const std::list<std::shared_ptr<Analyzer::Expr>>
      value_list;  // the list of values right of IN
};

/*
 * @type InIntegerSet
 * @brief represents predicate expr IN (v1, v2, ...) for the case where the right
 *        hand side is a list of integers or dictionary-encoded strings generated
 *        by a IN subquery. Avoids the overhead of storing a list of shared pointers
 *        to Constant objects, making it more suitable for IN sub-queries usage.
 * v1, v2, ... are integers
 */
class InIntegerSet : public Expr {
 public:
  InIntegerSet(const std::shared_ptr<const Analyzer::Expr> a,
               const std::vector<int64_t>& values,
               const bool not_null);

  const Expr* get_arg() const { return arg.get(); }

  const std::vector<int64_t>& get_value_list() const { return value_list; }

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

 private:
  const std::shared_ptr<const Analyzer::Expr> arg;  // the argument left of IN
  const std::vector<int64_t> value_list;            // the list of values right of IN
};

/*
 * @type CharLengthExpr
 * @brief expression for the CHAR_LENGTH expression.
 * arg must evaluate to char, varchar or text.
 */
class CharLengthExpr : public Expr {
 public:
  CharLengthExpr(std::shared_ptr<Analyzer::Expr> a, bool e)
      : Expr(kINT, a->get_type_info().get_notnull()), arg(a), calc_encoded_length(e) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  bool get_calc_encoded_length() const { return calc_encoded_length; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CharLengthExpr>(arg->rewrite_with_targetlist(tlist),
                                    calc_encoded_length);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CharLengthExpr>(arg->rewrite_with_child_targetlist(tlist),
                                    calc_encoded_length);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CharLengthExpr>(arg->rewrite_agg_to_var(tlist), calc_encoded_length);
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;
  bool calc_encoded_length;
};

/*
 * @type KeyForStringExpr
 * @brief expression for the KEY_FOR_STRING expression.
 * arg must be a dict encoded column, not str literal.
 */
class KeyForStringExpr : public Expr {
 public:
  KeyForStringExpr(std::shared_ptr<Analyzer::Expr> a)
      : Expr(kINT, a->get_type_info().get_notnull()), arg(a) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<KeyForStringExpr>(arg->rewrite_with_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<KeyForStringExpr>(arg->rewrite_with_child_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<KeyForStringExpr>(arg->rewrite_agg_to_var(tlist));
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;
};

/*
 * @type SampleRatioExpr
 * @brief expression for the SAMPLE_RATIO expression. Argument range is expected to be
 * between 0 and 1.
 */
class SampleRatioExpr : public Expr {
 public:
  SampleRatioExpr(std::shared_ptr<Analyzer::Expr> a)
      : Expr(kBOOLEAN, a->get_type_info().get_notnull()), arg(a) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<SampleRatioExpr>(arg->rewrite_with_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<SampleRatioExpr>(arg->rewrite_with_child_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<SampleRatioExpr>(arg->rewrite_agg_to_var(tlist));
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;
};

/**
 * @brief Expression class for the LOWER (lowercase) string function.
 * The "arg" constructor parameter must be an expression that resolves to a string
 * datatype (e.g. TEXT).
 */
class LowerExpr : public Expr {
 public:
  LowerExpr(std::shared_ptr<Analyzer::Expr> arg) : Expr(arg->get_type_info()), arg(arg) {}

  const Expr* get_arg() const { return arg.get(); }

  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }

  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }

  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }

  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LowerExpr>(arg->rewrite_with_targetlist(tlist));
  }

  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LowerExpr>(arg->rewrite_with_child_targetlist(tlist));
  }

  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LowerExpr>(arg->rewrite_agg_to_var(tlist));
  }

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;

  bool operator==(const Expr& rhs) const override;

  std::string toString() const override;

  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;
};

/*
 * @type CardinalityExpr
 * @brief expression for the CARDINALITY expression.
 * arg must evaluate to array (or multiset when supported).
 */
class CardinalityExpr : public Expr {
 public:
  CardinalityExpr(std::shared_ptr<Analyzer::Expr> a)
      : Expr(kINT, a->get_type_info().get_notnull()), arg(a) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CardinalityExpr>(arg->rewrite_with_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CardinalityExpr>(arg->rewrite_with_child_targetlist(tlist));
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<CardinalityExpr>(arg->rewrite_agg_to_var(tlist));
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;
};

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
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  const Expr* get_like_expr() const { return like_expr.get(); }
  const Expr* get_escape_expr() const { return escape_expr.get(); }
  bool get_is_ilike() const { return is_ilike; }
  bool get_is_simple() const { return is_simple; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_with_targetlist(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_with_child_targetlist(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikeExpr>(arg->rewrite_agg_to_var(tlist),
                              like_expr->deep_copy(),
                              escape_expr ? escape_expr->deep_copy() : nullptr,
                              is_ilike,
                              is_simple);
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  std::shared_ptr<Analyzer::Expr> get_shared_arg() { return arg; }
  std::shared_ptr<Analyzer::Expr> get_shared_Like() { return like_expr; }
  std::shared_ptr<Analyzer::Expr> get_shared_escape() { return escape_expr; }

 private:
  std::shared_ptr<Analyzer::Expr> arg;        // the argument to the left of LIKE
  std::shared_ptr<Analyzer::Expr> like_expr;  // expression that evaluates to like string
  std::shared_ptr<Analyzer::Expr>
      escape_expr;  // expression that evaluates to escape string, can be nullptr
  bool is_ilike;    // is this ILIKE?
  bool is_simple;   // is this simple, meaning we can use fast path search (fits '%str%'
                    // pattern with no inner '%','_','[',']'
};

/*
 * @type RegexpExpr
 * @brief expression for REGEXP.
 * arg must evaluate to char, varchar or text.
 */
class RegexpExpr : public Expr {
 public:
  RegexpExpr(std::shared_ptr<Analyzer::Expr> a,
             std::shared_ptr<Analyzer::Expr> p,
             std::shared_ptr<Analyzer::Expr> e)
      : Expr(kBOOLEAN, a->get_type_info().get_notnull())
      , arg(a)
      , pattern_expr(p)
      , escape_expr(e) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  const Expr* get_pattern_expr() const { return pattern_expr.get(); }
  const Expr* get_escape_expr() const { return escape_expr.get(); }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<RegexpExpr>(arg->rewrite_with_targetlist(tlist),
                                pattern_expr->deep_copy(),
                                escape_expr ? escape_expr->deep_copy() : nullptr);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<RegexpExpr>(arg->rewrite_with_child_targetlist(tlist),
                                pattern_expr->deep_copy(),
                                escape_expr ? escape_expr->deep_copy() : nullptr);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<RegexpExpr>(arg->rewrite_agg_to_var(tlist),
                                pattern_expr->deep_copy(),
                                escape_expr ? escape_expr->deep_copy() : nullptr);
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;  // the argument to the left of REGEXP
  std::shared_ptr<Analyzer::Expr>
      pattern_expr;  // expression that evaluates to pattern string
  std::shared_ptr<Analyzer::Expr>
      escape_expr;  // expression that evaluates to escape string, can be nullptr
};

/*
 * @type WidthBucketExpr
 * @brief expression for width_bucket functions.
 */
class WidthBucketExpr : public Expr {
 public:
  WidthBucketExpr(const std::shared_ptr<Analyzer::Expr> target_value,
                  const std::shared_ptr<Analyzer::Expr> lower_bound,
                  const std::shared_ptr<Analyzer::Expr> upper_bound,
                  const std::shared_ptr<Analyzer::Expr> partition_count)
      : Expr(kINT, target_value->get_type_info().get_notnull())
      , target_value_(target_value)
      , lower_bound_(lower_bound)
      , upper_bound_(upper_bound)
      , partition_count_(partition_count)
      , constant_expr_(false)
      , skip_out_of_bound_check_(false) {}
  const Expr* get_target_value() const { return target_value_.get(); }
  const Expr* get_lower_bound() const { return lower_bound_.get(); }
  const Expr* get_upper_bound() const { return upper_bound_.get(); }
  const Expr* get_partition_count() const { return partition_count_.get(); }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    target_value_->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    target_value_->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<WidthBucketExpr>(target_value_->rewrite_with_targetlist(tlist),
                                     lower_bound_,
                                     upper_bound_,
                                     partition_count_);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<WidthBucketExpr>(target_value_->rewrite_with_child_targetlist(tlist),
                                     lower_bound_,
                                     upper_bound_,
                                     partition_count_);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<WidthBucketExpr>(target_value_->rewrite_agg_to_var(tlist),
                                     lower_bound_,
                                     upper_bound_,
                                     partition_count_);
  }
  double get_bound_val(const Analyzer::Expr* bound_expr) const;
  int32_t get_partition_count_val() const;
  template <typename T>
  int32_t compute_bucket(T target_const_val, SQLTypeInfo& ti) const {
    // this utility function is useful for optimizing expression range decision
    // for an expression depending on width_bucket expr
    T null_val = ti.is_integer() ? inline_int_null_val(ti) : inline_fp_null_val(ti);
    double lower_bound_val = get_bound_val(lower_bound_.get());
    double upper_bound_val = get_bound_val(upper_bound_.get());
    auto partition_count_val = get_partition_count_val();
    if (target_const_val == null_val) {
      return INT32_MIN;
    }
    float res;
    if (lower_bound_val < upper_bound_val) {
      if (target_const_val < lower_bound_val) {
        return 0;
      } else if (target_const_val >= upper_bound_val) {
        return partition_count_val + 1;
      }
      double dividend = upper_bound_val - lower_bound_val;
      res = ((partition_count_val * (target_const_val - lower_bound_val)) / dividend) + 1;
    } else {
      if (target_const_val > lower_bound_val) {
        return 0;
      } else if (target_const_val <= upper_bound_val) {
        return partition_count_val + 1;
      }
      double dividend = lower_bound_val - upper_bound_val;
      res = ((partition_count_val * (lower_bound_val - target_const_val)) / dividend) + 1;
    }
    return res;
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  bool can_skip_out_of_bound_check() const { return skip_out_of_bound_check_; }
  void skip_out_of_bound_check() const { skip_out_of_bound_check_ = true; }
  void set_constant_expr() const { constant_expr_ = true; }
  bool is_constant_expr() const { return constant_expr_; }

 private:
  std::shared_ptr<Analyzer::Expr> target_value_;     // target value expression
  std::shared_ptr<Analyzer::Expr> lower_bound_;      // lower_bound
  std::shared_ptr<Analyzer::Expr> upper_bound_;      // upper_bound
  std::shared_ptr<Analyzer::Expr> partition_count_;  // partition_count
  // true if lower, upper and partition count exprs are constant
  mutable bool constant_expr_;
  // true if we can skip oob check and is determined within compile time
  mutable bool skip_out_of_bound_check_;
};

/*
 * @type LikelihoodExpr
 * @brief expression for LIKELY and UNLIKELY boolean identity functions.
 */
class LikelihoodExpr : public Expr {
 public:
  LikelihoodExpr(std::shared_ptr<Analyzer::Expr> a, float l = 0.5)
      : Expr(kBOOLEAN, a->get_type_info().get_notnull()), arg(a), likelihood(l) {}
  const Expr* get_arg() const { return arg.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  float get_likelihood() const { return likelihood; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    arg->collect_rte_idx(rte_idx_set);
  }
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    arg->collect_column_var(colvar_set, include_agg);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikelihoodExpr>(arg->rewrite_with_targetlist(tlist), likelihood);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikelihoodExpr>(arg->rewrite_with_child_targetlist(tlist),
                                    likelihood);
  }
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override {
    return makeExpr<LikelihoodExpr>(arg->rewrite_agg_to_var(tlist), likelihood);
  }
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  std::shared_ptr<Analyzer::Expr> arg;  // the argument to LIKELY, UNLIKELY
  float likelihood;
};

/*
 * @type AggExpr
 * @brief expression for builtin SQL aggregates.
 */
class AggExpr : public Expr {
 public:
  AggExpr(const SQLTypeInfo& ti,
          SQLAgg a,
          std::shared_ptr<Analyzer::Expr> g,
          bool d,
          std::shared_ptr<Analyzer::Constant> e)
      : Expr(ti, true), aggtype(a), arg(g), is_distinct(d), arg1(e) {}
  AggExpr(SQLTypes t,
          SQLAgg a,
          Expr* g,
          bool d,
          std::shared_ptr<Analyzer::Constant> e,
          int idx)
      : Expr(SQLTypeInfo(t, g == nullptr ? true : g->get_type_info().get_notnull()), true)
      , aggtype(a)
      , arg(g)
      , is_distinct(d)
      , arg1(e) {}
  SQLAgg get_aggtype() const { return aggtype; }
  Expr* get_arg() const { return arg.get(); }
  std::shared_ptr<Analyzer::Expr> get_own_arg() const { return arg; }
  bool get_is_distinct() const { return is_distinct; }
  std::shared_ptr<Analyzer::Constant> get_arg1() const { return arg1; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override {
    if (arg) {
      arg->collect_rte_idx(rte_idx_set);
    }
  };
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override {
    if (include_agg && arg != nullptr) {
      arg->collect_column_var(colvar_set, include_agg);
    }
  }
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  SQLAgg aggtype;                       // aggregate type: kAVG, kMIN, kMAX, kSUM, kCOUNT
  std::shared_ptr<Analyzer::Expr> arg;  // argument to aggregate
  bool is_distinct;                     // true only if it is for COUNT(DISTINCT x)
  // APPROX_COUNT_DISTINCT error_rate, APPROX_QUANTILE quantile
  std::shared_ptr<Analyzer::Constant> arg1;
};

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
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;
  std::shared_ptr<Analyzer::Expr> add_cast(const SQLTypeInfo& new_type_info) override;
  void get_domain(DomainSet& domain_set) const override;

 private:
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      expr_pair_list;  // a pair of expressions for each WHEN expr1 THEN expr2.  expr1
                       // must be of boolean type.  all expr2's must be of compatible
                       // types and will be promoted to the common type.
  std::shared_ptr<Analyzer::Expr> else_expr;  // expression for ELSE.  nullptr if omitted.
};

/*
 * @type ExtractExpr
 * @brief the EXTRACT expression
 */
class ExtractExpr : public Expr {
 public:
  ExtractExpr(const SQLTypeInfo& ti,
              bool has_agg,
              ExtractField f,
              std::shared_ptr<Analyzer::Expr> e)
      : Expr(ti, has_agg), field_(f), from_expr_(e) {}
  ExtractField get_field() const { return field_; }
  const Expr* get_from_expr() const { return from_expr_.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_from_expr() const { return from_expr_; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  ExtractField field_;
  std::shared_ptr<Analyzer::Expr> from_expr_;
};

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
  const Expr* get_number_expr() const { return number_.get(); }
  const Expr* get_datetime_expr() const { return datetime_.get(); }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  const DateaddField field_;
  const std::shared_ptr<Analyzer::Expr> number_;
  const std::shared_ptr<Analyzer::Expr> datetime_;
};

/*
 * @type DatediffExpr
 * @brief the DATEDIFF expression
 */
class DatediffExpr : public Expr {
 public:
  DatediffExpr(const SQLTypeInfo& ti,
               const DatetruncField f,
               const std::shared_ptr<Analyzer::Expr> start,
               const std::shared_ptr<Analyzer::Expr> end)
      : Expr(ti, false), field_(f), start_(start), end_(end) {}
  DatetruncField get_field() const { return field_; }
  const Expr* get_start_expr() const { return start_.get(); }
  const Expr* get_end_expr() const { return end_.get(); }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  const DatetruncField field_;
  const std::shared_ptr<Analyzer::Expr> start_;
  const std::shared_ptr<Analyzer::Expr> end_;
};

/*
 * @type DatetruncExpr
 * @brief the DATE_TRUNC expression
 */
class DatetruncExpr : public Expr {
 public:
  DatetruncExpr(const SQLTypeInfo& ti,
                bool has_agg,
                DatetruncField f,
                std::shared_ptr<Analyzer::Expr> e)
      : Expr(ti, has_agg), field_(f), from_expr_(e) {}
  DatetruncField get_field() const { return field_; }
  const Expr* get_from_expr() const { return from_expr_.get(); }
  const std::shared_ptr<Analyzer::Expr> get_own_from_expr() const { return from_expr_; }
  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void check_group_by(
      const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const override;
  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;
  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

 private:
  DatetruncField field_;
  std::shared_ptr<Analyzer::Expr> from_expr_;
};

class StringOper : public Expr {
 public:
  // Todo(todd): Set nullability based on literals too

  enum class OperandTypeFamily { STRING_FAMILY, INT_FAMILY };

  StringOper(const SqlStringOpKind kind,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : Expr(StringOper::get_return_type(kind, args)), kind_(kind), args_(args) {}

  StringOper(const SqlStringOpKind kind,
             const SQLTypeInfo& return_ti,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : Expr(return_ti), kind_(kind), args_(args) {}

  StringOper(const SqlStringOpKind kind,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& args,
             const size_t min_args,
             const std::vector<OperandTypeFamily>& expected_type_families,
             const std::vector<std::string>& arg_names)
      : Expr(StringOper::get_return_type(kind, args)), kind_(kind), args_(args) {
    check_operand_types(min_args, expected_type_families, arg_names);
  }

  StringOper(const SqlStringOpKind kind,
             const SQLTypeInfo& return_ti,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& args,
             const size_t min_args,
             const std::vector<OperandTypeFamily>& expected_type_families,
             const std::vector<std::string>& arg_names)
      : Expr(return_ti), kind_(kind), args_(args) {
    check_operand_types(min_args, expected_type_families, arg_names);
  }

  StringOper(const SqlStringOpKind kind,
             const SQLTypeInfo& return_ti,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& args,
             const std::vector<std::shared_ptr<Analyzer::Expr>>& chained_string_op_exprs)
      : Expr(return_ti)
      , kind_(kind)
      , args_(args)
      , chained_string_op_exprs_(chained_string_op_exprs) {}

  StringOper(const StringOper& other_string_oper)
      : Expr(other_string_oper.get_type_info()) {
    kind_ = other_string_oper.kind_;
    args_ = other_string_oper.args_;
    chained_string_op_exprs_ = other_string_oper.chained_string_op_exprs_;
  }

  StringOper(const std::shared_ptr<StringOper>& other_string_oper)
      : Expr(other_string_oper->get_type_info()) {
    kind_ = other_string_oper->kind_;
    args_ = other_string_oper->args_;
    chained_string_op_exprs_ = other_string_oper->chained_string_op_exprs_;
  }

  SqlStringOpKind get_kind() const { return kind_; }

  size_t getArity() const { return args_.size(); }

  size_t getLiteralsArity() const {
    size_t num_literals{0UL};
    for (const auto& arg : args_) {
      if (dynamic_cast<const Constant*>(arg.get())) {
        num_literals++;
      }
    }
    return num_literals;
  }

  const Expr* getArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i].get();
  }

  std::shared_ptr<Analyzer::Expr> getOwnArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i];
  }

  std::vector<std::shared_ptr<Analyzer::Expr>> getOwnArgs() const { return args_; }

  std::vector<std::shared_ptr<Analyzer::Expr>> getChainedStringOpExprs() const {
    return chained_string_op_exprs_;
  }

  void collect_rte_idx(std::set<int>& rte_idx_set) const override;

  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;

  bool hasNoneEncodedTextArg() const {
    if (args_.empty()) {
      return false;
    }
    const auto& arg0_ti = args_[0]->get_type_info();
    if (!arg0_ti.is_string()) {
      return false;
    }
    if (arg0_ti.is_none_encoded_string()) {
      return true;
    }
    CHECK(arg0_ti.is_dict_encoded_string());
    return arg0_ti.get_comp_param() == TRANSIENT_DICT_ID;
  }

  /**
   * @brief returns whether we have one and only one column involved
   * in this StringOper and all its descendents, and that that column
   * is a dictionary-encoded text type
   *
   * @return std::vector<SqlTypeInfo>
   */
  bool hasSingleDictEncodedColInput() const;

  std::vector<size_t> getLiteralArgIndexes() const;

  using LiteralArgMap = std::map<size_t, std::pair<SQLTypes, Datum>>;

  LiteralArgMap getLiteralArgs() const;

  std::shared_ptr<Analyzer::Expr> rewrite_with_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;

  std::shared_ptr<Analyzer::Expr> rewrite_with_child_targetlist(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;

  std::shared_ptr<Analyzer::Expr> rewrite_agg_to_var(
      const std::vector<std::shared_ptr<TargetEntry>>& tlist) const override;

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  void group_predicates(std::list<const Expr*>& scan_predicates,
                        std::list<const Expr*>& join_predicates,
                        std::list<const Expr*>& const_predicates) const override;

  bool operator==(const Expr& rhs) const override;

  std::string toString() const override;

  void find_expr(bool (*f)(const Expr*),
                 std::list<const Expr*>& expr_list) const override;

  virtual size_t getMinArgs() const {
    CHECK(false);
    return {};
  }
  virtual std::vector<OperandTypeFamily> getExpectedTypeFamilies() const {
    CHECK(false);
    return {};
  }
  virtual const std::vector<std::string>& getArgNames() const {
    CHECK(false);
    return {};
  }

 private:
  static SQLTypeInfo get_return_type(
      const SqlStringOpKind kind,
      const std::vector<std::shared_ptr<Analyzer::Expr>>& args);

  void check_operand_types(const size_t min_args,
                           const std::vector<OperandTypeFamily>& expected_type_families,
                           const std::vector<std::string>& arg_names,
                           const bool dict_encoded_cols_only = true,
                           const bool cols_first_arg_only = true) const;

  SqlStringOpKind kind_;
  std::vector<std::shared_ptr<Analyzer::Expr>> args_;
  std::vector<std::shared_ptr<Analyzer::Expr>> chained_string_op_exprs_;
};

class CharLengthStringOper : public StringOper {
 public:
  CharLengthStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::CHAR_LENGTH,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  CharLengthStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::CHAR_LENGTH,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  CharLengthStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 1UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand"};
    return names;
  }
};

class LowerStringOper : public StringOper {
 public:
  LowerStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::LOWER,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  LowerStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::LOWER,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  LowerStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 1UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand"};
    return names;
  }
};

class UpperStringOper : public StringOper {
 public:
  UpperStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::UPPER,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  UpperStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::UPPER,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  UpperStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 1UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand"};
    return names;
  }
};

class TrimStringOper : public StringOper {
 public:
  TrimStringOper(const SqlStringOpKind& op_kind,
                 const std::shared_ptr<Analyzer::Expr>& input,
                 const std::shared_ptr<Analyzer::Expr>& characters)
      : StringOper(checkOpKindValidity(op_kind),
                   {input, characters},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  TrimStringOper(const SqlStringOpKind& op_kind,
                 const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(checkOpKindValidity(op_kind),
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  TrimStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 2UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY, OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    // args[0]: the string to remove characters from
    // args[1]: the set of characters to remove
    static std::vector<std::string> names{"input", "characters"};
    return names;
  }

 private:
  SqlStringOpKind checkOpKindValidity(const SqlStringOpKind& op_kind) {
    CHECK(op_kind == SqlStringOpKind::TRIM || op_kind == SqlStringOpKind::LTRIM ||
          op_kind == SqlStringOpKind::RTRIM);
    return op_kind;
  }
};

class SubstringStringOper : public StringOper {
 public:
  SubstringStringOper(const std::shared_ptr<Analyzer::Expr>& operand,
                      const std::shared_ptr<Analyzer::Expr>& start_pos)
      : StringOper(SqlStringOpKind::SUBSTRING,
                   {operand, start_pos},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  SubstringStringOper(const std::shared_ptr<Analyzer::Expr>& operand,
                      const std::shared_ptr<Analyzer::Expr>& start_pos,
                      const std::shared_ptr<Analyzer::Expr>& length)
      : StringOper(SqlStringOpKind::SUBSTRING,
                   {operand, start_pos, length},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  SubstringStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::SUBSTRING,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  SubstringStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 2UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::INT_FAMILY,
            OperandTypeFamily::INT_FAMILY};
  }
  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand", "start_pos", "substr_len"};
    return names;
  }
};

class ConcatStringOper : public StringOper {
 public:
  ConcatStringOper(const std::shared_ptr<Analyzer::Expr>& former,
                   const std::shared_ptr<Analyzer::Expr>& latter)
      : StringOper(getConcatOpKind({former, latter}),
                   rearrangeOperands({former, latter}),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  ConcatStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(getConcatOpKind(operands),
                   rearrangeOperands(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  ConcatStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 2UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY, OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand_0", "operand_1"};
    return names;
  }

 private:
  bool isLiteralOrCastLiteral(const Analyzer::Expr* operand);

  SqlStringOpKind getConcatOpKind(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands);

  std::vector<std::shared_ptr<Analyzer::Expr>> rearrangeOperands(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands);
};

class RegexpReplaceStringOper : public StringOper {
 public:
  RegexpReplaceStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::REGEXP_REPLACE,
                   foldLiteralStrCasts(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  RegexpReplaceStringOper(const std::shared_ptr<Analyzer::Expr>& input,
                          const std::shared_ptr<Analyzer::Expr>& pattern,
                          const std::shared_ptr<Analyzer::Expr>& replacement,
                          const std::shared_ptr<Analyzer::Expr>& position,
                          const std::shared_ptr<Analyzer::Expr>& occurrence)
      : StringOper(SqlStringOpKind::REGEXP_REPLACE,
                   {input, pattern, replacement, position, occurrence},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  RegexpReplaceStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 5UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::INT_FAMILY,
            OperandTypeFamily::INT_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{
        "input", "pattern", "replacement", "position", "occurrence"};
    return names;
  }

 private:
  std::vector<std::shared_ptr<Analyzer::Expr>> foldLiteralStrCasts(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands,
      int start_idx = 1);
};

class TryStringCastOper : public StringOper {
 public:
  TryStringCastOper(const SQLTypeInfo& ti, const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::TRY_STRING_CAST,
                   ti,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  TryStringCastOper(const SQLTypeInfo& ti,
                    const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::TRY_STRING_CAST,
                   ti,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  TryStringCastOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 1UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY};
  }
  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"operand"};
    return names;
  }
};

class FunctionOper : public Expr {
 public:
  FunctionOper(const SQLTypeInfo& ti,
               const std::string& name,
               const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : Expr(ti, false), name_(name), args_(args) {}

  std::string getName() const { return name_; }

  size_t getArity() const { return args_.size(); }

  const Analyzer::Expr* getArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i].get();
  }

  std::shared_ptr<Analyzer::Expr> getOwnArg(const size_t i) const {
    CHECK_LT(i, args_.size());
    return args_[i];
  }

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;
  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

 private:
  const std::string name_;
  const std::vector<std::shared_ptr<Analyzer::Expr>> args_;
};

class FunctionOperWithCustomTypeHandling : public FunctionOper {
 public:
  FunctionOperWithCustomTypeHandling(
      const SQLTypeInfo& ti,
      const std::string& name,
      const std::vector<std::shared_ptr<Analyzer::Expr>>& args)
      : FunctionOper(ti, name, args) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
};

/*
 * @type OffsetInFragment
 * @brief The offset of a row in the current fragment. To be used by updates.
 */
class OffsetInFragment : public Expr {
 public:
  OffsetInFragment() : Expr(SQLTypeInfo(kBIGINT, true)){};

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;
};

/*
 * @type OrderEntry
 * @brief represents an entry in ORDER BY clause.
 */
struct OrderEntry {
  OrderEntry(int t, bool d, bool nf) : tle_no(t), is_desc(d), nulls_first(nf){};
  ~OrderEntry() {}
  std::string toString() const;
  void print() const { std::cout << toString(); }
  int tle_no;       /* targetlist entry number: 1-based */
  bool is_desc;     /* true if order is DESC */
  bool nulls_first; /* true if nulls are ordered first.  otherwise last. */
};

/*
 * @type WindowFunction
 * @brief A window function.
 */
class WindowFunction : public Expr {
 public:
  WindowFunction(const SQLTypeInfo& ti,
                 const SqlWindowFunctionKind kind,
                 const std::vector<std::shared_ptr<Analyzer::Expr>>& args,
                 const std::vector<std::shared_ptr<Analyzer::Expr>>& partition_keys,
                 const std::vector<std::shared_ptr<Analyzer::Expr>>& order_keys,
                 const std::vector<OrderEntry>& collation)
      : Expr(ti)
      , kind_(kind)
      , args_(args)
      , partition_keys_(partition_keys)
      , order_keys_(order_keys)
      , collation_(collation){};

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  bool operator==(const Expr& rhs) const override;
  std::string toString() const override;

  SqlWindowFunctionKind getKind() const { return kind_; }

  const std::vector<std::shared_ptr<Analyzer::Expr>>& getArgs() const { return args_; }

  const std::vector<std::shared_ptr<Analyzer::Expr>>& getPartitionKeys() const {
    return partition_keys_;
  }

  const std::vector<std::shared_ptr<Analyzer::Expr>>& getOrderKeys() const {
    return order_keys_;
  }

  const std::vector<OrderEntry>& getCollation() const { return collation_; }

 private:
  const SqlWindowFunctionKind kind_;
  const std::vector<std::shared_ptr<Analyzer::Expr>> args_;
  const std::vector<std::shared_ptr<Analyzer::Expr>> partition_keys_;
  const std::vector<std::shared_ptr<Analyzer::Expr>> order_keys_;
  const std::vector<OrderEntry> collation_;
};

/*
 * @type ArrayExpr
 * @brief Corresponds to ARRAY[] statements in SQL
 */

class ArrayExpr : public Expr {
 public:
  ArrayExpr(SQLTypeInfo const& array_ti,
            ExpressionPtrVector const& array_exprs,
            bool is_null = false,
            bool local_alloc = false)
      : Expr(array_ti)
      , contained_expressions_(array_exprs)
      , local_alloc_(local_alloc)
      , is_null_(is_null) {}

  Analyzer::ExpressionPtr deep_copy() const override;
  std::string toString() const override;
  bool operator==(Expr const& rhs) const override;
  size_t getElementCount() const { return contained_expressions_.size(); }
  bool isLocalAlloc() const { return local_alloc_; }
  bool isNull() const { return is_null_; }

  const Analyzer::Expr* getElement(const size_t i) const {
    CHECK_LT(i, contained_expressions_.size());
    return contained_expressions_[i].get();
  }

  void collect_rte_idx(std::set<int>& rte_idx_set) const override;
  void collect_column_var(
      std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>&
          colvar_set,
      bool include_agg) const override;

 private:
  ExpressionPtrVector contained_expressions_;
  bool local_alloc_;
  bool is_null_;  // constant is NULL
};

/*
 * @type TargetEntry
 * @brief Target list defines a relational projection.  It is a list of TargetEntry's.
 */
class TargetEntry {
 public:
  TargetEntry(const std::string& n, std::shared_ptr<Analyzer::Expr> e, bool u)
      : resname(n), expr(e), unnest(u) {}
  virtual ~TargetEntry() {}
  const std::string& get_resname() const { return resname; }
  void set_resname(const std::string& name) { resname = name; }
  Expr* get_expr() const { return expr.get(); }
  std::shared_ptr<Expr> get_own_expr() const { return expr; }
  void set_expr(std::shared_ptr<Analyzer::Expr> e) { expr = e; }
  bool get_unnest() const { return unnest; }
  std::string toString() const;
  void print() const { std::cout << toString(); }

 private:
  std::string resname;  // alias name, e.g., SELECT salary + bonus AS compensation,
  std::shared_ptr<Analyzer::Expr> expr;  // expression to evaluate for the value
  bool unnest;                           // unnest a collection type
};

/*
 * @type Query
 * @brief parse tree for a query
 */
class Query {
 public:
  Query()
      : is_distinct(false)
      , where_predicate(nullptr)
      , having_predicate(nullptr)
      , order_by(nullptr)
      , next_query(nullptr)
      , is_unionall(false)
      , stmt_type(kSELECT)
      , num_aggs(0)
      , result_table_id(0)
      , limit(0)
      , offset(0) {}
  virtual ~Query();
  bool get_is_distinct() const { return is_distinct; }
  int get_num_aggs() const { return num_aggs; }
  const std::vector<std::shared_ptr<TargetEntry>>& get_targetlist() const {
    return targetlist;
  }
  std::vector<std::shared_ptr<TargetEntry>>& get_targetlist_nonconst() {
    return targetlist;
  }
  const Expr* get_where_predicate() const { return where_predicate.get(); }
  const std::list<std::shared_ptr<Analyzer::Expr>>& get_group_by() const {
    return group_by;
  };
  const Expr* get_having_predicate() const { return having_predicate.get(); }
  const std::list<OrderEntry>* get_order_by() const { return order_by; }
  const Query* get_next_query() const { return next_query; }
  SQLStmtType get_stmt_type() const { return stmt_type; }
  bool get_is_unionall() const { return is_unionall; }
  int get_result_table_id() const { return result_table_id; }
  const std::list<int>& get_result_col_list() const { return result_col_list; }
  void set_result_col_list(const std::list<int>& col_list) { result_col_list = col_list; }
  void set_result_table_id(int id) { result_table_id = id; }
  void set_is_distinct(bool d) { is_distinct = d; }
  void set_where_predicate(std::shared_ptr<Analyzer::Expr> p) { where_predicate = p; }
  void set_group_by(std::list<std::shared_ptr<Analyzer::Expr>>& g) { group_by = g; }
  void set_having_predicate(std::shared_ptr<Analyzer::Expr> p) { having_predicate = p; }
  void set_order_by(std::list<OrderEntry>* o) { order_by = o; }
  void set_next_query(Query* q) { next_query = q; }
  void set_is_unionall(bool u) { is_unionall = u; }
  void set_stmt_type(SQLStmtType t) { stmt_type = t; }
  void set_num_aggs(int a) { num_aggs = a; }
  int get_rte_idx(const std::string& range_var_name) const;
  void add_tle(std::shared_ptr<TargetEntry> tle) { targetlist.push_back(tle); }
  int64_t get_limit() const { return limit; }
  void set_limit(int64_t l) { limit = l; }
  int64_t get_offset() const { return offset; }
  void set_offset(int64_t o) { offset = o; }

 private:
  bool is_distinct;                                      // true only if SELECT DISTINCT
  std::vector<std::shared_ptr<TargetEntry>> targetlist;  // represents the SELECT clause
  std::shared_ptr<Analyzer::Expr> where_predicate;       // represents the WHERE clause
  std::list<std::shared_ptr<Analyzer::Expr>> group_by;   // represents the GROUP BY clause
  std::shared_ptr<Analyzer::Expr> having_predicate;      // represents the HAVING clause
  std::list<OrderEntry>* order_by;                       // represents the ORDER BY clause
  Query* next_query;                                     // the next query to UNION
  bool is_unionall;                                      // true only if it is UNION ALL
  SQLStmtType stmt_type;
  int num_aggs;                    // number of aggregate functions in query
  int result_table_id;             // for INSERT statements only
  std::list<int> result_col_list;  // for INSERT statement only
  int64_t limit;                   // row count for LIMIT clause.  0 means ALL
  int64_t offset;                  // offset in OFFSET clause.  0 means no offset.
};

}  // namespace Analyzer

inline std::shared_ptr<Analyzer::Var> var_ref(const Analyzer::Expr* expr,
                                              const Analyzer::Var::WhichRow which_row,
                                              const int varno) {
  if (const auto col_expr = dynamic_cast<const Analyzer::ColumnVar*>(expr)) {
    return makeExpr<Analyzer::Var>(
        col_expr->get_column_info(), col_expr->get_rte_idx(), which_row, varno);
  }
  return makeExpr<Analyzer::Var>(expr->get_type_info(), which_row, varno);
}

// Returns true iff the two expression lists are equal (same size and each element are
// equal).
bool expr_list_match(const std::vector<std::shared_ptr<Analyzer::Expr>>& lhs,
                     const std::vector<std::shared_ptr<Analyzer::Expr>>& rhs);

// Remove a cast operator if present.
std::shared_ptr<Analyzer::Expr> remove_cast(const std::shared_ptr<Analyzer::Expr>& expr);
const Analyzer::Expr* remove_cast(const Analyzer::Expr* expr);

#endif  // TYPE_PLAN_ANALYZER_H
