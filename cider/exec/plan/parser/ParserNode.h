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
#ifndef PARSER_NODE_H_
#define PARSER_NODE_H_

#include <cstdint>
#include <cstring>
#include <list>
#include <string>

#include <rapidjson/document.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/process/search_path.hpp>

#include "cider/CiderException.h"
#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"
#include "util/DdlUtils.h"

#include <functional>

namespace Parser {

/*
 * @type Node
 * @brief root super class for all nodes in a pre-Analyzer
 * parse tree.
 */
class Node {
 public:
  virtual ~Node() {}
};

/*
 * @type SQLType
 * @brief class that captures type, predication and scale.
 */
class SQLType : public Node, public ddl_utils::SqlType {
 public:
  explicit SQLType(SQLTypes t) : ddl_utils::SqlType(t, -1, 0, false, -1) {}
  SQLType(SQLTypes t, int p1) : ddl_utils::SqlType(t, p1, 0, false, -1) {}
  SQLType(SQLTypes t, int p1, int p2, bool a) : ddl_utils::SqlType(t, p1, p2, a, -1) {}
  SQLType(SQLTypes t, int p1, int p2, bool a, int array_size)
      : ddl_utils::SqlType(t, p1, p2, a, array_size) {}
  SQLType(SQLTypes t, bool a, int array_size)
      : ddl_utils::SqlType(t, -1, 0, a, array_size) {}
};

/*
 * @type Expr
 * @brief root super class for all expression nodes
 */
class Expr : public Node {
 public:
  enum TlistRefType { TLIST_NONE, TLIST_REF, TLIST_COPY };

  virtual std::string to_string() const = 0;
};

/*
 * @type Literal
 * @brief root super class for all literals
 */
class Literal : public Expr {
 public:
  std::string to_string() const override = 0;
};

/*
 * @type NullLiteral
 * @brief the Literal NULL
 */
class NullLiteral : public Literal {
 public:
  NullLiteral() {}
  std::string to_string() const override { return "NULL"; }
};

/*
 * @type StringLiteral
 * @brief the literal for string constants
 */
class StringLiteral : public Literal {
 public:
  explicit StringLiteral(std::string* s) : stringval_(s) {}
  const std::string* get_stringval() const { return stringval_.get(); }

  static std::shared_ptr<Analyzer::Expr> analyzeValue(const std::string&);
  std::string to_string() const override { return "'" + *stringval_ + "'"; }

 private:
  std::unique_ptr<std::string> stringval_;
};

/*
 * @type IntLiteral
 * @brief the literal for integer constants
 */
class IntLiteral : public Literal {
 public:
  explicit IntLiteral(int64_t i) : intval_(i) {}
  int64_t get_intval() const { return intval_; }
  static std::shared_ptr<Analyzer::Expr> analyzeValue(const int64_t intval);
  std::string to_string() const override {
    return boost::lexical_cast<std::string>(intval_);
  }

 private:
  int64_t intval_;
};

/*
 * @type FixedPtLiteral
 * @brief the literal for DECIMAL and NUMERIC
 */
class FixedPtLiteral : public Literal {
 public:
  explicit FixedPtLiteral(std::string* n) : fixedptval_(n) {}
  const std::string* get_fixedptval() const { return fixedptval_.get(); }

  static std::shared_ptr<Analyzer::Expr> analyzeValue(const int64_t numericval,
                                                      const int scale,
                                                      const int precision);
  std::string to_string() const override { return *fixedptval_; }

 private:
  std::unique_ptr<std::string> fixedptval_;
};

/*
 * @type FloatLiteral
 * @brief the literal for FLOAT or REAL
 */
class FloatLiteral : public Literal {
 public:
  explicit FloatLiteral(float f) : floatval_(f) {}
  float get_floatval() const { return floatval_; }

  std::string to_string() const override {
    return boost::lexical_cast<std::string>(floatval_);
  }

 private:
  float floatval_;
};

/*
 * @type DoubleLiteral
 * @brief the literal for DOUBLE PRECISION
 */
class DoubleLiteral : public Literal {
 public:
  explicit DoubleLiteral(double d) : doubleval_(d) {}
  double get_doubleval() const { return doubleval_; }

  std::string to_string() const override {
    return boost::lexical_cast<std::string>(doubleval_);
  }

 private:
  double doubleval_;
};

/*
 * @type TimestampLiteral
 * @brief the literal for Timestamp
 */
class TimestampLiteral : public Literal {
 public:
  explicit TimestampLiteral() { time(reinterpret_cast<time_t*>(&timestampval_)); }

  static std::shared_ptr<Analyzer::Expr> get(const int64_t);
  std::string to_string() const override {
    // TODO: Should we convert to a datum and use the datum toString converters to pretty
    // print?
    return boost::lexical_cast<std::string>(timestampval_);
  }

 private:
  int64_t timestampval_;
};

/*
 * @type UserLiteral
 * @brief the literal for USER
 */
class UserLiteral : public Literal {
 public:
  UserLiteral() {}

  static std::shared_ptr<Analyzer::Expr> get(const std::string&);
  std::string to_string() const override { return "USER"; }
};

/*
 * @type ArrayLiteral
 * @brief the literal for arrays
 */
class ArrayLiteral : public Literal {
 public:
  ArrayLiteral() {}
  ArrayLiteral(std::list<Expr*>* v) {
    CHECK(v);
    for (const auto e : *v) {
      value_list_.emplace_back(e);
    }
    delete v;
  }
  const std::list<std::unique_ptr<Expr>>& get_value_list() const { return value_list_; }

  std::string to_string() const override;

 private:
  std::list<std::unique_ptr<Expr>> value_list_;
};

/*
 * @type OperExpr
 * @brief all operator expressions
 */
class OperExpr : public Expr {
 public:
  OperExpr(SQLOps t, Expr* l, Expr* r)
      : optype_(t), opqualifier_(kONE), left_(l), right_(r) {}
  OperExpr(SQLOps t, SQLQualifier q, Expr* l, Expr* r)
      : optype_(t), opqualifier_(q), left_(l), right_(r) {}
  SQLOps get_optype() const { return optype_; }
  const Expr* get_left() const { return left_.get(); }
  const Expr* get_right() const { return right_.get(); }
  static std::shared_ptr<Analyzer::Expr> normalize(
      const SQLOps optype,
      const SQLQualifier qual,
      std::shared_ptr<Analyzer::Expr> left_expr,
      std::shared_ptr<Analyzer::Expr> right_expr);
  std::string to_string() const override;

 private:
  SQLOps optype_;
  SQLQualifier opqualifier_;
  std::unique_ptr<Expr> left_;
  std::unique_ptr<Expr> right_;
};

// forward reference of QuerySpec
class QuerySpec;

/*
 * @type SubqueryExpr
 * @brief expression for subquery
 */
class SubqueryExpr : public Expr {
 public:
  explicit SubqueryExpr(QuerySpec* q) : query_(q) {}
  const QuerySpec* get_query() const { return query_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<QuerySpec> query_;
};

class IsNullExpr : public Expr {
 public:
  IsNullExpr(bool n, Expr* a) : is_not_(n), arg_(a) {}
  bool get_is_not() const { return is_not_; }

  std::string to_string() const override;

 private:
  bool is_not_;
  std::unique_ptr<Expr> arg_;
};

/*
 * @type InExpr
 * @brief expression for the IS NULL predicate
 */
class InExpr : public Expr {
 public:
  InExpr(bool n, Expr* a) : is_not_(n), arg_(a) {}
  bool get_is_not() const { return is_not_; }
  const Expr* get_arg() const { return arg_.get(); }

  std::string to_string() const override;

 protected:
  bool is_not_;
  std::unique_ptr<Expr> arg_;
};

/*
 * @type InSubquery
 * @brief expression for the IN (subquery) predicate
 */
class InSubquery : public InExpr {
 public:
  InSubquery(bool n, Expr* a, SubqueryExpr* q) : InExpr(n, a), subquery_(q) {}
  const SubqueryExpr* get_subquery() const { return subquery_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<SubqueryExpr> subquery_;
};

/*
 * @type InValues
 * @brief expression for IN (val1, val2, ...)
 */
class InValues : public InExpr {
 public:
  InValues(bool n, Expr* a, std::list<Expr*>* v) : InExpr(n, a) {
    CHECK(v);
    for (const auto e : *v) {
      value_list_.emplace_back(e);
    }
    delete v;
  }
  const std::list<std::unique_ptr<Expr>>& get_value_list() const { return value_list_; }

  std::string to_string() const override;

 private:
  std::list<std::unique_ptr<Expr>> value_list_;
};

/*
 * @type BetweenExpr
 * @brief expression for BETWEEN lower AND upper
 */
class BetweenExpr : public Expr {
 public:
  BetweenExpr(bool n, Expr* a, Expr* l, Expr* u)
      : is_not_(n), arg_(a), lower_(l), upper_(u) {}
  bool get_is_not() const { return is_not_; }
  const Expr* get_arg() const { return arg_.get(); }
  const Expr* get_lower() const { return lower_.get(); }
  const Expr* get_upper() const { return upper_.get(); }

  std::string to_string() const override;

 private:
  bool is_not_;
  std::unique_ptr<Expr> arg_;
  std::unique_ptr<Expr> lower_;
  std::unique_ptr<Expr> upper_;
};

/*
 * @type CharLengthExpr
 * @brief expression to get length of string
 */

class CharLengthExpr : public Expr {
 public:
  CharLengthExpr(Expr* a, bool e) : arg_(a), calc_encoded_length_(e) {}
  const Expr* get_arg() const { return arg_.get(); }
  bool get_calc_encoded_length() const { return calc_encoded_length_; }

  std::string to_string() const override;

 private:
  std::unique_ptr<Expr> arg_;
  bool calc_encoded_length_;
};

/*
 * @type CardinalityExpr
 * @brief expression to get cardinality of an array
 */

class CardinalityExpr : public Expr {
 public:
  CardinalityExpr(Expr* a) : arg_(a) {}
  const Expr* get_arg() const { return arg_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<Expr> arg_;
};

/*
 * @type LikeExpr
 * @brief expression for the LIKE predicate
 */
class LikeExpr : public Expr {
 public:
  LikeExpr(bool n, bool i, Expr* a, Expr* l, Expr* e)
      : is_not_(n), is_ilike_(i), arg_(a), like_string_(l), escape_string_(e) {}
  bool get_is_not() const { return is_not_; }
  const Expr* get_arg() const { return arg_.get(); }
  const Expr* get_like_string() const { return like_string_.get(); }
  const Expr* get_escape_string() const { return escape_string_.get(); }

  static std::shared_ptr<Analyzer::Expr> get(std::shared_ptr<Analyzer::Expr> arg_expr,
                                             std::shared_ptr<Analyzer::Expr> like_expr,
                                             std::shared_ptr<Analyzer::Expr> escape_expr,
                                             const bool is_ilike,
                                             const bool is_not);
  std::string to_string() const override;

 private:
  bool is_not_;
  bool is_ilike_;
  std::unique_ptr<Expr> arg_;
  std::unique_ptr<Expr> like_string_;
  std::unique_ptr<Expr> escape_string_;

  static void check_like_expr(const std::string& like_str, char escape_char);
  static bool test_is_simple_expr(const std::string& like_str, char escape_char);
  static void erase_cntl_chars(std::string& like_str, char escape_char);
};

/*
 * @type RegexpExpr
 * @brief expression for REGEXP
 */
class RegexpExpr : public Expr {
 public:
  RegexpExpr(bool n, Expr* a, Expr* p, Expr* e)
      : is_not_(n), arg_(a), pattern_string_(p), escape_string_(e) {}
  bool get_is_not() const { return is_not_; }
  const Expr* get_arg() const { return arg_.get(); }
  const Expr* get_pattern_string() const { return pattern_string_.get(); }
  const Expr* get_escape_string() const { return escape_string_.get(); }

  static std::shared_ptr<Analyzer::Expr> get(std::shared_ptr<Analyzer::Expr> arg_expr,
                                             std::shared_ptr<Analyzer::Expr> pattern_expr,
                                             std::shared_ptr<Analyzer::Expr> escape_expr,
                                             const bool is_not);
  std::string to_string() const override;

 private:
  bool is_not_;
  std::unique_ptr<Expr> arg_;
  std::unique_ptr<Expr> pattern_string_;
  std::unique_ptr<Expr> escape_string_;

  static void check_pattern_expr(const std::string& pattern_str, char escape_char);
  static bool translate_to_like_pattern(std::string& pattern_str, char escape_char);
};

class WidthBucketExpr : public Expr {
 public:
  WidthBucketExpr(Expr* t, Expr* l, Expr* u, Expr* p)
      : target_value_(t), lower_bound_(l), upper_bound_(u), partition_count_(p) {}
  const Expr* get_target_value() const { return target_value_.get(); }
  const Expr* get_lower_bound() const { return lower_bound_.get(); }
  const Expr* get_upper_bound() const { return upper_bound_.get(); }
  const Expr* get_partition_count() const { return partition_count_.get(); }

  static std::shared_ptr<Analyzer::Expr> get(
      std::shared_ptr<Analyzer::Expr> target_value,
      std::shared_ptr<Analyzer::Expr> lower_bound,
      std::shared_ptr<Analyzer::Expr> upper_bound,
      std::shared_ptr<Analyzer::Expr> partition_count);
  std::string to_string() const override;

 private:
  std::unique_ptr<Expr> target_value_;
  std::unique_ptr<Expr> lower_bound_;
  std::unique_ptr<Expr> upper_bound_;
  std::unique_ptr<Expr> partition_count_;
};

/*
 * @type LikelihoodExpr
 * @brief expression for LIKELY, UNLIKELY
 */
class LikelihoodExpr : public Expr {
 public:
  LikelihoodExpr(bool n, Expr* a, float l) : is_not_(n), arg_(a), likelihood_(l) {}
  bool get_is_not() const { return is_not_; }
  const Expr* get_arg() const { return arg_.get(); }
  float get_likelihood() const { return likelihood_; }

  static std::shared_ptr<Analyzer::Expr> get(std::shared_ptr<Analyzer::Expr> arg_expr,
                                             float likelihood,
                                             const bool is_not);
  std::string to_string() const override;

 private:
  bool is_not_;
  std::unique_ptr<Expr> arg_;
  float likelihood_;
};

/*
 * @type ExistsExpr
 * @brief expression for EXISTS (subquery)
 */
class ExistsExpr : public Expr {
 public:
  explicit ExistsExpr(QuerySpec* q) : query_(q) {}
  const QuerySpec* get_query() const { return query_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<QuerySpec> query_;
};

/*
 * @type ColumnRefExpr
 * @brief expression for a column reference
 */
class ColumnRefExpr : public Expr {
 public:
  explicit ColumnRefExpr(std::string* n1) : table_(nullptr), column_(n1) {}
  ColumnRefExpr(std::string* n1, std::string* n2) : table_(n1), column_(n2) {}
  const std::string* get_table() const { return table_.get(); }
  const std::string* get_column() const { return column_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<std::string> table_;
  std::unique_ptr<std::string> column_;  // can be nullptr in the t.* case
};

/*
 * @type FunctionRef
 * @brief expression for a function call
 */
class FunctionRef : public Expr {
 public:
  explicit FunctionRef(std::string* n) : name_(n), distinct_(false), arg_(nullptr) {}
  FunctionRef(std::string* n, Expr* a) : name_(n), distinct_(false), arg_(a) {}
  FunctionRef(std::string* n, bool d, Expr* a) : name_(n), distinct_(d), arg_(a) {}
  const std::string* get_name() const { return name_.get(); }
  bool get_distinct() const { return distinct_; }
  Expr* get_arg() const { return arg_.get(); }

  std::string to_string() const override;

 private:
  std::unique_ptr<std::string> name_;
  bool distinct_;              // only true for COUNT(DISTINCT x)
  std::unique_ptr<Expr> arg_;  // for COUNT, nullptr means '*'
};

class CastExpr : public Expr {
 public:
  CastExpr(Expr* a, SQLType* t) : arg_(a), target_type_(t) {}

  std::string to_string() const override {
    return "CAST(" + arg_->to_string() + " AS " + target_type_->to_string() + ")";
  }

 private:
  std::unique_ptr<Expr> arg_;
  std::unique_ptr<SQLType> target_type_;
};

class ExprPair : public Node {
 public:
  ExprPair(Expr* e1, Expr* e2) : expr1_(e1), expr2_(e2) {}
  const Expr* get_expr1() const { return expr1_.get(); }
  const Expr* get_expr2() const { return expr2_.get(); }

 private:
  std::unique_ptr<Expr> expr1_;
  std::unique_ptr<Expr> expr2_;
};

class CaseExpr : public Expr {
 public:
  CaseExpr(std::list<ExprPair*>* w, Expr* e) : else_expr_(e) {
    CHECK(w);
    for (const auto e : *w) {
      when_then_list_.emplace_back(e);
    }
    delete w;
  }

  static std::shared_ptr<Analyzer::Expr> normalize(
      const std::list<
          std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>&,
      const std::shared_ptr<Analyzer::Expr>);
  std::string to_string() const override;

 private:
  std::list<std::unique_ptr<ExprPair>> when_then_list_;
  std::unique_ptr<Expr> else_expr_;
};

/*
 * @type TableRefNode
 * @brief table reference in FROM clause
 */
class TableRefNode : public Node {
 public:
  explicit TableRefNode(std::string* t) : table_name_(t), range_var_(nullptr) {}
  TableRefNode(std::string* t, std::string* r) : table_name_(t), range_var_(r) {}
  const std::string* get_table_name() const { return table_name_.get(); }
  const std::string* get_range_var() const { return range_var_.get(); }
  std::string to_string() const;

 private:
  std::unique_ptr<std::string> table_name_;
  std::unique_ptr<std::string> range_var_;
};

/*
 * @type Stmt
 * @brief root super class for all SQL statements
 */
class Stmt : public Node {
  // intentionally empty
};

/*
 * @type DMLStmt
 * @brief DML Statements
 */
class DMLStmt : public Stmt {};

/*
 * @type TableElement
 * @brief elements in table definition
 */
class TableElement : public Node {
  // intentionally empty
};

/*
 * @type ColumnConstraintDef
 * @brief integrity constraint on a column
 */
class ColumnConstraintDef : public Node {
 public:
  ColumnConstraintDef(bool n, bool u, bool p, Literal* d)
      : notnull_(n), unique_(u), is_primarykey_(p), defaultval_(d) {}
  ColumnConstraintDef(Expr* c)
      : notnull_(false), unique_(false), is_primarykey_(false), check_condition_(c) {}
  ColumnConstraintDef(std::string* t, std::string* c)
      : notnull_(false)
      , unique_(false)
      , is_primarykey_(false)
      , foreign_table_(t)
      , foreign_column_(c) {}
  bool get_notnull() const { return notnull_; }
  bool get_unique() const { return unique_; }
  bool get_is_primarykey() const { return is_primarykey_; }
  const Literal* get_defaultval() const { return defaultval_.get(); }
  const Expr* get_check_condition() const { return check_condition_.get(); }
  const std::string* get_foreign_table() const { return foreign_table_.get(); }
  const std::string* get_foreign_column() const { return foreign_column_.get(); }

 private:
  bool notnull_;
  bool unique_;
  bool is_primarykey_;
  std::unique_ptr<Literal> defaultval_;
  std::unique_ptr<Expr> check_condition_;
  std::unique_ptr<std::string> foreign_table_;
  std::unique_ptr<std::string> foreign_column_;
};

/*
 * @type CompressDef
 * @brief Node for compression scheme definition
 */
class CompressDef : public Node, public ddl_utils::Encoding {
 public:
  CompressDef(std::string* n, int p) : ddl_utils::Encoding(n, p) {}
};

/*
 * @type ColumnDef
 * @brief Column definition
 */
class ColumnDef : public TableElement {
 public:
  ColumnDef(std::string* c, SQLType* t, CompressDef* cp, ColumnConstraintDef* cc)
      : column_name_(c), column_type_(t), compression_(cp), column_constraint_(cc) {}
  const std::string* get_column_name() const { return column_name_.get(); }
  SQLType* get_column_type() const { return column_type_.get(); }
  const CompressDef* get_compression() const { return compression_.get(); }
  const ColumnConstraintDef* get_column_constraint() const {
    return column_constraint_.get();
  }

 private:
  std::unique_ptr<std::string> column_name_;
  std::unique_ptr<SQLType> column_type_;
  std::unique_ptr<CompressDef> compression_;
  std::unique_ptr<ColumnConstraintDef> column_constraint_;
};

/*
 * @type TableConstraintDef
 * @brief integrity constraint for table
 */
class TableConstraintDef : public TableElement {
  // intentionally empty
};

/*
 * @type UniqueDef
 * @brief uniqueness constraint
 */
class UniqueDef : public TableConstraintDef {
 public:
  UniqueDef(bool p, std::list<std::string*>* cl) : is_primarykey_(p) {
    CHECK(cl);
    for (const auto s : *cl) {
      column_list_.emplace_back(s);
    }
    delete cl;
  }
  bool get_is_primarykey() const { return is_primarykey_; }
  const std::list<std::unique_ptr<std::string>>& get_column_list() const {
    return column_list_;
  }

 private:
  bool is_primarykey_;
  std::list<std::unique_ptr<std::string>> column_list_;
};

/*
 * @type ForeignKeyDef
 * @brief foreign key constraint
 */
class ForeignKeyDef : public TableConstraintDef {
 public:
  ForeignKeyDef(std::list<std::string*>* cl, std::string* t, std::list<std::string*>* fcl)
      : foreign_table_(t) {
    CHECK(cl);
    for (const auto s : *cl) {
      column_list_.emplace_back(s);
    }
    delete cl;
    if (fcl) {
      for (const auto s : *fcl) {
        foreign_column_list_.emplace_back(s);
      }
    }
    delete fcl;
  }
  const std::list<std::unique_ptr<std::string>>& get_column_list() const {
    return column_list_;
  }
  const std::string* get_foreign_table() const { return foreign_table_.get(); }
  const std::list<std::unique_ptr<std::string>>& get_foreign_column_list() const {
    return foreign_column_list_;
  }

 private:
  std::list<std::unique_ptr<std::string>> column_list_;
  std::unique_ptr<std::string> foreign_table_;
  std::list<std::unique_ptr<std::string>> foreign_column_list_;
};

/*
 * @type CheckDef
 * @brief Check constraint
 */
class CheckDef : public TableConstraintDef {
 public:
  CheckDef(Expr* c) : check_condition_(c) {}
  const Expr* get_check_condition() const { return check_condition_.get(); }

 private:
  std::unique_ptr<Expr> check_condition_;
};

/*
 * @type SharedDictionaryDef
 * @brief Shared dictionary hint. The underlying string dictionary will be shared with the
 * referenced column.
 */
class SharedDictionaryDef : public TableConstraintDef {
 public:
  SharedDictionaryDef(const std::string& column,
                      const std::string& foreign_table,
                      const std::string foreign_column)
      : column_(column), foreign_table_(foreign_table), foreign_column_(foreign_column) {}

  const std::string& get_column() const { return column_; }

  const std::string& get_foreign_table() const { return foreign_table_; }

  const std::string& get_foreign_column() const { return foreign_column_; }

 private:
  const std::string column_;
  const std::string foreign_table_;
  const std::string foreign_column_;
};

/*
 * @type NameValueAssign
 * @brief Assignment of a string value to a named attribute
 */
class NameValueAssign : public Node {
 public:
  NameValueAssign(std::string* n, Literal* v) : name_(n), value_(v) {}
  const std::string* get_name() const { return name_.get(); }
  const Literal* get_value() const { return value_.get(); }

 private:
  std::unique_ptr<std::string> name_;
  std::unique_ptr<Literal> value_;
};

/*
 * @type QueryExpr
 * @brief query expression
 */
class QueryExpr : public Node {};

/*
 * @type UnionQuery
 * @brief UNION or UNION ALL queries
 */
class UnionQuery : public QueryExpr {
 public:
  UnionQuery(bool u, QueryExpr* l, QueryExpr* r) : is_unionall_(u), left_(l), right_(r) {}
  bool get_is_unionall() const { return is_unionall_; }
  const QueryExpr* get_left() const { return left_.get(); }
  const QueryExpr* get_right() const { return right_.get(); }

 private:
  bool is_unionall_;
  std::unique_ptr<QueryExpr> left_;
  std::unique_ptr<QueryExpr> right_;
};

class SelectEntry : public Node {
 public:
  SelectEntry(Expr* e, std::string* r) : select_expr_(e), alias_(r) {}
  const Expr* get_select_expr() const { return select_expr_.get(); }
  const std::string* get_alias() const { return alias_.get(); }
  std::string to_string() const;

 private:
  std::unique_ptr<Expr> select_expr_;
  std::unique_ptr<std::string> alias_;
};

/*
 * @type QuerySpec
 * @brief a simple query
 */
class QuerySpec : public QueryExpr {
 public:
  QuerySpec(bool d,
            std::list<SelectEntry*>* s,
            std::list<TableRefNode*>* f,
            Expr* w,
            std::list<Expr*>* g,
            Expr* h)
      : is_distinct_(d), where_clause_(w), having_clause_(h) {
    if (s) {
      for (const auto e : *s) {
        select_clause_.emplace_back(e);
      }
      delete s;
    }
    CHECK(f);
    for (const auto e : *f) {
      from_clause_.emplace_back(e);
    }
    delete f;
    if (g) {
      for (const auto e : *g) {
        groupby_clause_.emplace_back(e);
      }
      delete g;
    }
  }
  bool get_is_distinct() const { return is_distinct_; }
  const std::list<std::unique_ptr<SelectEntry>>& get_select_clause() const {
    return select_clause_;
  }
  const std::list<std::unique_ptr<TableRefNode>>& get_from_clause() const {
    return from_clause_;
  }
  const Expr* get_where_clause() const { return where_clause_.get(); }
  const std::list<std::unique_ptr<Expr>>& get_groupby_clause() const {
    return groupby_clause_;
  }
  const Expr* get_having_clause() const { return having_clause_.get(); }

  std::string to_string() const;

 private:
  bool is_distinct_;
  std::list<std::unique_ptr<SelectEntry>> select_clause_; /* nullptr means SELECT * */
  std::list<std::unique_ptr<TableRefNode>> from_clause_;
  std::unique_ptr<Expr> where_clause_;
  std::list<std::unique_ptr<Expr>> groupby_clause_;
  std::unique_ptr<Expr> having_clause_;
};

/*
 * @type OrderSpec
 * @brief order spec for a column in ORDER BY clause
 */
class OrderSpec : public Node {
 public:
  OrderSpec(int n, ColumnRefExpr* c, bool d, bool f)
      : colno_(n), column_(c), is_desc_(d), nulls_first_(f) {}
  int get_colno() const { return colno_; }
  const ColumnRefExpr* get_column() const { return column_.get(); }
  bool get_is_desc() const { return is_desc_; }
  bool get_nulls_first() const { return nulls_first_; }

 private:
  int colno_; /* 0 means use column name */
  std::unique_ptr<ColumnRefExpr> column_;
  bool is_desc_;
  bool nulls_first_;
};

/*
 * @type SelectStmt
 * @brief SELECT statement
 */
class SelectStmt : public DMLStmt {
 public:
  SelectStmt(QueryExpr* q, std::list<OrderSpec*>* o, int64_t l, int64_t f)
      : query_expr_(q), limit_(l), offset_(f) {
    if (o) {
      for (const auto e : *o) {
        orderby_clause_.emplace_back(e);
      }
      delete o;
    }
  }
  const QueryExpr* get_query_expr() const { return query_expr_.get(); }
  const std::list<std::unique_ptr<OrderSpec>>& get_orderby_clause() const {
    return orderby_clause_;
  }

 private:
  std::unique_ptr<QueryExpr> query_expr_;
  std::list<std::unique_ptr<OrderSpec>> orderby_clause_;
  int64_t limit_;
  int64_t offset_;
};

template <typename LITERAL_TYPE>
struct DefaultValidate {};

template <>
struct DefaultValidate<IntLiteral> {
  template <typename T>
  decltype(auto) operator()(T t) {
    const std::string property_name(boost::to_upper_copy<std::string>(*t->get_name()));
    if (!dynamic_cast<const IntLiteral*>(t->get_value())) {
      CIDER_THROW(CiderCompileException, property_name + " must be an integer literal.");
    }
    const auto val = static_cast<const IntLiteral*>(t->get_value())->get_intval();
    if (val <= 0) {
      CIDER_THROW(CiderCompileException, property_name + " must be a positive number.");
    }
    return val;
  }
};

template <>
struct DefaultValidate<StringLiteral> {
  template <typename T>
  decltype(auto) operator()(T t) {
    const auto val = static_cast<const StringLiteral*>(t->get_value())->get_stringval();
    CHECK(val);
    const auto val_upper = boost::to_upper_copy<std::string>(*val);
    return val_upper;
  }
};

struct CaseSensitiveValidate {
  template <typename T>
  decltype(auto) operator()(T t) {
    const auto val = static_cast<const StringLiteral*>(t->get_value())->get_stringval();
    CHECK(val);
    return *val;
  }
};

}  // namespace Parser

#endif  // PARSERNODE_H_
