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

#ifndef TYPE_PLAN_STRING_OP_EXPR_H
#define TYPE_PLAN_STRING_OP_EXPR_H

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "type/plan/SqlTypes.h"
#include "type/plan/ColumnExpr.h"
#include "type/plan/ConstantExpr.h"
#include "type/plan/Expr.h"
#include "util/sqldefs.h"

namespace Analyzer {

class StringOper : public Expr {
 public:
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

  explicit StringOper(const std::shared_ptr<StringOper>& other_string_oper)
      : Expr(other_string_oper->get_type_info()) {
    kind_ = other_string_oper->kind_;
    args_ = other_string_oper->args_;
    chained_string_op_exprs_ = other_string_oper->chained_string_op_exprs_;
  }

  SqlStringOpKind get_kind() const { return kind_; }

  ExprPtrRefVector get_children_reference() override {
    // For StringOper Expr and its inherited classes, get_children_reference by default
    // returns references to the StringOper's arguments. it is implemented here
    // in the base class for code reusability and access to private member args_
    return get_argument_references();
  }

  size_t getArity() const { return args_.size(); }

  size_t getLiteralsArity() const {
    size_t num_literals{0UL};
    for (const auto& arg : args_) {
      if (dynamic_cast<const Analyzer::Constant*>(arg.get())) {
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

 protected:
  std::vector<std::shared_ptr<Analyzer::Expr>> foldLiteralStrCasts(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands,
      int start_idx = 1);

  virtual ExprPtrRefVector get_argument_references() {
    ExprPtrRefVector ret;
    for (auto& arg : args_) {
      ret.push_back(&arg);
    }
    return ret;
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

 public:
  JITExprValue& codegen(CodegenContext& context) override { UNREACHABLE(); }
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

  explicit SubstringStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::SUBSTRING,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit SubstringStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
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

  JITExprValue& codegen(CodegenContext& context) override;
};

class LowerStringOper : public StringOper {
 public:
  explicit LowerStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::LOWER,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit LowerStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::LOWER,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit LowerStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
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

  JITExprValue& codegen(CodegenContext& context) override;
};

class UpperStringOper : public StringOper {
 public:
  explicit UpperStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::UPPER,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit UpperStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::UPPER,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit UpperStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
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

  JITExprValue& codegen(CodegenContext& context) override;
};

class CharLengthStringOper : public StringOper {
 public:
  explicit CharLengthStringOper(const std::shared_ptr<Analyzer::Expr>& operand)
      : StringOper(SqlStringOpKind::CHAR_LENGTH,
                   {operand},
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit CharLengthStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::CHAR_LENGTH,
                   operands,
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit CharLengthStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
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

  JITExprValue& codegen(CodegenContext& context) override;
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

  explicit ConcatStringOper(const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(getConcatOpKind(operands),
                   rearrangeOperands(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit ConcatStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 2UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY, OperandTypeFamily::STRING_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"lhs", "rhs"};
    return names;
  }

  JITExprValue& codegen(CodegenContext& context) override;

 private:
  // TODO: Deprecated. The following 3 methods are only used in template-based codegen
  // the templated interface supports at most one variable column input in stringop
  // these methods are used to ensure that the variable input is always the first arg
  // they are not required in nextgen because nextgen supports two variable inputs

  // To be removed after migration to nextgen.
  bool isLiteralOrCastLiteral(const Analyzer::Expr* operand);

  // To be removed after migration to nextgen.
  SqlStringOpKind getConcatOpKind(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands);

  // To be removed after migration to nextgen.
  std::vector<std::shared_ptr<Analyzer::Expr>> rearrangeOperands(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands);
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

  explicit TrimStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
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

  JITExprValue& codegen(CodegenContext& context) override;

 private:
  SqlStringOpKind checkOpKindValidity(const SqlStringOpKind& op_kind) {
    CHECK(op_kind == SqlStringOpKind::TRIM || op_kind == SqlStringOpKind::LTRIM ||
          op_kind == SqlStringOpKind::RTRIM);
    return op_kind;
  }
};

class SplitPartStringOper : public StringOper {
 public:
  /**
   * split(input, delimiter)[split_part]
   * split_part(input, delimiter, split_part)
   */
  SplitPartStringOper(const std::shared_ptr<Analyzer::Expr>& operand,
                      const std::shared_ptr<Analyzer::Expr>& delimiter,
                      const std::shared_ptr<Analyzer::Expr>& split_part)
      : StringOper(SqlStringOpKind::SPLIT_PART,
                   foldLiteralStrCasts({operand, delimiter, split_part}),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  /**
   * split(input, delimiter, limit)[split_part]
   * returns a list of size at most `limit`, the last element always contains everything
   * left in the string
   */
  SplitPartStringOper(const std::shared_ptr<Analyzer::Expr>& operand,
                      const std::shared_ptr<Analyzer::Expr>& delimiter,
                      const std::shared_ptr<Analyzer::Expr>& limit,
                      const std::shared_ptr<Analyzer::Expr>& split_part)
      : StringOper(SqlStringOpKind::SPLIT_PART,
                   foldLiteralStrCasts({operand, delimiter, limit, split_part}),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit SplitPartStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::SPLIT_PART,
                   foldLiteralStrCasts(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit SplitPartStringOper(const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  JITExprValue& codegen(CodegenContext& context) override;

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 3UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::INT_FAMILY,
            OperandTypeFamily::INT_FAMILY};
  }
  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{
        "operand", "delimiter", "limit_or_splitpart", "splitpart"};
    return names;
  }
};

class RegexpReplaceStringOper : public StringOper {
 public:
  explicit RegexpReplaceStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
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
      : StringOper(
            SqlStringOpKind::REGEXP_REPLACE,
            foldLiteralStrCasts({input, pattern, replacement, position, occurrence}),
            getMinArgs(),
            getExpectedTypeFamilies(),
            getArgNames()) {}

  explicit RegexpReplaceStringOper(
      const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  JITExprValue& codegen(CodegenContext& context) override;

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
};

class RegexpExtractStringOper : public StringOper {
 public:
  RegexpExtractStringOper(const std::shared_ptr<Analyzer::Expr>& input,
                          const std::shared_ptr<Analyzer::Expr>& pattern,
                          const std::shared_ptr<Analyzer::Expr>& group)
      : StringOper(SqlStringOpKind::REGEXP_EXTRACT,
                   foldLiteralStrCasts({input, pattern, group}),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit RegexpExtractStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::REGEXP_EXTRACT,
                   foldLiteralStrCasts(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit RegexpExtractStringOper(
      const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  JITExprValue& codegen(CodegenContext& context) override;

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  size_t getMinArgs() const override { return 3UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::INT_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"input", "pattern", "group"};
    return names;
  }
};

class RegexpSubstrStringOper : public StringOper {
 public:
  RegexpSubstrStringOper(const std::shared_ptr<Analyzer::Expr>& input,
                         const std::shared_ptr<Analyzer::Expr>& pattern,
                         const std::shared_ptr<Analyzer::Expr>& position,
                         const std::shared_ptr<Analyzer::Expr>& occurrence)
      : StringOper(SqlStringOpKind::REGEXP_SUBSTR,
                   foldLiteralStrCasts({input, pattern, position, occurrence}),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit RegexpSubstrStringOper(
      const std::vector<std::shared_ptr<Analyzer::Expr>>& operands)
      : StringOper(SqlStringOpKind::REGEXP_SUBSTR,
                   foldLiteralStrCasts(operands),
                   getMinArgs(),
                   getExpectedTypeFamilies(),
                   getArgNames()) {}

  explicit RegexpSubstrStringOper(
      const std::shared_ptr<Analyzer::StringOper>& string_oper)
      : StringOper(string_oper) {}

  std::shared_ptr<Analyzer::Expr> deep_copy() const override;

  JITExprValue& codegen(CodegenContext& context) override;

  size_t getMinArgs() const override { return 4UL; }

  std::vector<OperandTypeFamily> getExpectedTypeFamilies() const override {
    return {OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::STRING_FAMILY,
            OperandTypeFamily::INT_FAMILY,
            OperandTypeFamily::INT_FAMILY};
  }

  const std::vector<std::string>& getArgNames() const override {
    static std::vector<std::string> names{"input", "pattern", "position", "occurrence"};
    return names;
  }
};

}  // namespace Analyzer

#endif
