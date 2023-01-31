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
#include "ParserNode.h"
#include <rapidjson/document.h>
#include <boost/algorithm/string.hpp>
#include <regex>
#include <stdexcept>
#include <string>
#include "cider/CiderException.h"
#include "util/measure.h"

using namespace std::string_literals;

namespace Parser {

std::shared_ptr<Analyzer::Expr> StringLiteral::analyzeValue(const std::string& stringval,
                                                            const bool is_null) {
  if (!is_null) {
    const SQLTypeInfo ti(kVARCHAR, stringval.length(), 0, true);
    Datum d;
    d.stringval = new std::string(stringval);
    return makeExpr<Analyzer::Constant>(ti, false, d);
  }
  // Null value
  return makeExpr<Analyzer::Constant>(kVARCHAR, true);
}

std::shared_ptr<Analyzer::Expr> IntLiteral::analyzeValue(const int64_t intval) {
  SQLTypes t;
  Datum d;
  if (intval >= INT16_MIN && intval <= INT16_MAX) {
    t = kSMALLINT;
    d.smallintval = (int16_t)intval;
  } else if (intval >= INT32_MIN && intval <= INT32_MAX) {
    t = kINT;
    d.intval = (int32_t)intval;
  } else {
    t = kBIGINT;
    d.bigintval = intval;
  }
  return makeExpr<Analyzer::Constant>(t, false, d);
}

std::shared_ptr<Analyzer::Expr> FixedPtLiteral::analyzeValue(const int64_t numericval,
                                                             const int scale,
                                                             const int precision) {
  SQLTypeInfo ti(kNUMERIC, 0, 0, false);
  ti.set_scale(scale);
  ti.set_precision(precision);
  Datum d;
  d.bigintval = numericval;
  return makeExpr<Analyzer::Constant>(ti, false, d);
}

std::shared_ptr<Analyzer::Expr> TimestampLiteral::get(const int64_t timestampval) {
  Datum d;
  d.bigintval = timestampval;
  return makeExpr<Analyzer::Constant>(kTIMESTAMP, false, d);
}

std::shared_ptr<Analyzer::Expr> UserLiteral::get(const std::string& user) {
  Datum d;
  d.stringval = new std::string(user);
  return makeExpr<Analyzer::Constant>(kTEXT, false, d);
}

std::string ArrayLiteral::to_string() const {
  std::string str = "{";
  bool notfirst = false;
  for (auto& p : value_list_) {
    if (notfirst) {
      str += ", ";
    } else {
      notfirst = true;
    }
    str += p->to_string();
  }
  str += "}";
  return str;
}

std::shared_ptr<Analyzer::Expr> OperExpr::normalize(
    const SQLOps optype,
    const SQLQualifier qual,
    std::shared_ptr<Analyzer::Expr> left_expr,
    std::shared_ptr<Analyzer::Expr> right_expr) {
  if (left_expr->get_type_info().is_date_in_days() ||
      right_expr->get_type_info().is_date_in_days()) {
    // Do not propogate encoding
    left_expr = left_expr->decompress();
    right_expr = right_expr->decompress();
  }
  const auto& left_type = left_expr->get_type_info();
  auto right_type = right_expr->get_type_info();
  if (qual != kONE) {
    // subquery not supported yet.
    CHECK(!std::dynamic_pointer_cast<Analyzer::Subquery>(right_expr));
    if (right_type.get_type() != kARRAY) {
      CIDER_THROW(
          CiderCompileException,
          "Existential or universal qualifiers can only be used in front of a subquery "
          "or an "
          "expression of array type.");
    }
    right_type = right_type.get_elem_type();
  }
  SQLTypeInfo new_left_type;
  SQLTypeInfo new_right_type;
  auto result_type = Analyzer::BinOper::analyze_type_info(
      optype, left_type, right_type, &new_left_type, &new_right_type);
  if (result_type.is_timeinterval()) {
    return makeExpr<Analyzer::BinOper>(
        result_type, false, optype, qual, left_expr, right_expr);
  }
  if (left_type != new_left_type) {
    left_expr = left_expr->add_cast(new_left_type);
  }
  if (right_type != new_right_type) {
    if (qual == kONE) {
      right_expr = right_expr->add_cast(new_right_type);
    } else {
      right_expr = right_expr->add_cast(new_right_type.get_array_type());
    }
  }

  if (IS_COMPARISON(optype)) {
    if (new_left_type.get_compression() == kENCODING_DICT &&
        new_right_type.get_compression() == kENCODING_DICT &&
        new_left_type.get_comp_param() == new_right_type.get_comp_param()) {
      // do nothing
    } else if (new_left_type.get_compression() == kENCODING_DICT &&
               new_right_type.get_compression() == kENCODING_NONE) {
      SQLTypeInfo ti(new_right_type);
      ti.set_compression(new_left_type.get_compression());
      ti.set_comp_param(new_left_type.get_comp_param());
      ti.set_fixed_size();
      right_expr = right_expr->add_cast(ti);
    } else if (new_right_type.get_compression() == kENCODING_DICT &&
               new_left_type.get_compression() == kENCODING_NONE) {
      SQLTypeInfo ti(new_left_type);
      ti.set_compression(new_right_type.get_compression());
      ti.set_comp_param(new_right_type.get_comp_param());
      ti.set_fixed_size();
      left_expr = left_expr->add_cast(ti);
    } else {
      left_expr = left_expr->decompress();
      right_expr = right_expr->decompress();
    }
  } else {
    left_expr = left_expr->decompress();
    right_expr = right_expr->decompress();
  }
  bool has_agg = (left_expr->get_contains_agg() || right_expr->get_contains_agg());
  return makeExpr<Analyzer::BinOper>(
      result_type, has_agg, optype, qual, left_expr, right_expr);
}

void LikeExpr::check_like_expr(const std::string& like_str, char escape_char) {
  if (like_str.back() == escape_char) {
    CIDER_THROW(CiderCompileException,
                "LIKE pattern must not end with escape character.");
  }
}

bool LikeExpr::test_is_simple_expr(const std::string& like_str, char escape_char) {
  // if not bounded by '%' then not a simple string
  if (like_str.size() < 2 || like_str[0] != '%' || like_str[like_str.size() - 1] != '%') {
    return false;
  }
  // if the last '%' is escaped then not a simple string
  if (like_str[like_str.size() - 2] == escape_char &&
      like_str[like_str.size() - 3] != escape_char) {
    return false;
  }
  for (size_t i = 1; i < like_str.size() - 1; i++) {
    if (like_str[i] == '%' || like_str[i] == '_' || like_str[i] == '[' ||
        like_str[i] == ']') {
      if (like_str[i - 1] != escape_char) {
        return false;
      }
    }
  }
  return true;
}

void LikeExpr::erase_cntl_chars(std::string& like_str, char escape_char) {
  char prev_char = '\0';
  // easier to create new string of allowable chars
  // rather than erase chars from
  // existing string
  std::string new_str;
  for (char& cur_char : like_str) {
    if (cur_char == '%' || cur_char == escape_char) {
      if (prev_char != escape_char) {
        prev_char = cur_char;
        continue;
      }
    }
    new_str.push_back(cur_char);
    prev_char = cur_char;
  }
  like_str = new_str;
}

std::shared_ptr<Analyzer::Expr> LikeExpr::get(std::shared_ptr<Analyzer::Expr> arg_expr,
                                              std::shared_ptr<Analyzer::Expr> like_expr,
                                              std::shared_ptr<Analyzer::Expr> escape_expr,
                                              const bool is_ilike,
                                              const bool is_not) {
  if (!arg_expr->get_type_info().is_string()) {
    CIDER_THROW(CiderCompileException,
                "expression before LIKE must be of a string type.");
  }
  if (!like_expr->get_type_info().is_string()) {
    CIDER_THROW(CiderCompileException, "expression after LIKE must be of a string type.");
  }
  char escape_char = '\\';
  if (escape_expr != nullptr) {
    if (!escape_expr->get_type_info().is_string()) {
      CIDER_THROW(CiderCompileException,
                  "expression after ESCAPE must be of a string type.");
    }
    if (!escape_expr->get_type_info().is_string()) {
      CIDER_THROW(CiderCompileException,
                  "expression after ESCAPE must be of a string type.");
    }
    auto c = std::dynamic_pointer_cast<Analyzer::Constant>(escape_expr);
    if (c != nullptr && c->get_constval().stringval->length() > 1) {
      CIDER_THROW(CiderCompileException,
                  "String after ESCAPE must have a single character.");
    }
    escape_char = (*c->get_constval().stringval)[0];
  }
  auto c = std::dynamic_pointer_cast<Analyzer::Constant>(like_expr);
  bool is_simple = false;
  if (c != nullptr) {
    std::string& pattern = *c->get_constval().stringval;
    if (is_ilike) {
      std::transform(pattern.begin(), pattern.end(), pattern.begin(), ::tolower);
    }
    check_like_expr(pattern, escape_char);
    is_simple = test_is_simple_expr(pattern, escape_char);
    if (is_simple) {
      erase_cntl_chars(pattern, escape_char);
    }
  }
  std::shared_ptr<Analyzer::Expr> result = makeExpr<Analyzer::LikeExpr>(
      arg_expr->decompress(), like_expr, escape_expr, is_ilike, is_simple);
  if (is_not) {
    result = makeExpr<Analyzer::UOper>(kBOOLEAN, kNOT, result);
  }
  return result;
}

void RegexpExpr::check_pattern_expr(const std::string& pattern_str, char escape_char) {
  if (pattern_str.back() == escape_char) {
    CIDER_THROW(CiderCompileException,
                "REGEXP pattern must not end with escape character.");
  }
}

bool RegexpExpr::translate_to_like_pattern(std::string& pattern_str, char escape_char) {
  char prev_char = '\0';
  char prev_prev_char = '\0';
  std::string like_str;
  for (char& cur_char : pattern_str) {
    if (prev_char == escape_char || isalnum(cur_char) || cur_char == ' ' ||
        cur_char == '.') {
      like_str.push_back((cur_char == '.') ? '_' : cur_char);
      prev_prev_char = prev_char;
      prev_char = cur_char;
      continue;
    }
    if (prev_char == '.' && prev_prev_char != escape_char) {
      if (cur_char == '*' || cur_char == '+') {
        if (cur_char == '*') {
          like_str.pop_back();
        }
        // .* --> %
        // .+ --> _%
        like_str.push_back('%');
        prev_prev_char = prev_char;
        prev_char = cur_char;
        continue;
      }
    }
    return false;
  }
  pattern_str = like_str;
  return true;
}

std::shared_ptr<Analyzer::Expr> RegexpExpr::get(
    std::shared_ptr<Analyzer::Expr> arg_expr,
    std::shared_ptr<Analyzer::Expr> pattern_expr,
    std::shared_ptr<Analyzer::Expr> escape_expr,
    const bool is_not) {
  if (!arg_expr->get_type_info().is_string()) {
    CIDER_THROW(CiderCompileException,
                "expression before REGEXP must be of a string type.");
  }
  if (!pattern_expr->get_type_info().is_string()) {
    CIDER_THROW(CiderCompileException,
                "expression after REGEXP must be of a string type.");
  }
  char escape_char = '\\';
  if (escape_expr != nullptr) {
    if (!escape_expr->get_type_info().is_string()) {
      CIDER_THROW(CiderCompileException,
                  "expression after ESCAPE must be of a string type.");
    }
    if (!escape_expr->get_type_info().is_string()) {
      CIDER_THROW(CiderCompileException,
                  "expression after ESCAPE must be of a string type.");
    }
    auto c = std::dynamic_pointer_cast<Analyzer::Constant>(escape_expr);
    if (c != nullptr && c->get_constval().stringval->length() > 1) {
      CIDER_THROW(CiderCompileException,
                  "String after ESCAPE must have a single character.");
    }
    escape_char = (*c->get_constval().stringval)[0];
    if (escape_char != '\\') {
      CIDER_THROW(CiderCompileException, "Only supporting '\\' escape character.");
    }
  }
  auto c = std::dynamic_pointer_cast<Analyzer::Constant>(pattern_expr);
  if (c != nullptr) {
    std::string& pattern = *c->get_constval().stringval;
    if (translate_to_like_pattern(pattern, escape_char)) {
      return LikeExpr::get(arg_expr, pattern_expr, escape_expr, false, is_not);
    }
  }
  std::shared_ptr<Analyzer::Expr> result =
      makeExpr<Analyzer::RegexpExpr>(arg_expr->decompress(), pattern_expr, escape_expr);
  if (is_not) {
    result = makeExpr<Analyzer::UOper>(kBOOLEAN, kNOT, result);
  }
  return result;
}

std::shared_ptr<Analyzer::Expr> LikelihoodExpr::get(
    std::shared_ptr<Analyzer::Expr> arg_expr,
    float likelihood,
    const bool is_not) {
  if (!arg_expr->get_type_info().is_boolean()) {
    CIDER_THROW(CiderCompileException, "likelihood expression expects boolean type.");
  }
  std::shared_ptr<Analyzer::Expr> result = makeExpr<Analyzer::LikelihoodExpr>(
      arg_expr->decompress(), is_not ? 1 - likelihood : likelihood);
  return result;
}

std::shared_ptr<Analyzer::Expr> WidthBucketExpr::get(
    std::shared_ptr<Analyzer::Expr> target_value,
    std::shared_ptr<Analyzer::Expr> lower_bound,
    std::shared_ptr<Analyzer::Expr> upper_bound,
    std::shared_ptr<Analyzer::Expr> partition_count) {
  std::shared_ptr<Analyzer::Expr> result = makeExpr<Analyzer::WidthBucketExpr>(
      target_value, lower_bound, upper_bound, partition_count);
  return result;
}

namespace {

bool expr_is_null(const Analyzer::Expr* expr) {
  if (expr->get_type_info().get_type() == kNULLT) {
    return true;
  }
  const auto const_expr = dynamic_cast<const Analyzer::Constant*>(expr);
  return const_expr && const_expr->get_is_null();
}

}  // namespace

std::shared_ptr<Analyzer::Expr> CaseExpr::normalize(
    const std::list<std::pair<std::shared_ptr<Analyzer::Expr>,
                              std::shared_ptr<Analyzer::Expr>>>& expr_pair_list,
    const std::shared_ptr<Analyzer::Expr> else_e_in) {
  SQLTypeInfo ti;
  bool has_agg = false;
  std::set<int> dictionary_ids;
  bool has_none_encoded_str_projection = false;

  for (auto& p : expr_pair_list) {
    auto e1 = p.first;
    // When the conditional expression is ColumnVar(boolean type), The post-update column
    // type strategy will make this checker failed at the first time.
    // CHECK(e1->get_type_info().is_boolean());
    auto e2 = p.second;
    if (e2->get_type_info().is_string()) {
      if (e2->get_type_info().is_dict_encoded_string()) {
        dictionary_ids.insert(e2->get_type_info().get_comp_param());
        // allow literals to potentially fall down the transient path
      } else if (std::dynamic_pointer_cast<const Analyzer::ColumnVar>(e2)) {
        has_none_encoded_str_projection = true;
      }
    }

    if (ti.get_type() == kNULLT) {
      ti = e2->get_type_info();
    } else if (e2->get_type_info().get_type() == kNULLT) {
      ti.set_notnull(false);
      e2->set_type_info(ti);
    } else if (ti != e2->get_type_info()) {
      if (ti.is_string() && e2->get_type_info().is_string()) {
        ti = Analyzer::BinOper::common_string_type(ti, e2->get_type_info());
      } else if (ti.is_number() && e2->get_type_info().is_number()) {
        ti = Analyzer::BinOper::common_numeric_type(ti, e2->get_type_info());
      } else if (ti.is_boolean() && e2->get_type_info().is_boolean()) {
        ti = Analyzer::BinOper::common_numeric_type(ti, e2->get_type_info());
      } else {
        CIDER_THROW(
            CiderCompileException,
            "expressions in THEN clause must be of the same or compatible types.");
      }
    }
    if (e2->get_contains_agg()) {
      has_agg = true;
    }
  }
  auto else_e = else_e_in;
  if (else_e) {
    if (else_e->get_contains_agg()) {
      has_agg = true;
    }
    if (expr_is_null(else_e.get())) {
      ti.set_notnull(false);
      else_e->set_type_info(ti);
    } else if (ti != else_e->get_type_info()) {
      if (else_e->get_type_info().is_string()) {
        if (else_e->get_type_info().is_dict_encoded_string()) {
          dictionary_ids.insert(else_e->get_type_info().get_comp_param());
          // allow literals to potentially fall down the transient path
        } else if (std::dynamic_pointer_cast<const Analyzer::ColumnVar>(else_e)) {
          has_none_encoded_str_projection = true;
        }
      }
      ti.set_notnull(false);
      if (ti.is_string() && else_e->get_type_info().is_string()) {
        ti = Analyzer::BinOper::common_string_type(ti, else_e->get_type_info());
      } else if (ti.is_number() && else_e->get_type_info().is_number()) {
        ti = Analyzer::BinOper::common_numeric_type(ti, else_e->get_type_info());
      } else if (ti.is_boolean() && else_e->get_type_info().is_boolean()) {
        ti = Analyzer::BinOper::common_numeric_type(ti, else_e->get_type_info());
      } else if (get_logical_type_info(ti) !=
                 get_logical_type_info(else_e->get_type_info())) {
        CIDER_THROW(
            CiderCompileException,
            // types differing by encoding will be resolved at decode

            "expressions in ELSE clause must be of the same or compatible types as those "
            "in the THEN clauses.");
      }
    }
  }
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      cast_expr_pair_list;
  for (auto p : expr_pair_list) {
    ti.set_notnull(false);
    cast_expr_pair_list.emplace_back(p.first, p.second->add_cast(ti));
  }
  if (else_e != nullptr) {
    else_e = else_e->add_cast(ti);
  } else {
    Datum d;
    // always create an else expr so that executor doesn't need to worry about it
    ti.set_notnull(false);
    else_e = makeExpr<Analyzer::Constant>(ti, true, d);
  }
  if (ti.get_type() == kNULLT) {
    CIDER_THROW(CiderCompileException,
                "Can't deduce the type for case expressions, all branches null");
  }

  auto case_expr = makeExpr<Analyzer::CaseExpr>(ti, has_agg, cast_expr_pair_list, else_e);
  if (ti.get_compression() != kENCODING_DICT && dictionary_ids.size() == 1 &&
      *(dictionary_ids.begin()) > 0 && !has_none_encoded_str_projection) {
    // the above logic makes two assumptions when strings are present. 1) that all types
    // in the case statement are either null or strings, and 2) that none-encoded strings
    // will always win out over dict encoding. If we only have one dictionary, and that
    // dictionary is not a transient dictionary, we can cast the entire case to be dict
    // encoded and use transient dictionaries for any literals
    ti.set_compression(kENCODING_DICT);
    ti.set_comp_param(*dictionary_ids.begin());
    case_expr->add_cast(ti);
  }
  return case_expr;
}

std::string CaseExpr::to_string() const {
  std::string str("CASE ");
  for (auto& p : when_then_list_) {
    str += "WHEN " + p->get_expr1()->to_string() + " THEN " +
           p->get_expr2()->to_string() + " ";
  }
  if (else_expr_ != nullptr) {
    str += "ELSE " + else_expr_->to_string();
  }
  str += " END";
  return str;
}

std::string SelectEntry::to_string() const {
  std::string str = select_expr_->to_string();
  if (alias_ != nullptr) {
    str += " AS " + *alias_;
  }
  return str;
}

std::string TableRefNode::to_string() const {
  std::string str = *table_name_;
  if (range_var_ != nullptr) {
    str += " " + *range_var_;
  }
  return str;
}

std::string ColumnRefExpr::to_string() const {
  std::string str;
  if (table_ == nullptr) {
    str = *column_;
  } else if (column_ == nullptr) {
    str = *table_ + ".*";
  } else {
    str = *table_ + "." + *column_;
  }
  return str;
}

std::string OperExpr::to_string() const {
  std::string op_str[] = {
      "=", "===", "<>", "<", ">", "<=", ">=", " AND ", " OR ", "NOT", "-", "+", "*", "/"};
  std::string str;
  if (optype_ == kUMINUS) {
    str = "-(" + left_->to_string() + ")";
  } else if (optype_ == kNOT) {
    str = "NOT (" + left_->to_string() + ")";
  } else if (optype_ == kARRAY_AT) {
    str = left_->to_string() + "[" + right_->to_string() + "]";
  } else if (optype_ == kUNNEST) {
    str = "UNNEST(" + left_->to_string() + ")";
  } else if (optype_ == kIN) {
    str = "(" + left_->to_string() + " IN " + right_->to_string() + ")";
  } else {
    str = "(" + left_->to_string() + op_str[optype_] + right_->to_string() + ")";
  }
  return str;
}

std::string InExpr::to_string() const {
  std::string str = arg_->to_string();
  if (is_not_) {
    str += " NOT IN ";
  } else {
    str += " IN ";
  }
  return str;
}

std::string ExistsExpr::to_string() const {
  return "EXISTS (" + query_->to_string() + ")";
}

std::string SubqueryExpr::to_string() const {
  std::string str;
  str = "(";
  str += query_->to_string();
  str += ")";
  return str;
}

std::string IsNullExpr::to_string() const {
  std::string str = arg_->to_string();
  if (is_not_) {
    str += " IS NOT NULL";
  } else {
    str += " IS NULL";
  }
  return str;
}

std::string InSubquery::to_string() const {
  std::string str = InExpr::to_string();
  str += subquery_->to_string();
  return str;
}

std::string InValues::to_string() const {
  std::string str = InExpr::to_string() + "(";
  bool notfirst = false;
  for (auto& p : value_list_) {
    if (notfirst) {
      str += ", ";
    } else {
      notfirst = true;
    }
    str += p->to_string();
  }
  str += ")";
  return str;
}

std::string BetweenExpr::to_string() const {
  std::string str = arg_->to_string();
  if (is_not_) {
    str += " NOT BETWEEN ";
  } else {
    str += " BETWEEN ";
  }
  str += lower_->to_string() + " AND " + upper_->to_string();
  return str;
}

std::string CharLengthExpr::to_string() const {
  std::string str;
  if (calc_encoded_length_) {
    str = "CHAR_LENGTH (" + arg_->to_string() + ")";
  } else {
    str = "LENGTH (" + arg_->to_string() + ")";
  }
  return str;
}

std::string CardinalityExpr::to_string() const {
  std::string str = "CARDINALITY(" + arg_->to_string() + ")";
  return str;
}

std::string LikeExpr::to_string() const {
  std::string str = arg_->to_string();
  if (is_not_) {
    str += " NOT LIKE ";
  } else {
    str += " LIKE ";
  }
  str += like_string_->to_string();
  if (escape_string_ != nullptr) {
    str += " ESCAPE " + escape_string_->to_string();
  }
  return str;
}

std::string RegexpExpr::to_string() const {
  std::string str = arg_->to_string();
  if (is_not_) {
    str += " NOT REGEXP ";
  } else {
    str += " REGEXP ";
  }
  str += pattern_string_->to_string();
  if (escape_string_ != nullptr) {
    str += " ESCAPE " + escape_string_->to_string();
  }
  return str;
}

std::string WidthBucketExpr::to_string() const {
  std::string str = " WIDTH_BUCKET ";
  str += target_value_->to_string();
  str += " ";
  str += lower_bound_->to_string();
  str += " ";
  str += upper_bound_->to_string();
  str += " ";
  str += partition_count_->to_string();
  str += " ";
  return str;
}

std::string LikelihoodExpr::to_string() const {
  std::string str = " LIKELIHOOD ";
  str += arg_->to_string();
  str += " ";
  str += std::to_string(is_not_ ? 1.0 - likelihood_ : likelihood_);
  return str;
}

std::string FunctionRef::to_string() const {
  std::string str = *name_ + "(";
  if (distinct_) {
    str += "DISTINCT ";
  }
  if (arg_ == nullptr) {
    str += "*)";
  } else {
    str += arg_->to_string() + ")";
  }
  return str;
}

std::string QuerySpec::to_string() const {
  std::string query_str = "SELECT ";
  if (is_distinct_) {
    query_str += "DISTINCT ";
  }
  if (select_clause_.empty()) {
    query_str += "* ";
  } else {
    bool notfirst = false;
    for (auto& p : select_clause_) {
      if (notfirst) {
        query_str += ", ";
      } else {
        notfirst = true;
      }
      query_str += p->to_string();
    }
  }
  query_str += " FROM ";
  bool notfirst = false;
  for (auto& p : from_clause_) {
    if (notfirst) {
      query_str += ", ";
    } else {
      notfirst = true;
    }
    query_str += p->to_string();
  }
  if (where_clause_) {
    query_str += " WHERE " + where_clause_->to_string();
  }
  if (!groupby_clause_.empty()) {
    query_str += " GROUP BY ";
    bool notfirst = false;
    for (auto& p : groupby_clause_) {
      if (notfirst) {
        query_str += ", ";
      } else {
        notfirst = true;
      }
      query_str += p->to_string();
    }
  }
  if (having_clause_) {
    query_str += " HAVING " + having_clause_->to_string();
  }
  query_str += ";";
  return query_str;
}

}  // namespace Parser
