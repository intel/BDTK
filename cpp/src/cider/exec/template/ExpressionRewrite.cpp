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

#include "exec/template/ExpressionRewrite.h"

#include <algorithm>
#include <boost/locale/conversion.hpp>
#include <unordered_set>

#include "exec/plan/parser/ParserNode.h"
#include "exec/template/DeepCopyVisitor.h"
#include "exec/template/ScalarExprVisitor.h"
#include "function/string/StringOps.h"
#include "type/plan/Analyzer.h"
#include "type/plan/InValues.h"
#include "type/plan/Utils.h"
#include "util/Logger.h"
#include "util/sqldefs.h"

extern bool g_strip_join_covered_quals;
namespace {

class OrToInVisitor : public ScalarExprVisitor<std::shared_ptr<Analyzer::InValues>> {
 protected:
  std::shared_ptr<Analyzer::InValues> visitBinOper(
      const Analyzer::BinOper* bin_oper) const override {
    switch (bin_oper->get_optype()) {
      case kEQ: {
        const auto rhs_owned = bin_oper->get_own_right_operand();
        auto rhs_no_cast = extract_cast_arg(rhs_owned.get());
        if (!dynamic_cast<const Analyzer::Constant*>(rhs_no_cast)) {
          return nullptr;
        }
        const auto arg = bin_oper->get_own_left_operand();
        const auto& arg_ti = arg->get_type_info();
        auto rhs = rhs_no_cast->deep_copy()->add_cast(arg_ti);
        return makeExpr<Analyzer::InValues>(
            arg, std::list<std::shared_ptr<Analyzer::Expr>>{rhs});
      }
      case kOR: {
        return aggregateResult(visit(bin_oper->get_left_operand()),
                               visit(bin_oper->get_right_operand()));
      }
      default:
        break;
    }
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitUOper(
      const Analyzer::UOper* uoper) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitInValues(
      const Analyzer::InValues*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitInIntegerSet(
      const Analyzer::InIntegerSet*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitCharLength(
      const Analyzer::CharLengthExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitKeyForString(
      const Analyzer::KeyForStringExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitSampleRatio(
      const Analyzer::SampleRatioExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitCardinality(
      const Analyzer::CardinalityExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitLikeExpr(
      const Analyzer::LikeExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitRegexpExpr(
      const Analyzer::RegexpExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitCaseExpr(
      const Analyzer::CaseExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitDatetruncExpr(
      const Analyzer::DatetruncExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitDatediffExpr(
      const Analyzer::DatediffExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitDateaddExpr(
      const Analyzer::DateaddExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitExtractExpr(
      const Analyzer::ExtractExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitLikelihood(
      const Analyzer::LikelihoodExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> visitAggExpr(
      const Analyzer::AggExpr*) const override {
    return nullptr;
  }

  std::shared_ptr<Analyzer::InValues> aggregateResult(
      const std::shared_ptr<Analyzer::InValues>& lhs,
      const std::shared_ptr<Analyzer::InValues>& rhs) const override {
    if (!lhs || !rhs) {
      return nullptr;
    }

    if (lhs->get_arg()->get_type_info() == rhs->get_arg()->get_type_info() &&
        (*lhs->get_arg() == *rhs->get_arg())) {
      auto union_values = lhs->get_value_list();
      const auto& rhs_values = rhs->get_value_list();
      union_values.insert(union_values.end(), rhs_values.begin(), rhs_values.end());
      return makeExpr<Analyzer::InValues>(lhs->get_own_arg(), union_values);
    }
    return nullptr;
  }
};

class RecursiveOrToInVisitor : public DeepCopyVisitor {
 protected:
  std::shared_ptr<Analyzer::Expr> visitBinOper(
      const Analyzer::BinOper* bin_oper) const override {
    OrToInVisitor simple_visitor;
    if (bin_oper->get_optype() == kOR) {
      auto rewritten = simple_visitor.visit(bin_oper);
      if (rewritten) {
        return rewritten;
      }
    }
    auto lhs = bin_oper->get_own_left_operand();
    auto rhs = bin_oper->get_own_right_operand();
    auto rewritten_lhs = visit(lhs.get());
    auto rewritten_rhs = visit(rhs.get());
    return makeExpr<Analyzer::BinOper>(bin_oper->get_type_info(),
                                       bin_oper->get_contains_agg(),
                                       bin_oper->get_optype(),
                                       bin_oper->get_qualifier(),
                                       rewritten_lhs ? rewritten_lhs : lhs,
                                       rewritten_rhs ? rewritten_rhs : rhs);
  }
};

class ArrayElementStringLiteralEncodingVisitor : public DeepCopyVisitor {
 protected:
  using RetType = DeepCopyVisitor::RetType;

  RetType visitArrayOper(const Analyzer::ArrayExpr* array_expr) const override {
    std::vector<std::shared_ptr<Analyzer::Expr>> args_copy;
    for (size_t i = 0; i < array_expr->getElementCount(); ++i) {
      auto const element_expr_ptr = visit(array_expr->getElement(i));
      auto const& element_expr_type_info = element_expr_ptr->get_type_info();

      if (!element_expr_type_info.is_string() ||
          element_expr_type_info.get_compression() != kENCODING_NONE) {
        args_copy.push_back(element_expr_ptr);
      } else {
        auto transient_dict_type_info = element_expr_type_info;

        transient_dict_type_info.set_compression(kENCODING_DICT);
        transient_dict_type_info.set_comp_param(TRANSIENT_DICT_ID);
        transient_dict_type_info.set_fixed_size();
        args_copy.push_back(element_expr_ptr->add_cast(transient_dict_type_info));
      }
    }

    const auto& type_info = array_expr->get_type_info();
    return makeExpr<Analyzer::ArrayExpr>(
        type_info, args_copy, array_expr->isNull(), array_expr->isLocalAlloc());
  }
};

class ConstantFoldingVisitor : public DeepCopyVisitor {
  template <typename T>
  bool foldComparison(SQLOps optype, T t1, T t2) const {
    switch (optype) {
      case kEQ:
        return t1 == t2;
      case kNE:
        return t1 != t2;
      case kLT:
        return t1 < t2;
      case kLE:
        return t1 <= t2;
      case kGT:
        return t1 > t2;
      case kGE:
        return t1 >= t2;
      default:
        break;
    }
    CIDER_THROW(CiderCompileException, "Unable to fold");
    return false;
  }

  template <typename T>
  bool foldLogic(SQLOps optype, T t1, T t2) const {
    switch (optype) {
      case kAND:
        return t1 && t2;
      case kOR:
        return t1 || t2;
      case kNOT:
        return !t1;
      default:
        break;
    }
    CIDER_THROW(CiderCompileException, "Unable to fold");
    return false;
  }

  template <typename T>
  T foldArithmetic(SQLOps optype, T t1, T t2) const {
    bool t2_is_zero = (t2 == (t2 - t2));
    bool t2_is_negative = (t2 < (t2 - t2));
    switch (optype) {
      case kPLUS:
        // The MIN limit for float and double is the smallest representable value,
        // not the lowest negative value! Switching to C++11 lowest.
        if ((t2_is_negative && t1 < std::numeric_limits<T>::lowest() - t2) ||
            (!t2_is_negative && t1 > std::numeric_limits<T>::max() - t2)) {
          num_overflows_++;
          CIDER_THROW(CiderCompileException, "Plus overflow");
        }
        return t1 + t2;
      case kMINUS:
        if ((t2_is_negative && t1 > std::numeric_limits<T>::max() + t2) ||
            (!t2_is_negative && t1 < std::numeric_limits<T>::lowest() + t2)) {
          num_overflows_++;
          CIDER_THROW(CiderCompileException, "Minus overflow");
        }
        return t1 - t2;
      case kMULTIPLY: {
        if (t2_is_zero) {
          return t2;
        }
        auto ct1 = t1;
        auto ct2 = t2;
        // Need to keep t2's sign on the left
        if (t2_is_negative) {
          if (t1 == std::numeric_limits<T>::lowest() ||
              t2 == std::numeric_limits<T>::lowest()) {
            // negation could overflow - bail
            num_overflows_++;
            CIDER_THROW(CiderCompileException, "Mul neg overflow");
          }
          ct1 = -t1;  // ct1 gets t2's negativity
          ct2 = -t2;  // ct2 is now positive
        }
        // Don't check overlow if we are folding FP mul by a fraction
        bool ct2_is_fraction = (ct2 < (ct2 / ct2));
        if (!ct2_is_fraction) {
          if (ct1 > std::numeric_limits<T>::max() / ct2 ||
              ct1 < std::numeric_limits<T>::lowest() / ct2) {
            num_overflows_++;
            CIDER_THROW(CiderCompileException, "Mul overflow");
          }
        }
        return t1 * t2;
      }
      case kDIVIDE:
        if (t2_is_zero) {
          CIDER_THROW(CiderCompileException, "Will not fold division by zero");
        }
        return t1 / t2;
      default:
        break;
    }
    CIDER_THROW(CiderCompileException, "Unable to fold");
  }

  bool foldOper(SQLOps optype,
                SQLTypes type,
                Datum lhs,
                Datum rhs,
                Datum& result,
                SQLTypes& result_type) const {
    result_type = type;

    try {
      switch (type) {
        case kBOOLEAN:
          if (IS_COMPARISON(optype)) {
            result.boolval = foldComparison<bool>(optype, lhs.boolval, rhs.boolval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_LOGIC(optype)) {
            result.boolval = foldLogic<bool>(optype, lhs.boolval, rhs.boolval);
            result_type = kBOOLEAN;
            return true;
          }
          CHECK(!IS_ARITHMETIC(optype));
          break;
        case kTINYINT:
          if (IS_COMPARISON(optype)) {
            result.boolval =
                foldComparison<int8_t>(optype, lhs.tinyintval, rhs.tinyintval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.tinyintval =
                foldArithmetic<int8_t>(optype, lhs.tinyintval, rhs.tinyintval);
            result_type = kTINYINT;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        case kSMALLINT:
          if (IS_COMPARISON(optype)) {
            result.boolval =
                foldComparison<int16_t>(optype, lhs.smallintval, rhs.smallintval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.smallintval =
                foldArithmetic<int16_t>(optype, lhs.smallintval, rhs.smallintval);
            result_type = kSMALLINT;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        case kINT:
          if (IS_COMPARISON(optype)) {
            result.boolval = foldComparison<int32_t>(optype, lhs.intval, rhs.intval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.intval = foldArithmetic<int32_t>(optype, lhs.intval, rhs.intval);
            result_type = kINT;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        case kBIGINT:
          if (IS_COMPARISON(optype)) {
            result.boolval =
                foldComparison<int64_t>(optype, lhs.bigintval, rhs.bigintval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.bigintval =
                foldArithmetic<int64_t>(optype, lhs.bigintval, rhs.bigintval);
            result_type = kBIGINT;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        case kFLOAT:
          if (IS_COMPARISON(optype)) {
            result.boolval = foldComparison<float>(optype, lhs.floatval, rhs.floatval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.floatval = foldArithmetic<float>(optype, lhs.floatval, rhs.floatval);
            result_type = kFLOAT;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        case kDOUBLE:
          if (IS_COMPARISON(optype)) {
            result.boolval = foldComparison<double>(optype, lhs.doubleval, rhs.doubleval);
            result_type = kBOOLEAN;
            return true;
          }
          if (IS_ARITHMETIC(optype)) {
            result.doubleval =
                foldArithmetic<double>(optype, lhs.doubleval, rhs.doubleval);
            result_type = kDOUBLE;
            return true;
          }
          CHECK(!IS_LOGIC(optype));
          break;
        default:
          break;
      }
    } catch (...) {
      return false;
    }
    return false;
  }

  std::shared_ptr<Analyzer::Expr> visitUOper(
      const Analyzer::UOper* uoper) const override {
    const auto unvisited_operand = uoper->get_operand();
    const auto optype = uoper->get_optype();
    const auto& ti = uoper->get_type_info();
    if (optype == kCAST) {
      // Cache the cast type so it could be used in operand rewriting/folding
      casts_.insert({unvisited_operand, ti});
    }
    const auto operand = visit(unvisited_operand);

    const auto& operand_ti = operand->get_type_info();
    const auto operand_type =
        operand_ti.is_decimal() ? decimal_to_int_type(operand_ti) : operand_ti.get_type();
    const auto const_operand =
        std::dynamic_pointer_cast<const Analyzer::Constant>(operand);

    if (const_operand) {
      const auto operand_datum = const_operand->get_constval();
      Datum zero_datum = {};
      Datum result_datum = {};
      SQLTypes result_type;
      switch (optype) {
        case kNOT: {
          if (foldOper(kEQ,
                       operand_type,
                       zero_datum,
                       operand_datum,
                       result_datum,
                       result_type)) {
            CHECK_EQ(result_type, kBOOLEAN);
            return makeExpr<Analyzer::Constant>(result_type, false, result_datum);
          }
          break;
        }
        case kUMINUS: {
          if (foldOper(kMINUS,
                       operand_type,
                       zero_datum,
                       operand_datum,
                       result_datum,
                       result_type)) {
            if (!operand_ti.is_decimal()) {
              return makeExpr<Analyzer::Constant>(result_type, false, result_datum);
            }
            return makeExpr<Analyzer::Constant>(ti, false, result_datum);
          }
          break;
        }
        case kCAST: {
          // Trying to fold number to number casts only
          if (!ti.is_number() || !operand_ti.is_number()) {
            break;
          }
          // Disallowing folding of FP to DECIMAL casts for now:
          // allowing them would make this test pass:
          //    update dectest set d=cast( 1234.0 as float );
          // which is expected to throw in Update.ImplicitCastToNumericTypes
          // due to cast codegen currently not supporting these casts
          if (ti.is_decimal() && operand_ti.is_fp()) {
            break;
          }
          auto operand_copy = const_operand->deep_copy();
          auto cast_operand = operand_copy->add_cast(ti);
          auto const_cast_operand =
              std::dynamic_pointer_cast<const Analyzer::Constant>(cast_operand);
          if (const_cast_operand) {
            auto const_cast_datum = const_cast_operand->get_constval();
            return makeExpr<Analyzer::Constant>(ti, false, const_cast_datum);
          }
        }
        default:
          break;
      }
    }

    return makeExpr<Analyzer::UOper>(
        uoper->get_type_info(), uoper->get_contains_agg(), optype, operand);
  }

  std::shared_ptr<Analyzer::Expr> visitBinOper(
      const Analyzer::BinOper* bin_oper) const override {
    const auto optype = bin_oper->get_optype();
    auto ti = bin_oper->get_type_info();
    auto left_operand = bin_oper->get_own_left_operand();
    auto right_operand = bin_oper->get_own_right_operand();

    // Check if bin_oper result is cast to a larger int or fp type
    if (casts_.find(bin_oper) != casts_.end()) {
      const auto cast_ti = casts_[bin_oper];
      const auto& lhs_ti = bin_oper->get_left_operand()->get_type_info();
      // Propagate cast down to the operands for folding
      if ((cast_ti.is_integer() || cast_ti.is_fp()) && lhs_ti.is_integer() &&
          cast_ti.get_size() > lhs_ti.get_size() &&
          (optype == kMINUS || optype == kPLUS || optype == kMULTIPLY)) {
        // Before folding, cast the operands to the bigger type to avoid overflows.
        // Currently upcasting smaller integer types to larger integers or double.
        left_operand = left_operand->deep_copy()->add_cast(cast_ti);
        right_operand = right_operand->deep_copy()->add_cast(cast_ti);
        ti = cast_ti;
      }
    }

    const auto lhs = visit(left_operand.get());
    const auto rhs = visit(right_operand.get());

    auto const_lhs = std::dynamic_pointer_cast<Analyzer::Constant>(lhs);
    auto const_rhs = std::dynamic_pointer_cast<Analyzer::Constant>(rhs);
    const auto& lhs_ti = lhs->get_type_info();
    const auto& rhs_ti = rhs->get_type_info();
    auto lhs_type = lhs_ti.is_decimal() ? decimal_to_int_type(lhs_ti) : lhs_ti.get_type();
    auto rhs_type = rhs_ti.is_decimal() ? decimal_to_int_type(rhs_ti) : rhs_ti.get_type();

    if (const_lhs && const_rhs && lhs_type == rhs_type) {
      auto lhs_datum = const_lhs->get_constval();
      auto rhs_datum = const_rhs->get_constval();
      Datum result_datum = {};
      SQLTypes result_type;
      if (foldOper(optype, lhs_type, lhs_datum, rhs_datum, result_datum, result_type)) {
        // Fold all ops that don't take in decimal operands, and also decimal comparisons
        if (!lhs_ti.is_decimal() || IS_COMPARISON(optype)) {
          return makeExpr<Analyzer::Constant>(result_type, false, result_datum);
        }
        // Decimal arithmetic has been done as kBIGINT. Selectively fold some decimal ops,
        // using result_datum and BinOper expr typeinfo which was adjusted for these ops.
        if (optype == kMINUS || optype == kPLUS || optype == kMULTIPLY) {
          return makeExpr<Analyzer::Constant>(ti, false, result_datum);
        }
      }
    }

    if (optype == kAND && lhs_type == rhs_type && lhs_type == kBOOLEAN) {
      if (const_rhs && !const_rhs->get_is_null()) {
        auto rhs_datum = const_rhs->get_constval();
        if (rhs_datum.boolval == false) {
          Datum d;
          d.boolval = false;
          // lhs && false --> false
          return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
        }
        // lhs && true --> lhs
        return lhs;
      }
      if (const_lhs && !const_lhs->get_is_null()) {
        auto lhs_datum = const_lhs->get_constval();
        if (lhs_datum.boolval == false) {
          Datum d;
          d.boolval = false;
          // false && rhs --> false
          return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
        }
        // true && rhs --> rhs
        return rhs;
      }
    }
    if (optype == kOR && lhs_type == rhs_type && lhs_type == kBOOLEAN) {
      if (const_rhs && !const_rhs->get_is_null()) {
        auto rhs_datum = const_rhs->get_constval();
        if (rhs_datum.boolval == true) {
          Datum d;
          d.boolval = true;
          // lhs || true --> true
          return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
        }
        // lhs || false --> lhs
        return lhs;
      }
      if (const_lhs && !const_lhs->get_is_null()) {
        auto lhs_datum = const_lhs->get_constval();
        if (lhs_datum.boolval == true) {
          Datum d;
          d.boolval = true;
          // true || rhs --> true
          return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
        }
        // false || rhs --> rhs
        return rhs;
      }
    }
    if (*lhs == *rhs) {
      // Tautologies: v=v; v<=v; v>=v
      if (optype == kEQ || optype == kLE || optype == kGE) {
        Datum d;
        d.boolval = true;
        return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
      }
      // Contradictions: v!=v; v<v; v>v
      if (optype == kNE || optype == kLT || optype == kGT) {
        Datum d;
        d.boolval = false;
        return makeExpr<Analyzer::Constant>(kBOOLEAN, false, d);
      }
      // v-v
      if (optype == kMINUS) {
        Datum d = {};
        return makeExpr<Analyzer::Constant>(lhs_type, false, d);
      }
    }
    // Convert fp division by a constant to multiplication by 1/constant
    if (optype == kDIVIDE && const_rhs && rhs_ti.is_fp()) {
      auto rhs_datum = const_rhs->get_constval();
      std::shared_ptr<Analyzer::Expr> recip_rhs = nullptr;
      if (rhs_ti.get_type() == kFLOAT) {
        if (rhs_datum.floatval == 1.0) {
          return lhs;
        }
        auto f = std::fabs(rhs_datum.floatval);
        if (f > 1.0 || (f != 0.0 && 1.0 < f * std::numeric_limits<float>::max())) {
          rhs_datum.floatval = 1.0 / rhs_datum.floatval;
          recip_rhs = makeExpr<Analyzer::Constant>(rhs_type, false, rhs_datum);
        }
      } else if (rhs_ti.get_type() == kDOUBLE) {
        if (rhs_datum.doubleval == 1.0) {
          return lhs;
        }
        auto d = std::fabs(rhs_datum.doubleval);
        if (d > 1.0 || (d != 0.0 && 1.0 < d * std::numeric_limits<double>::max())) {
          rhs_datum.doubleval = 1.0 / rhs_datum.doubleval;
          recip_rhs = makeExpr<Analyzer::Constant>(rhs_type, false, rhs_datum);
        }
      }
      if (recip_rhs) {
        return makeExpr<Analyzer::BinOper>(ti,
                                           bin_oper->get_contains_agg(),
                                           kMULTIPLY,
                                           bin_oper->get_qualifier(),
                                           lhs,
                                           recip_rhs);
      }
    }

    return makeExpr<Analyzer::BinOper>(ti,
                                       bin_oper->get_contains_agg(),
                                       bin_oper->get_optype(),
                                       bin_oper->get_qualifier(),
                                       lhs,
                                       rhs);
  }

 protected:
  mutable bool in_string_op_chain_{false};
  mutable std::vector<std::shared_ptr<Analyzer::Expr>> chained_string_op_exprs_;
  mutable std::unordered_map<const Analyzer::Expr*, const SQLTypeInfo> casts_;
  mutable int32_t num_overflows_;

  std::shared_ptr<Analyzer::Expr> makeStringOperExpr(
      SqlStringOpKind kind,
      const SQLTypeInfo& return_type,
      const std::vector<std::shared_ptr<Analyzer::Expr>>& args) const {
    switch (kind) {
      case SqlStringOpKind::SUBSTRING:
        return makeExpr<Analyzer::SubstringStringOper>(args);
      case SqlStringOpKind::UPPER:
        return makeExpr<Analyzer::UpperStringOper>(args);
      case SqlStringOpKind::LOWER:
        return makeExpr<Analyzer::LowerStringOper>(args);
      case SqlStringOpKind::CHAR_LENGTH:
        return makeExpr<Analyzer::CharLengthStringOper>(args);
      case SqlStringOpKind::CONCAT:
      case SqlStringOpKind::RCONCAT:
        return makeExpr<Analyzer::ConcatStringOper>(args);
      case SqlStringOpKind::TRIM:
      case SqlStringOpKind::LTRIM:
      case SqlStringOpKind::RTRIM:
        return makeExpr<Analyzer::TrimStringOper>(kind, args);
      case SqlStringOpKind::STRING_SPLIT:
      case SqlStringOpKind::SPLIT_PART:
        return makeExpr<Analyzer::SplitPartStringOper>(args);
      case SqlStringOpKind::REGEXP_REPLACE:
        return makeExpr<Analyzer::RegexpReplaceStringOper>(args);
      case SqlStringOpKind::REGEXP_SUBSTR:
        return makeExpr<Analyzer::RegexpSubstrStringOper>(args);
      case SqlStringOpKind::REGEXP_EXTRACT:
        return makeExpr<Analyzer::RegexpExtractStringOper>(args);
      case SqlStringOpKind::TRY_STRING_CAST:
        return makeExpr<Analyzer::TryStringCastOper>(return_type, args);
      default:
        CIDER_THROW(CiderUnsupportedException,
                    fmt::format("StringOp {} is not supported in constant folding",
                                ::toString(kind)));
    }
  }

 public:
  ConstantFoldingVisitor() : num_overflows_(0) {}
  int32_t get_num_overflows() { return num_overflows_; }
  void reset_num_overflows() { num_overflows_ = 0; }
};

const Analyzer::Expr* strip_likelihood(const Analyzer::Expr* expr) {
  const auto with_likelihood = dynamic_cast<const Analyzer::LikelihoodExpr*>(expr);
  if (!with_likelihood) {
    return expr;
  }
  return with_likelihood->get_arg();
}

}  // namespace

Analyzer::ExpressionPtr rewrite_array_elements(Analyzer::Expr const* expr) {
  return ArrayElementStringLiteralEncodingVisitor().visit(expr);
}

Analyzer::ExpressionPtr rewrite_expr(const Analyzer::Expr* expr) {
  // skip InValues expression rewrite
  auto in_values_expr = dynamic_cast<const Analyzer::InValues*>(expr);
  if (in_values_expr &&
      dynamic_cast<const Analyzer::SubstringStringOper*>(in_values_expr->get_arg())) {
    return in_values_expr->deep_copy();
  }
  const auto expr_no_likelihood = strip_likelihood(expr);
  // The following check is not strictly needed, but seems silly to transform a
  // simple string comparison to an IN just to codegen the same thing anyway.

  RecursiveOrToInVisitor visitor;
  auto rewritten_expr = visitor.visit(expr_no_likelihood);
  const auto expr_with_likelihood =
      std::dynamic_pointer_cast<const Analyzer::LikelihoodExpr>(rewritten_expr);
  if (expr_with_likelihood) {
    // Add back likelihood
    return std::make_shared<Analyzer::LikelihoodExpr>(
        rewritten_expr, expr_with_likelihood->get_likelihood());
  }
  return rewritten_expr;
}

namespace {

static const std::unordered_set<std::string> overlaps_supported_functions = {
    "ST_Contains_MultiPolygon_Point",
    "ST_Contains_Polygon_Point",
    "ST_cContains_MultiPolygon_Point",  // compressed coords version
    "ST_cContains_Polygon_Point",
    "ST_Contains_Polygon_Polygon",
    "ST_Contains_Polygon_MultiPolygon",
    "ST_Contains_MultiPolygon_MultiPolygon",
    "ST_Contains_MultiPolygon_Polygon",
    "ST_Intersects_Polygon_Point",
    "ST_Intersects_Polygon_Polygon",
    "ST_Intersects_Polygon_MultiPolygon",
    "ST_Intersects_MultiPolygon_MultiPolygon",
    "ST_Intersects_MultiPolygon_Polygon",
    "ST_Intersects_MultiPolygon_Point",
    "ST_Approx_Overlaps_MultiPolygon_Point",
    "ST_Overlaps"};

static const std::unordered_set<std::string> requires_many_to_many = {
    "ST_Contains_Polygon_Polygon",
    "ST_Contains_Polygon_MultiPolygon",
    "ST_Contains_MultiPolygon_MultiPolygon",
    "ST_Contains_MultiPolygon_Polygon",
    "ST_Intersects_Polygon_Polygon",
    "ST_Intersects_Polygon_MultiPolygon",
    "ST_Intersects_MultiPolygon_MultiPolygon",
    "ST_Intersects_MultiPolygon_Polygon"};

}  // namespace

/**
 * JoinCoveredQualVisitor returns true if the visited qual is true if and only if a
 * corresponding equijoin qual is true. During the pre-filtered count we can elide the
 * visited qual decreasing query run time while upper bounding the number of rows passing
 * the filter. Currently only used for expressions of the form `a OVERLAPS b AND Expr<a,
 * b>`. Strips `Expr<a,b>` if the expression has been pre-determined to be expensive to
 * compute twice.
 */
class JoinCoveredQualVisitor : public ScalarExprVisitor<bool> {
 public:
  JoinCoveredQualVisitor(const JoinQualsPerNestingLevel& join_quals) {
    for (const auto& join_condition : join_quals) {
      for (const auto& qual : join_condition.quals) {
        auto qual_bin_oper = dynamic_cast<Analyzer::BinOper*>(qual.get());
        if (qual_bin_oper) {
          join_qual_pairs.emplace_back(qual_bin_oper->get_left_operand(),
                                       qual_bin_oper->get_right_operand());
        }
      }
    }
  }

  bool visitFunctionOper(const Analyzer::FunctionOper* func_oper) const override {
    if (overlaps_supported_functions.find(func_oper->getName()) !=
        overlaps_supported_functions.end()) {
      const auto lhs = func_oper->getArg(2);
      const auto rhs = func_oper->getArg(1);
      for (const auto& qual_pair : join_qual_pairs) {
        if (*lhs == *qual_pair.first && *rhs == *qual_pair.second) {
          return true;
        }
      }
    }
    return false;
  }

  bool defaultResult() const override { return false; }

 private:
  std::vector<std::pair<const Analyzer::Expr*, const Analyzer::Expr*>> join_qual_pairs;
};

std::shared_ptr<Analyzer::Expr> fold_expr(const Analyzer::Expr* expr) {
  if (!expr) {
    return nullptr;
  }
  // Skip expr fold for InValues and BinOper with substr as left operand
  auto in_values_expr = dynamic_cast<const Analyzer::InValues*>(expr);
  if (in_values_expr &&
      dynamic_cast<const Analyzer::SubstringStringOper*>(in_values_expr->get_arg())) {
    return in_values_expr->deep_copy();
  }

  auto bin_substr = dynamic_cast<const Analyzer::BinOper*>(expr);
  if (bin_substr && dynamic_cast<const Analyzer::SubstringStringOper*>(
                        bin_substr->get_left_operand())) {
    return bin_substr->deep_copy();
  }
  const auto expr_no_likelihood = strip_likelihood(expr);
  ConstantFoldingVisitor visitor;
  auto rewritten_expr = visitor.visit(expr_no_likelihood);
  if (visitor.get_num_overflows() > 0 && rewritten_expr->get_type_info().is_integer() &&
      rewritten_expr->get_type_info().get_type() != kBIGINT) {
    auto rewritten_expr_const =
        std::dynamic_pointer_cast<const Analyzer::Constant>(rewritten_expr);
    if (!rewritten_expr_const) {
      // Integer expression didn't fold completely the first time due to
      // overflows in smaller type subexpressions, trying again with a cast
      const auto& ti = SQLTypeInfo(kBIGINT, false);
      auto bigint_expr_no_likelihood = expr_no_likelihood->deep_copy()->add_cast(ti);
      auto rewritten_expr_take2 = visitor.visit(bigint_expr_no_likelihood.get());
      auto rewritten_expr_take2_const =
          std::dynamic_pointer_cast<Analyzer::Constant>(rewritten_expr_take2);
      if (rewritten_expr_take2_const) {
        // Managed to fold, switch to the new constant
        rewritten_expr = rewritten_expr_take2_const;
      }
    }
  }
  const auto expr_with_likelihood = dynamic_cast<const Analyzer::LikelihoodExpr*>(expr);
  if (expr_with_likelihood) {
    // Add back likelihood
    return std::make_shared<Analyzer::LikelihoodExpr>(
        rewritten_expr, expr_with_likelihood->get_likelihood());
  }
  return rewritten_expr;
}

bool self_join_not_covered_by_left_deep_tree(const Analyzer::ColumnVar* key_side,
                                             const Analyzer::ColumnVar* val_side,
                                             const int max_rte_covered) {
  if (key_side->get_table_id() == val_side->get_table_id() &&
      key_side->get_rte_idx() == val_side->get_rte_idx() &&
      key_side->get_rte_idx() > max_rte_covered) {
    return true;
  }
  return false;
}

const int get_max_rte_scan_table(
    std::unordered_map<int, llvm::Value*>& scan_idx_to_hash_pos) {
  int ret = INT32_MIN;
  for (auto& kv : scan_idx_to_hash_pos) {
    if (kv.first > ret) {
      ret = kv.first;
    }
  }
  return ret;
}
