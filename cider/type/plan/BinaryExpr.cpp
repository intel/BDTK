/*
 * Copyright (c) 2022 Intel Corporation.
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
#include "type/plan/BinaryExpr.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/plan/parser/ParserNode.h"
#include "exec/template/Execute.h"  // for is_unnest

namespace Analyzer {
using namespace cider::jitlib;

std::shared_ptr<Analyzer::BinOper> lower_bw_eq(const Analyzer::BinOper* bw_eq) {
  const auto eq_oper =
      std::make_shared<Analyzer::BinOper>(bw_eq->get_type_info(),
                                          bw_eq->get_contains_agg(),
                                          kEQ,
                                          bw_eq->get_qualifier(),
                                          bw_eq->get_own_left_operand(),
                                          bw_eq->get_own_right_operand());
  const auto lhs_is_null = std::make_shared<Analyzer::UOper>(
      SQLTypes::kBOOLEAN, kISNULL, bw_eq->get_own_left_operand());
  const auto rhs_is_null = std::make_shared<Analyzer::UOper>(
      SQLTypes::kBOOLEAN, kISNULL, bw_eq->get_own_right_operand());
  const auto both_are_null =
      Parser::OperExpr::normalize(kAND, kONE, lhs_is_null, rhs_is_null);
  const auto bw_eq_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
      Parser::OperExpr::normalize(kOR, kONE, eq_oper, both_are_null));
  CHECK(bw_eq_oper);
  return bw_eq_oper;
}

std::shared_ptr<Analyzer::BinOper> lower_bw_ne(const Analyzer::BinOper* bw_ne) {
  const auto ne_oper =
      std::make_shared<Analyzer::BinOper>(bw_ne->get_type_info(),
                                          bw_ne->get_contains_agg(),
                                          kNE,
                                          bw_ne->get_qualifier(),
                                          bw_ne->get_own_left_operand(),
                                          bw_ne->get_own_right_operand());
  const auto lhs_is_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kISNULL, bw_ne->get_own_left_operand());
  const auto rhs_is_null = std::make_shared<Analyzer::UOper>(
      kBOOLEAN, kISNULL, bw_ne->get_own_right_operand());
  const auto lhs_is_not_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, lhs_is_null);
  const auto rhs_is_not_null =
      std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, rhs_is_null);
  const auto only_one_null =
      std::dynamic_pointer_cast<Analyzer::BinOper>(Parser::OperExpr::normalize(
          kOR,
          kONE,
          Parser::OperExpr::normalize(kAND, kONE, lhs_is_null, rhs_is_not_null),
          Parser::OperExpr::normalize(kAND, kONE, rhs_is_null, lhs_is_not_null)));

  const auto bw_ne_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
      Parser::OperExpr::normalize(kOR, kONE, ne_oper, only_one_null));
  return bw_ne_oper;
}

JITExprValue& BinOper::codegen(JITFunction& func) {
  if (auto& expr_var = get_expr_value()) {
    return expr_var;
  }

  auto lhs = const_cast<Analyzer::Expr*>(get_left_operand());
  auto rhs = const_cast<Analyzer::Expr*>(get_right_operand());

  if (is_unnest(lhs) || is_unnest(rhs)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in comparisons");
  }

  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();
  CHECK_EQ(lhs_ti.get_type(), rhs_ti.get_type());
  if (lhs_ti.is_decimal() || lhs_ti.is_timeinterval()) {
    CIDER_THROW(CiderCompileException,
                "Decimal and TimeInterval are not supported in arithmetic codegen now.");
  }
  if (get_optype() == kBW_EQ or get_optype() == kBW_NE) {
    return get_optype() == kBW_EQ ? lower_bw_eq(this)->codegen(func)
                                  : lower_bw_ne(this)->codegen(func);
  }
  FixSizeJITExprValue lhs_val(lhs->codegen(func));
  FixSizeJITExprValue rhs_val(rhs->codegen(func));

  auto null = lhs_val.getNull() || rhs_val.getNull();

  switch (lhs_ti.get_type()) {
    case kVARCHAR:
    case kTEXT:
    case kCHAR:
      // return codegenVarcharCmpFun(bin_oper, lhs_lv.get(), rhs_lv.get(), null);
      UNIMPLEMENTED();
    default:
      const auto optype = get_optype();
      if (IS_ARITHMETIC(optype)) {
        return codegenFixedSizeColArithFun(null, lhs_val.getValue(), rhs_val.getValue());
      } else if (IS_COMPARISON(optype)) {
        // return codegenCmpFun(bin_oper);
        return codegenFixedSizeColCmpFun(null, lhs_val.getValue(), rhs_val.getValue());
      } else if (IS_LOGIC(optype)) {
        // return codegenLogicalFun(bin_oper, co);
        return set_expr_value(null, codegenFixedSizeLogicalFun(lhs_val.getValue(), rhs_val.getValue()));
      }
  }
  UNREACHABLE();
  return expr_var_;
}

JITExprValue& BinOper::codegenFixedSizeColArithFun(JITValuePointer& null,
                                                   JITValue& lhs,
                                                   JITValue& rhs) {
  // TODO: Null Process
  switch (get_optype()) {
    case kMINUS:
      return set_expr_value(null, lhs - rhs);
    case kPLUS:
      return set_expr_value(null, lhs + rhs);
    case kMULTIPLY:
      return set_expr_value(null, lhs * rhs);
    case kDIVIDE:
      return set_expr_value(null, lhs / rhs);
    case kMODULO:
      return set_expr_value(null, lhs % rhs);
    default:
      UNREACHABLE();
  }

  return expr_var_;
}

JITExprValue& BinOper::codegenFixedSizeColCmpFun(JITValuePointer& null,
                                                 JITValue& lhs,
                                                 JITValue& rhs) {
  // TODO: Null Process
  switch (get_optype()) {
    case kEQ:
      return set_expr_value(null, lhs == rhs);
    case kNE:
      return set_expr_value(null, lhs != rhs);
    case kLT:
      return set_expr_value(null, lhs < rhs);
    case kGT:
      return set_expr_value(null, lhs > rhs);
    case kLE:
      return set_expr_value(null, lhs <= rhs);
    case kGE:
      return set_expr_value(null, lhs >= rhs);
    default:
      UNREACHABLE();
  }
}

JITValuePointer BinOper::codegenFixedSizeLogicalFun(JITValue& lhs, JITValue& rhs) {
  // JITValuePointer null(nullptr);
  // auto lhs_null = lhs_val.getNull();
  // auto rhs_null = rhs_val.getNull();
  // if (lhs_null.get() && rhs_null.get()) {
  //   null = lhs_null || rhs_null;
  // } else if (lhs_null.get() || rhs_null.get()) {
  //   null = lhs_null.get() ? lhs_null.get() : rhs_null.get();
  // }
  // JITValuePointer null(nullptr);
  // auto lhs_null = lhs_val.getNull().get();
  // auto rhs_null = rhs_val.getNull().get();
  // if (lhs_null && rhs_null) {
  //   null = lhs_val.getNull() || rhs_val.getNull();
  // } else if (lhs_null || rhs_null) {
  //   null = lhs_null ? lhs_null : rhs_null;
  // }
  switch (get_optype()) {
    case kAND:
      return lhs && rhs;
    case kOR:
      return lhs || rhs;
    default:
      UNREACHABLE();
  }
}

}  // namespace Analyzer
