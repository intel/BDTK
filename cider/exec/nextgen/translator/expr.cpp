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

#include <llvm/IR/Value.h>
#include <utility>

#include "exec/nextgen/jitlib/base/JITTuple.h"
#include "exec/nextgen/jitlib/base/JITValueOperations.h"
#include "exec/nextgen/translator/expr.h"
#include "exec/nextgen/translator/utils.h"
#include "exec/template/Execute.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::translator {
// Cider Data Format
// Generates IR value(s) for the given analyzer expression.
JITExprValue& ExprGenerator::codegen(Analyzer::Expr* expr) {
  CHECK(expr);
  if (auto expr_var = expr->get_expr_value()) {
    return *expr_var;
  }

  auto bin_oper = dynamic_cast<Analyzer::BinOper*>(expr);
  if (bin_oper) {
    return codegenBinOper(bin_oper);
  }
  // auto u_oper = dynamic_cast<const Analyzer::UOper*>(expr);
  // if (u_oper) {
  //   return codegenUOper(u_oper, co);
  // }
  auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr);
  if (col_var) {
    return codegenColumnExpr(col_var);
  }
  // auto constant = dynamic_cast<const Analyzer::Constant*>(expr);
  // if (constant) {
  //   return codegenConstantExpr(constant, co);
  // }
  // auto dateadd_expr = dynamic_cast<const Analyzer::DateaddExpr*>(expr);
  // if (dateadd_expr) {
  //   return codegenDateAdd(dateadd_expr, co);
  // }
  // auto datediff_expr = dynamic_cast<const Analyzer::DatediffExpr*>(expr);
  // if (datediff_expr) {
  //   return codegenDateDiff(datediff_expr, co);
  // }
  // auto datetrunc_expr = dynamic_cast<const Analyzer::DatetruncExpr*>(expr);
  // if (datetrunc_expr) {
  //   return codegenDateTrunc(datetrunc_expr, co);
  // }
  // auto case_expr = dynamic_cast<const Analyzer::CaseExpr*>(expr);
  // if (case_expr) {
  //   return codegenCaseExpr(case_expr, co);
  // }

  // auto in_values = dynamic_cast<const Analyzer::InValues*>(expr);
  // if (in_values) {
  //   return codegenInValues(in_values, co);
  // }

  CIDER_THROW(CiderCompileException, "Cider data format codegen is not avaliable.");
}

JITExprValue& ExprGenerator::codegenBinOper(Analyzer::BinOper* bin_oper) {
  const auto optype = bin_oper->get_optype();
  // if (IS_ARITHMETIC(optype)) {
  //   return codegenArithFun(bin_oper, co);
  // }
  if (IS_COMPARISON(optype)) {
    return codegenCmpFun(bin_oper);
  }
  // if (IS_LOGIC(optype)) {
  //   return codegenLogicalFun(bin_oper, co);
  // }

  UNREACHABLE();
  return fake_val_;
}

JITExprValue& ExprGenerator::codegenColumnExpr(Analyzer::ColumnVar* col_var) {
  return *col_var->get_expr_value();
}

JITExprValue& ExprGenerator::codegenCmpFun(Analyzer::BinOper* bin_oper) {
  if (auto expr_var = bin_oper->get_expr_value()) {
    return *expr_var;
  }

  auto lhs = const_cast<Analyzer::Expr*>(bin_oper->get_left_operand());
  auto rhs = const_cast<Analyzer::Expr*>(bin_oper->get_right_operand());

  if (is_unnest(lhs) || is_unnest(rhs)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in comparisons");
  }

  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();
  CHECK_EQ(lhs_ti.get_type(), rhs_ti.get_type());

  auto& lhs_val = codegen(lhs);
  auto& rhs_val = codegen(rhs);

  auto null = func_->createVariable(getJITTag(lhs), "null");
  TODO("MaJian", "merge null");
  // auto lhs_nullable = dynamic_cast<NullableColValues*>(lhs_lv.get());
  // auto rhs_nullable = dynamic_cast<NullableColValues*>(rhs_lv.get());
  // if (lhs_nullable && rhs_nullable) {
  //   if (lhs_nullable->getNull() && rhs_nullable->getNull()) {
  //     // null = cgen_state_->ir_builder_.CreateOr(lhs_nullable->getNull(),
  //     //                                          rhs_nullable->getNull());
  //     null = lhs->getNull() || rhs_nullable->getNull();
  //   } else {
  //     null = lhs_nullable->getNull() ? lhs_nullable->getNull() :
  //     rhs_nullable->getNull();
  //   }
  // } else if (lhs_nullable || rhs_nullable) {
  //   null = lhs_nullable ? lhs_nullable->getNull() : rhs_nullable->getNull();
  // }

  switch (lhs_ti.get_type()) {
    case kVARCHAR:
    case kTEXT:
    case kCHAR:
      // return codegenVarcharCmpFun(bin_oper, lhs_lv.get(), rhs_lv.get(), null);
      UNIMPLEMENTED();
    default:
      return codegenFixedSizeColCmpFun(
          bin_oper, lhs_val.get_value(), rhs_val.get_value(), null);
  }
  UNREACHABLE();
  return fake_val_;
}

JITExprValue& ExprGenerator::codegenFixedSizeColCmpFun(Analyzer::BinOper* bin_oper,
                                                       JITValue& lhs,
                                                       JITValue& rhs,
                                                       JITValue& null) {
  TODO("MaJian", "gen icmp operation");
  switch (bin_oper->get_optype()) {
    case kEQ:
      return bin_oper->set_expr_value(lhs == rhs);
    case kNE:
      return bin_oper->set_expr_value(lhs != rhs);
    case kLT:
      return bin_oper->set_expr_value(lhs < rhs);
    case kGT:
      return bin_oper->set_expr_value(lhs > rhs);
    case kLE:
      return bin_oper->set_expr_value(lhs <= rhs);
    case kGE:
      return bin_oper->set_expr_value(lhs >= rhs);
    default:
      UNREACHABLE();
  }

  return fake_val_;
}

}  // namespace cider::exec::nextgen::translator
