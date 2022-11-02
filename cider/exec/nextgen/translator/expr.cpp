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

#include <llvm/IR/Value.h>

#include "Execute.h"
#include "expr.h"
#include "util/Logger.h"

// Cider Data Format
// Generates IR value(s) for the given analyzer expression.
JITTuple ExprGenerator::codegen(const Analyzer::Expr* expr, const JITTuple& input) {
  if (!expr) {
    return JITTuple{};
  }
  auto bin_oper = dynamic_cast<const Analyzer::BinOper*>(expr);
  if (bin_oper) {
    return codegenBinOper(bin_oper, input);
  }
  // auto u_oper = dynamic_cast<const Analyzer::UOper*>(expr);
  // if (u_oper) {
  //   return codegenUOper(u_oper, co);
  // }
  auto col_var = dynamic_cast<const Analyzer::ColumnVar*>(expr);
  if (col_var) {
    return codegenColumnExpr(col_var, input);
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

JITTuple ExprGenerator::codegenBinOper(const Analyzer::BinOper* bin_oper,
                                       const JITTuple& input) {
  const auto optype = bin_oper->get_optype();
  // if (IS_ARITHMETIC(optype)) {
  //   return codegenArithFun(bin_oper, co);
  // }
  if (IS_COMPARISON(optype)) {
    return codegenCmpFun(bin_oper, input);
  }
  // if (IS_LOGIC(optype)) {
  //   return codegenLogicalFun(bin_oper, co);
  // }

  UNREACHABLE();
}

JITTuple ExprGenerator::codegenColumnExpr(const Analyzer::ColumnVar* col_var,
                                          const JITTuple& input) {
  TODO("MaJian", "how to find column from input?");
}

JITTuple ExprGenerator::codegenCmpFun(const Analyzer::BinOper* bin_oper,
                                      const JITTuple& input) {
  const auto lhs = bin_oper->get_left_operand();
  const auto rhs = bin_oper->get_right_operand();

  if (is_unnest(lhs) || is_unnest(rhs)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in comparisons");
  }

  const auto& lhs_ti = lhs->get_type_info();
  const auto& rhs_ti = rhs->get_type_info();
  CHECK_EQ(lhs_ti.get_type(), rhs_ti.get_type());

  auto lhs_tuple = codegen(lhs, input);
  auto rhs_tuple = codegen(rhs, input);

  llvm::Value* null = nullptr;
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
          bin_oper, lhs_tuple.getValue(), rhs_tuple.getValue(), null);
  }
}

JITTuple ExprGenerator::codegenFixedSizeColCmpFun(const Analyzer::BinOper* bin_oper,
                                                  llvm::Value* lhs,
                                                  llvm::Value* rhs,
                                                  llvm::Value* null) {
  CHECK(lhs && rhs);

  llvm::Value* value;
  TODO("MaJian", "gen icmp operation");
  // llvm::Value* value =
  //     lhs->getType()->isIntegerTy()
  //         ? ir_builder_.CreateICmp(llvm_icmp_pred(bin_oper->get_optype()), lhs, rhs)
  //         : ir_builder_.CreateFCmp(llvm_fcmp_pred(bin_oper->get_optype()), lhs, rhs);

  return JITTuple(value, null);
}