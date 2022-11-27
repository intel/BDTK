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
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/template/Execute.h"  // for is_unnest
#include "type/plan/BinaryExpr.h"

namespace Analyzer {
using namespace cider::jitlib;

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

  FixSizeJITExprValue lhs_val(lhs->codegen(func));
  FixSizeJITExprValue rhs_val(rhs->codegen(func));

  JITValuePointer null(nullptr);
  bool lhs_nullable = lhs_val.isNullable();
  bool rhs_nullable = rhs_val.isNullable();
  if (lhs_nullable && rhs_nullable) {
    null.replace(lhs_val.getNull() || rhs_val.getNull());
  } else if (lhs_nullable || rhs_nullable) {
    null.replace(lhs_nullable ? lhs_val.getNull() : rhs_val.getNull());
  }

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
        UNIMPLEMENTED();
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

  return expr_var_;
}

}  // namespace Analyzer
