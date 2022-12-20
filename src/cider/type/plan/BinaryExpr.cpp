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
  if (lhs_ti.is_string()) {
    CHECK(rhs_ti.is_string());
  } else {
    CHECK_EQ(lhs_ti.get_type(), rhs_ti.get_type());
  }
  if (lhs_ti.is_decimal() || lhs_ti.is_timeinterval()) {
    CIDER_THROW(CiderCompileException,
                "Decimal and TimeInterval are not supported in arithmetic codegen now.");
  }
  if (lhs_ti.is_string()) {
    // string binops, should only be comparisons
    const auto optype = get_optype();
    if (IS_COMPARISON(optype)) {
      VarSizeJITExprValue lhs_val(lhs->codegen(func));
      VarSizeJITExprValue rhs_val(rhs->codegen(func));
      JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "null_val");
      null = lhs_val.getNull() || rhs_val.getNull();
      return codegenVarcharCmpFun(func, null, lhs_val, rhs_val);
    } else {
      CIDER_THROW(CiderUnsupportedException, "string BinOp only supports comparison");
    }
  } else {
    // primitive type binops
    FixSizeJITExprValue lhs_val(lhs->codegen(func));
    FixSizeJITExprValue rhs_val(rhs->codegen(func));

    if (get_optype() == kBW_EQ or get_optype() == kBW_NE) {
      return codegenFixedSizeDistinctFrom(func, lhs_val, rhs_val);
    }
    JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "null_val");
    null = lhs_val.getNull() || rhs_val.getNull();

    const auto optype = get_optype();
    if (IS_ARITHMETIC(optype)) {
      return codegenFixedSizeColArithFun(null, lhs_val.getValue(), rhs_val.getValue());
    } else if (IS_COMPARISON(optype)) {
      return codegenFixedSizeColCmpFun(null, lhs_val.getValue(), rhs_val.getValue());
    } else if (IS_LOGIC(optype)) {
      return codegenFixedSizeLogicalFun(func, null, lhs_val, rhs_val);
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

JITExprValue& BinOper::codegenFixedSizeLogicalFun(JITFunction& func,
                                                  JITValuePointer& null,
                                                  FixSizeJITExprValue& lhs_val,
                                                  FixSizeJITExprValue& rhs_val) {
  switch (get_optype()) {
    case kAND: {
      // If one side is not null and false, return not null and FALSE
      // else no special handle needed
      auto if_builder = func.createIfBuilder();
      auto value = func.createVariable(JITTypeTag::BOOL, "logical_and_val");
      if_builder
          ->condition([&]() {
            auto condition = (!lhs_val.getNull() && !lhs_val.getValue()) ||
                             (!rhs_val.getNull() && !rhs_val.getValue());
            return condition;
          })
          ->ifTrue([&]() {
            null = func.createConstant(JITTypeTag::BOOL, false);
            value = func.createConstant(JITTypeTag::BOOL, false);
          })
          ->ifFalse([&]() { value = lhs_val.getValue() && rhs_val.getValue(); })
          ->build();
      return set_expr_value(null, value);
      // branchless implementation for future perf comparison
      // null = (lhs_val.getNull() && rhs_val.getNull()) ||
      //          (lhs_val.getNull() && lhs_val.getValue()) ||
      //          (rhs_val.getNull() && rhs_val.getValue());
      // return set_expr_value(null, lhs_val.getValue() && rhs_val.getValue());
    }
    case kOR: {
      // If one side is not null and true, return not null and TRUE
      // else no special handle needed
      auto if_builder = func.createIfBuilder();
      auto value = func.createVariable(JITTypeTag::BOOL, "logical_and_val");
      if_builder
          ->condition([&]() {
            auto condition = (!lhs_val.getNull() && lhs_val.getValue()) ||
                             (!rhs_val.getNull() && rhs_val.getValue());
            return condition;
          })
          ->ifTrue([&]() {
            null = func.createConstant(JITTypeTag::BOOL, false);
            value = func.createConstant(JITTypeTag::BOOL, true);
          })
          ->ifFalse([&]() { value = lhs_val.getValue() || rhs_val.getValue(); })
          ->build();
      return set_expr_value(null, value);
      // branchless implementation for future perf comparison
      // null = (lhs_val.getNull() && rhs_val.getNull()) ||
      //          (lhs_val.getNull() && !rhs_val.getValue()) ||
      //          (rhs_val.getNull() && !lhs_val.getValue());
      // return set_expr_value(null, lhs_val.getValue() || rhs_val.getValue());
    }
    default:
      UNREACHABLE();
  }
}

JITExprValue& BinOper::codegenFixedSizeDistinctFrom(JITFunction& func,
                                                    FixSizeJITExprValue& lhs_val,
                                                    FixSizeJITExprValue& rhs_val) {
  JITValuePointer value = func.createVariable(JITTypeTag::BOOL, "bw_cmp");
  // both not null and value not equal, or have different null property
  value = (!lhs_val.getNull() && !rhs_val.getNull() &&
           lhs_val.getValue() != rhs_val.getValue()) ||
          (lhs_val.getNull() != rhs_val.getNull());
  switch (get_optype()) {
    case kBW_NE: {
      return set_expr_value(func.createConstant(JITTypeTag::BOOL, false), value);
    }
    case kBW_EQ: {
      return set_expr_value(func.createConstant(JITTypeTag::BOOL, false), !value);
    }
    default:
      UNREACHABLE();
  }
}

JITExprValue& BinOper::codegenVarcharCmpFun(JITFunction& func,
                                            JITValuePointer& null,
                                            VarSizeJITExprValue& lhs,
                                            VarSizeJITExprValue& rhs) {
  const std::unordered_map<SQLOps, std::string> op_to_func_map{{kEQ, "string_eq"},
                                                               {kNE, "string_ne"},
                                                               {kLT, "string_lt"},
                                                               {kLE, "string_le"},
                                                               {kGT, "string_gt"},
                                                               {kGE, "string_ge"}};

  auto it = op_to_func_map.find(get_optype());
  if (it == op_to_func_map.end()) {
    CIDER_THROW(CiderUnsupportedException,
                fmt::format("unsupported varchar optype: {}", optype));
  }
  std::string func_name = it->second;
  auto cmp_res = func.emitRuntimeFunctionCall(
      func_name,
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::BOOL,
                                .params_vector = {lhs.getValue().get(),
                                                  lhs.getLength().get(),
                                                  rhs.getValue().get(),
                                                  rhs.getLength().get()}});

  return set_expr_value(null, cmp_res);
}

}  // namespace Analyzer
