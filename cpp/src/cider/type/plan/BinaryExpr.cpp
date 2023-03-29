/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#include <cstddef>
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/utils/JITExprValue.h"
#include "type/plan/ConstantExpr.h"
#include "type/plan/Utils.h"  // for is_unnest
#include "util/Logger.h"

namespace Analyzer {
using namespace cider::jitlib;

void BinOper::initAutoVectorizeFlag() {
  if (left_operand->isAutoVectorizable() && right_operand->isAutoVectorizable()) {
    auto op_type = get_optype();
    // TODO (bigPYJ1151): Support more operations and types.
    switch (op_type) {
      case kPLUS:
      case kMINUS:
      case kMULTIPLY:
      case kAND:
      case kOR:
      case kEQ:
      case kNE:
      case kLT:
      case kGT:
      case kLE:
      case kGE:
        auto_vectorizable_ = true;
        break;
      case kDIVIDE: {
        if (dynamic_cast<Constant*>(right_operand.get())) {
          auto_vectorizable_ = true;
        } else {
          auto_vectorizable_ = false;
        }
        break;
      }
      default:
        auto_vectorizable_ = false;
    }
  } else {
    auto_vectorizable_ = false;
  }
}

int32_t getBinOpRescaledFactor(const SQLOps ops,
                               const int32_t self_scale,
                               const int32_t other_scale,
                               const int32_t res_scale,
                               const bool first_operand) {
  switch (ops) {
    case kPLUS:
    case kMINUS:
      return res_scale - self_scale;
    case kMULTIPLY:
      return 0;
    case kDIVIDE:
    case kMODULO:
      return first_operand ? (other_scale + res_scale - self_scale) : 0;
    case kEQ:
    case kBW_EQ:
    case kNE:
    case kLT:
    case kGT:
    case kLE:
    case kGE:
    case kBW_NE:
      return std::max(self_scale, other_scale) - self_scale;
    default:
      return 0;
  }
}

JITValuePointer getRescaledValue(CodegenContext& context,
                                 FixSizeJITExprValue& expr_val,
                                 const int32_t scaled_factor) {
  JITFunction& func = *context.getJITFunction();
  auto operand_val = expr_val.getValue();
  if (scaled_factor == 0) {
    return operand_val;
  }
  // only do upscale to keep precision
  CHECK_GT(scaled_factor, 0);
  JITValuePointer scaled_val =
      func.createLiteral(operand_val->getValueTypeTag(), exp_to_scale(scaled_factor));
  // TODO(kaidi): cast overflow check
  JITValuePointer rescaled_val =
      func.createVariable(operand_val->getValueTypeTag(), "rescaled_val", 0);
  rescaled_val = scaled_val * operand_val;
  return rescaled_val;
}

JITExprValue& BinOper::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
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
  if (lhs_ti.is_timeinterval()) {
    CIDER_THROW(CiderCompileException,
                "TimeInterval is not supported in arithmetic codegen now.");
  }
  if (lhs_ti.is_string()) {
    // string binops, should only be comparisons
    const auto optype = get_optype();
    if (IS_COMPARISON(optype)) {
      VarSizeJITExprValue lhs_val(lhs->codegen(context));
      VarSizeJITExprValue rhs_val(rhs->codegen(context));
      JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "null_val");
      null = lhs_val.getNull() || rhs_val.getNull();
      return codegenVarcharCmpFun(func, null, lhs_val, rhs_val);
    } else {
      CIDER_THROW(CiderUnsupportedException, "string BinOp only supports comparison");
    }
  } else {
    // primitive type binops
    FixSizeJITExprValue lhs_expr_val(lhs->codegen(context));
    FixSizeJITExprValue rhs_expr_val(rhs->codegen(context));

    if (get_optype() == kBW_EQ or get_optype() == kBW_NE) {
      return codegenFixedSizeDistinctFrom(func, lhs_expr_val, rhs_expr_val);
    }

    const auto optype = get_optype();
    // rescale decimal value before integer calculation
    auto lhs_jit_val =
        !lhs_ti.is_decimal()
            ? lhs_expr_val.getValue()
            : getRescaledValue(context,
                               lhs_expr_val,
                               getBinOpRescaledFactor(optype,
                                                      lhs_ti.get_scale(),
                                                      rhs_ti.get_scale(),
                                                      get_type_info().get_scale(),
                                                      true));
    auto rhs_jit_val =
        !rhs_ti.is_decimal()
            ? rhs_expr_val.getValue()
            : getRescaledValue(context,
                               rhs_expr_val,
                               getBinOpRescaledFactor(optype,
                                                      rhs_ti.get_scale(),
                                                      lhs_ti.get_scale(),
                                                      get_type_info().get_scale(),
                                                      false));
    if (IS_ARITHMETIC(optype)) {
      auto null = lhs_expr_val.getNull() || rhs_expr_val.getNull();
      return codegenFixedSizeColArithFun(context, null, lhs_jit_val, rhs_jit_val);
    } else if (IS_COMPARISON(optype)) {
      auto null = lhs_expr_val.getNull() || rhs_expr_val.getNull();
      return codegenFixedSizeColCmpFun(null, lhs_jit_val, rhs_jit_val);
    } else if (IS_LOGIC(optype)) {
      return codegenFixedSizeLogicalFun(context, func, lhs_expr_val, rhs_expr_val);
    }
  }
  UNREACHABLE();
  return expr_var_;
}

JITValuePointer BinOper::codegenArithWithErrorCheck(JITValuePointer lhs,
                                                    JITValuePointer rhs) {
  switch (get_optype()) {
    case kMINUS:
      return lhs->subWithErrorCheck(rhs);
    case kPLUS:
      return lhs->addWithErrorCheck(rhs);
    case kMULTIPLY:
      return lhs->mulWithErrorCheck(rhs);
    case kDIVIDE:
      return lhs->divWithErrorCheck(rhs);
    case kMODULO:
      return lhs->modWithErrorCheck(rhs);
    default:
      UNREACHABLE();
  }
  return JITValuePointer(nullptr);
}

JITExprValue& BinOper::codegenFixedSizeColArithFun(CodegenContext& context,
                                                   JITValuePointer null,
                                                   JITValuePointer lhs,
                                                   JITValuePointer rhs) {
  bool needs_error_check = context.getCodegenOptions().needs_error_check;
  JITFunction& func = *context.getJITFunction();

  // No need to compute if any operand is null, return a null result directly
  JITValuePointer res_val = func.createVariable(lhs->getValueTypeTag(), "res_val");
  func.createIfBuilder()
      ->condition([&]() { return null; })
      ->ifTrue([&]() { *res_val = *lhs; })
      ->ifFalse([&]() {
        if (needs_error_check) {
          res_val = codegenArithWithErrorCheck(lhs, rhs);
        } else {
          switch (get_optype()) {
            case kMINUS:
              res_val = lhs - rhs;
              break;
            case kPLUS:
              res_val = lhs + rhs;
              break;
            case kMULTIPLY:
              res_val = lhs * rhs;
              break;
            case kDIVIDE:
              res_val = lhs / rhs;
              break;
            case kMODULO:
              res_val = lhs % rhs;
              break;
            default:
              UNREACHABLE();
          }
        }
      })
      ->build();
  return set_expr_value(null, res_val);
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

JITExprValue& BinOper::codegenFixedSizeLogicalFun(CodegenContext& context,
                                                  JITFunction& func,
                                                  FixSizeJITExprValue& lhs_val,
                                                  FixSizeJITExprValue& rhs_val) {
  // branchless implementation for future perf comparison
  bool branchless_logic = context.getCodegenOptions().branchless_logic;
  if (branchless_logic) {
    switch (get_optype()) {
      case kAND: {
        //|     |TRUE |FALSE|NULL |
        //|TRUE |TRUE |FALSE|NULL |
        //|FALSE|FALSE|FALSE|FALSE|
        //|NULL |NULL |FALSE|NULL |
        auto lhs_null = lhs_val.getNull();
        auto lhs_data = lhs_val.getValue();
        auto rhs_null = rhs_val.getNull();
        auto rhs_data = rhs_val.getValue();
        auto null =
            is_trivial_null_process_
                ? (lhs_null & rhs_null)
                : (lhs_null & rhs_null) | (lhs_null & rhs_data) | (rhs_null & lhs_data);
        return set_expr_value(null, lhs_data & rhs_data);
      }
      case kOR: {
        // If one side is not null and true, return not null and TRUE
        // else no special handle needed
        auto lhs_null = lhs_val.getNull();
        auto lhs_data = lhs_val.getValue();
        auto rhs_null = rhs_val.getNull();
        auto rhs_data = rhs_val.getValue();
        auto null =
            is_trivial_null_process_
                ? (lhs_null & rhs_null)
                : (lhs_null & rhs_null) | (lhs_null & ~rhs_data) | (rhs_null & ~lhs_data);
        return set_expr_value(null, lhs_data | rhs_data);
      }
      default:
        UNREACHABLE();
    }
    return expr_var_;
  }

  switch (get_optype()) {
    case kAND: {
      // If one side is not null and false, return not null and FALSE
      // else no special handle needed
      JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "logical_and_null");
      auto value = func.createVariable(JITTypeTag::BOOL, "logical_and_val");
      auto if_builder = func.createIfBuilder();
      // getNull = 0 represents it's null
      if_builder
          ->condition([&]() {
            auto condition = (!lhs_val.getNull() && !lhs_val.getValue()) ||
                             (!rhs_val.getNull() && !rhs_val.getValue());
            return condition;
          })
          ->ifTrue([&]() {
            null = func.createLiteral(JITTypeTag::BOOL, false);
            value = func.createLiteral(JITTypeTag::BOOL, false);
          })
          ->ifFalse([&]() {
            null = lhs_val.getNull() || rhs_val.getNull();
            value = lhs_val.getValue() && rhs_val.getValue();
          })
          ->build();
      return set_expr_value(null, value);
    }
    case kOR: {
      // If one side is not null and true, return not null and TRUE
      // else no special handle needed
      JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "logical_or_null");
      auto value = func.createVariable(JITTypeTag::BOOL, "logical_or_val");
      auto if_builder = func.createIfBuilder();
      if_builder
          ->condition([&]() {
            auto condition = (!lhs_val.getNull() && lhs_val.getValue()) ||
                             (!rhs_val.getNull() && rhs_val.getValue());
            return condition;
          })
          ->ifTrue([&]() {
            null = func.createLiteral(JITTypeTag::BOOL, false);
            value = func.createLiteral(JITTypeTag::BOOL, true);
          })
          ->ifFalse([&]() {
            null = lhs_val.getNull() || rhs_val.getNull();
            value = lhs_val.getValue() || rhs_val.getValue();
          })
          ->build();
      return set_expr_value(null, value);
    }
    default:
      UNREACHABLE();
  }
  return expr_var_;
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
      return set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), value);
    }
    case kBW_EQ: {
      return set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), !value);
    }
    default:
      UNREACHABLE();
  }
  return expr_var_;
}

JITExprValue& BinOper::codegenVarcharCmpFun(JITFunction& func,
                                            JITValuePointer& null,
                                            VarSizeJITExprValue& lhs,
                                            VarSizeJITExprValue& rhs) {
  if (get_optype() == kBW_EQ or get_optype() == kBW_NE) {
    return codegenVarcharDistinctFrom(func, lhs, rhs);
  }
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

JITExprValue& BinOper::codegenVarcharDistinctFrom(JITFunction& func,
                                                  VarSizeJITExprValue& lhs,
                                                  VarSizeJITExprValue& rhs) {
  JITValuePointer value = func.createVariable(JITTypeTag::BOOL, "bw_cmp");

  auto cmp_res = func.emitRuntimeFunctionCall(
      "string_ne",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::BOOL,
                                .params_vector = {lhs.getValue().get(),
                                                  lhs.getLength().get(),
                                                  rhs.getValue().get(),
                                                  rhs.getLength().get()}});
  // both not null and value not equal, or have different null property
  value =
      (!lhs.getNull() && !rhs.getNull() && cmp_res) || (lhs.getNull() != rhs.getNull());
  switch (get_optype()) {
    case kBW_NE: {
      return set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), value);
    }
    case kBW_EQ: {
      return set_expr_value(func.createLiteral(JITTypeTag::BOOL, false), !value);
    }
    default:
      UNREACHABLE();
  }
  return expr_var_;
}

}  // namespace Analyzer
