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
#include "UnaryExpr.h"
#include "exec/template/Execute.h"

namespace Analyzer {
using namespace cider::jitlib;

JITExprValue& UOper::codegen(JITFunction& func) {
  if (auto& expr_var = get_expr_value()) {
    return expr_var;
  }
  auto operand = const_cast<Analyzer::Expr*>(get_operand());
  if (is_unnest(operand) || is_unnest(operand)) {
    CIDER_THROW(CiderCompileException, "Unnest not supported in UOper");
  }
  const auto& operand_ti = operand->get_type_info();
  if (operand_ti.is_decimal() || operand_ti.is_timeinterval()) {
    CIDER_THROW(CiderCompileException,
                "Decimal and TimeInterval are not supported in Uoper codegen now.");
  }
  FixSizeJITExprValue operand_val(operand->codegen(func));
  const auto& ti = get_type_info();
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();
  switch (get_optype()) {
    case kISNULL: {
      // For ISNULL, the null will always be false
      auto null_value = operand_val.getNull().get()
                            ? operand_val.getNull()
                            : func.createConstant(getJITTag(type), false);
      return set_expr_value(func.createConstant(getJITTag(type), false), null_value);
    }
    case kISNOTNULL: {
      auto null_value = operand_val.getNull().get()
                            ? operand_val.getNull()->notOp()
                            : func.createConstant(getJITTag(type), true);
      return set_expr_value(func.createConstant(getJITTag(type), false), null_value);
    }
    case kNOT: {
      CHECK(operand_val.getNull().get());
      return set_expr_value(operand_val.getNull(), operand_val.getValue()->notOp());
    }
    case kCAST: {
      return set_expr_value(operand_val.getNull(),
                            codegenCastFunc(func, operand_val.getValue()));
    }
    default:
      UNIMPLEMENTED();
  }
}

JITValuePointer UOper::codegenCastFunc(JITFunction& func, JITValue& operand_val) {
  const auto& ti = this->get_type_info();
  const auto& operand_ti = this->get_operand()->get_type_info();
  JITTypeTag ti_jit_tag = getJITTypeTag(ti.get_type());
  if (operand_ti.is_string() || ti.is_string()) {
    UNIMPLEMENTED();
  } else if (operand_ti.is_time() || ti.is_time()) {
    UNIMPLEMENTED();
  } else if (operand_ti.is_integer()) {
    if (ti.is_fp() || ti.is_integer()) {
      return operand_val.castJITValuePrimitiveType(ti_jit_tag);
    }
  } else if (operand_ti.is_fp()) {
    // Round by adding/subtracting 0.5 before fptosi.
    if (ti.is_integer()) {
      func.createIfBuilder()
          ->condition([&]() { return operand_val < 0; })
          ->ifTrue([&]() { operand_val = operand_val - 0.5; })
          ->ifFalse([&]() { operand_val = operand_val + 0.5; })
          ->build();
    }
    return operand_val.castJITValuePrimitiveType(ti_jit_tag);
  }
  CIDER_THROW(CiderCompileException,
              fmt::format("cast type:{} into type:{} not support yet",
                          operand_ti.get_type_name(),
                          ti.get_type_name()));
}

std::shared_ptr<Analyzer::Expr> UOper::deep_copy() const {
  return makeExpr<UOper>(type_info, contains_agg, optype, operand->deep_copy());
}

std::shared_ptr<Analyzer::Expr> UOper::add_cast(const SQLTypeInfo& new_type_info) {
  if (optype != kCAST) {
    return Expr::add_cast(new_type_info);
  }
  if (type_info.is_string() && new_type_info.is_string() &&
      new_type_info.get_compression() == kENCODING_DICT &&
      type_info.get_compression() == kENCODING_NONE) {
    const SQLTypeInfo oti = operand->get_type_info();
    if (oti.is_string() && oti.get_compression() == kENCODING_DICT &&
        (oti.get_comp_param() == new_type_info.get_comp_param() ||
         oti.get_comp_param() == TRANSIENT_DICT(new_type_info.get_comp_param()))) {
      auto result = operand;
      operand = nullptr;
      return result;
    }
  }
  return Expr::add_cast(new_type_info);
}

void UOper::check_group_by(
    const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const {
  operand->check_group_by(groupby);
}

void UOper::group_predicates(std::list<const Expr*>& scan_predicates,
                             std::list<const Expr*>& join_predicates,
                             std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  operand->collect_rte_idx(rte_idx_set);
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

bool UOper::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(UOper)) {
    return false;
  }
  const UOper& rhs_uo = dynamic_cast<const UOper&>(rhs);
  return optype == rhs_uo.get_optype() && *operand == *rhs_uo.get_operand();
}

std::string UOper::toString() const {
  std::string op;
  switch (optype) {
    case kNOT:
      op = "NOT ";
      break;
    case kUMINUS:
      op = "- ";
      break;
    case kISNULL:
      op = "IS NULL ";
      break;
    case kEXISTS:
      op = "EXISTS ";
      break;
    case kCAST:
      op = "CAST " + type_info.get_type_name() + "(" +
           std::to_string(type_info.get_precision()) + "," +
           std::to_string(type_info.get_scale()) + ") " +
           type_info.get_compression_name() + "(" +
           std::to_string(type_info.get_comp_param()) + ") ";
      break;
    case kUNNEST:
      op = "UNNEST ";
      break;
    default:
      break;
  }
  return "(" + op + operand->toString() + ") ";
}

void UOper::find_expr(bool (*f)(const Expr*), std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  operand->find_expr(f, expr_list);
}

}  // namespace Analyzer
