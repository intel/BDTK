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
#include "InValues.h"
#include <type/data/sqltypes.h>
#include "exec/nextgen/context/CiderSet.h"
#include "type/plan/ConstantExpr.h"
#include "type/plan/Utils.h"

namespace Analyzer {
using namespace cider::jitlib;
using namespace cider::exec::nextgen::context;
namespace {

bool is_expr_nullable(const std::shared_ptr<Analyzer::Expr>& expr) {
  const auto const_expr = dynamic_cast<const Analyzer::Constant*>(expr.get());
  if (const_expr) {
    return const_expr->get_is_null();
  }
  const auto& expr_ti = expr->get_type_info();
  return !expr_ti.get_notnull();
}

bool is_in_values_nullable(const std::shared_ptr<Analyzer::Expr>& a,
                           const std::list<std::shared_ptr<Analyzer::Expr>>& l) {
  if (is_expr_nullable(a)) {
    return true;
  }
  for (const auto& v : l) {
    if (is_expr_nullable(v)) {
      return true;
    }
  }
  return false;
}
}  // namespace

std::shared_ptr<Analyzer::Expr> InValues::deep_copy() const {
  std::list<std::shared_ptr<Analyzer::Expr>> new_value_list;
  for (auto p : value_list) {
    new_value_list.push_back(p->deep_copy());
  }
  return makeExpr<InValues>(arg->deep_copy(), new_value_list);
}

InValues::InValues(std::shared_ptr<Analyzer::Expr> a,
                   const std::list<std::shared_ptr<Analyzer::Expr>>& l)
    : Expr(kBOOLEAN, !is_in_values_nullable(a, l)), arg(a), value_list(l) {}

void InValues::group_predicates(std::list<const Expr*>& scan_predicates,
                                std::list<const Expr*>& join_predicates,
                                std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  arg->collect_rte_idx(rte_idx_set);
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

std::shared_ptr<Analyzer::Expr> InValues::rewrite_with_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::shared_ptr<Analyzer::Expr>> new_value_list;
  for (auto v : value_list) {
    new_value_list.push_back(v->deep_copy());
  }
  return makeExpr<InValues>(arg->rewrite_with_targetlist(tlist), new_value_list);
}

std::shared_ptr<Analyzer::Expr> InValues::rewrite_with_child_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::shared_ptr<Analyzer::Expr>> new_value_list;
  for (auto v : value_list) {
    new_value_list.push_back(v->deep_copy());
  }
  return makeExpr<InValues>(arg->rewrite_with_child_targetlist(tlist), new_value_list);
}

std::shared_ptr<Analyzer::Expr> InValues::rewrite_agg_to_var(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::shared_ptr<Analyzer::Expr>> new_value_list;
  for (auto v : value_list) {
    new_value_list.push_back(v->rewrite_agg_to_var(tlist));
  }
  return makeExpr<InValues>(arg->rewrite_agg_to_var(tlist), new_value_list);
}

bool InValues::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(InValues)) {
    return false;
  }
  const InValues& rhs_iv = dynamic_cast<const InValues&>(rhs);
  if (!(*arg == *rhs_iv.get_arg())) {
    return false;
  }
  if (value_list.size() != rhs_iv.get_value_list().size()) {
    return false;
  }
  auto q = rhs_iv.get_value_list().begin();
  for (auto p : value_list) {
    if (!(*p == **q)) {
      return false;
    }
    q++;
  }
  return true;
}

std::string InValues::toString() const {
  std::string str{"(IN "};
  str += arg->toString();
  str += "(";
  int cnt = 0;
  bool shorted_value_list_str = false;
  for (auto e : value_list) {
    str += e->toString();
    cnt++;
    if (cnt > 4) {
      shorted_value_list_str = true;
      break;
    }
  }
  if (shorted_value_list_str) {
    str += "... | ";
    str += "Total # values: ";
    str += std::to_string(value_list.size());
  }
  str += ") ";
  return str;
}

void InValues::find_expr(bool (*f)(const Expr*),
                         std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  arg->find_expr(f, expr_list);
  for (auto e : value_list) {
    e->find_expr(f, expr_list);
  }
}

CiderSetPtr insertValuesToSet(
    CiderSetPtr set,
    const std::list<std::shared_ptr<Analyzer::Expr>>& val_list) {
  auto& in_val_ti = val_list.front()->get_type_info();
  for (auto in_val : val_list) {
    const auto in_val_const =
        dynamic_cast<const Analyzer::Constant*>(extract_cast_arg(in_val.get()));
    if (!in_val_const) {
      CIDER_THROW(CiderCompileException, "InValues only support constant value list.");
    }
    // For case like IN [1, 2, NULL], NULL will be ignored
    // TODO: (yma11) fot case like NOT IN [1, 2, NULL], should pops up error during
    // parse?
    switch (in_val_ti.get_type()) {
      case kTINYINT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().tinyintval);
        }
        break;
      }
      case kSMALLINT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().smallintval);
        }
        break;
      }
      case kINT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().intval);
        }
        break;
      }
      case kBIGINT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().bigintval);
        }
        break;
      }
      case kFLOAT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().floatval);
        }
        break;
      }
      case kDOUBLE: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(in_val_const->get_constval().doubleval);
        }
        break;
      }
      case kVARCHAR:
      case kCHAR:
      case kTEXT: {
        if (in_val_const->get_type_info().get_notnull()) {
          set->insert(*in_val_const->get_constval().stringval);
        }
        break;
      }
      default:
        UNIMPLEMENTED();
    }
  }
  return std::move(set);
}

std::string get_fn_name(const SQLTypeInfo& type_info) {
  switch (type_info.get_type()) {
    case kTINYINT:
      return "cider_set_contains_int8_t_val";
    case kSMALLINT:
      return "cider_set_contains_int16_t_val";
    case kINT:
      return "cider_set_contains_int32_t_val";
    case kBIGINT:
      return "cider_set_contains_int64_t_val";
    case kFLOAT:
      return "cider_set_contains_float_val";
    case kDOUBLE:
      return "cider_set_contains_double_val";
    case kVARCHAR:
    case kCHAR:
    case kTEXT:
      return "cider_set_contains_string_val";
    default:
      UNIMPLEMENTED();
  }
}

JITExprValue& InValues::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // get the constant list and insert them in the set
  auto in_arg = const_cast<Analyzer::Expr*>(get_arg());
  if (is_unnest(in_arg)) {
    CIDER_THROW(CiderCompileException, "IN not supported for unnested expressions");
  }
  const auto& expr_ti = get_type_info();
  CHECK(expr_ti.is_boolean());
  if (in_arg->get_type_info().is_string()) {
    return codegenForString(context);
  }
  FixSizeJITExprValue in_arg_val(in_arg->codegen(context));
  auto null_value = in_arg_val.getNull();
  // For values count >= 3, use CiderSet for evaluation
  // Otherwise, translate it into OR exprs
  if (get_value_list().size() >= 3) {
    CiderSetPtr cider_set;
    if (in_arg->get_type_info().is_integer()) {
      cider_set = std::make_unique<CiderInt64Set>();
    }
    if (in_arg->get_type_info().is_fp()) {
      cider_set = std::make_unique<CiderDoubleSet>();
    }
    auto filled_set = insertValuesToSet(std::move(cider_set), get_value_list());
    auto set_ptr = context.registerCiderSet("in_set", expr_ti, std::move(filled_set));
    auto emit_desc = JITFunctionEmitDescriptor{
        .ret_type = JITTypeTag::BOOL,
        .params_vector = {set_ptr.get(), in_arg_val.getValue().get()}};
    // call corresponding contains function
    auto value =
        func.emitRuntimeFunctionCall(get_fn_name(in_arg->get_type_info()), emit_desc);
    return set_expr_value(null_value, value);
  } else {
    JITValuePointer val = func.createVariable(JITTypeTag::BOOL, "null_val", false);
    for (auto in_val : get_value_list()) {
      auto in_val_const = dynamic_cast<Analyzer::Constant*>(
          const_cast<Analyzer::Expr*>(extract_cast_arg(in_val.get())));
      if (!in_val_const) {
        CIDER_THROW(CiderCompileException, "InValues only support constant value list.");
      }
      if (in_val_const->get_type_info().get_notnull()) {
        FixSizeJITExprValue in_val_const_jit(in_val_const->codegen(context));
        val.replace(val || (in_arg_val.getValue() == in_val_const_jit.getValue()));
      }
    }
    return set_expr_value(null_value, val);
  }
  UNREACHABLE();
}

JITExprValue& InValues::codegenForString(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  auto in_arg = const_cast<Analyzer::Expr*>(get_arg());
  VarSizeJITExprValue in_arg_val(in_arg->codegen(context));
  auto null_value = in_arg_val.getNull();
  // For values count >= 3, use CiderSet for evaluation
  // Otherwise, translate it into OR exprs
  if (get_value_list().size() >= 3) {
    CiderSetPtr cider_set = std::make_unique<CiderStringSet>();
    auto filled_set = insertValuesToSet(std::move(cider_set), get_value_list());
    auto set_ptr =
        context.registerCiderSet("in_set", get_type_info(), std::move(filled_set));
    auto emit_desc = JITFunctionEmitDescriptor{
        .ret_type = JITTypeTag::BOOL,
        .params_vector = {
            set_ptr.get(), in_arg_val.getValue().get(), in_arg_val.getLength().get()}};
    // call corresponding contains function
    auto value =
        func.emitRuntimeFunctionCall(get_fn_name(in_arg->get_type_info()), emit_desc);
    return set_expr_value(null_value, value);
  } else {
    JITValuePointer val = func.createVariable(JITTypeTag::BOOL, "null_val", false);
    for (auto in_val : get_value_list()) {
      auto in_val_const = dynamic_cast<Analyzer::Constant*>(
          const_cast<Analyzer::Expr*>(extract_cast_arg(in_val.get())));
      if (!in_val_const) {
        CIDER_THROW(CiderCompileException, "InValues only support constant value list.");
      }
      if (in_val_const->get_type_info().get_notnull()) {
        VarSizeJITExprValue in_val_const_jit(in_val_const->codegen(context));
        auto cmp_res = func.emitRuntimeFunctionCall(
            "string_eq",
            JITFunctionEmitDescriptor{
                .ret_type = JITTypeTag::BOOL,
                .params_vector = {in_arg_val.getValue().get(),
                                  in_arg_val.getLength().get(),
                                  in_val_const_jit.getValue().get(),
                                  in_val_const_jit.getLength().get()}});
        val.replace(val || cmp_res);
      }
    }
    return set_expr_value(null_value, val);
  }
}
}  // namespace Analyzer
