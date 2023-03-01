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
#include "type/plan/CaseExpr.h"

namespace Analyzer {
using namespace cider::jitlib;

std::shared_ptr<Analyzer::Expr> CaseExpr::deep_copy() const {
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      new_list;
  for (auto p : expr_pair_list) {
    new_list.emplace_back(p.first->deep_copy(), p.second->deep_copy());
  }
  return makeExpr<CaseExpr>(type_info,
                            contains_agg,
                            new_list,
                            else_expr == nullptr ? nullptr : else_expr->deep_copy());
}

std::shared_ptr<Analyzer::Expr> CaseExpr::add_cast(const SQLTypeInfo& new_type_info) {
  SQLTypeInfo ti = new_type_info;
  if (new_type_info.is_string() && new_type_info.get_compression() == kENCODING_DICT &&
      new_type_info.get_comp_param() == TRANSIENT_DICT_ID && type_info.is_string() &&
      type_info.get_compression() == kENCODING_NONE &&
      type_info.get_comp_param() > TRANSIENT_DICT_ID) {
    ti.set_comp_param(TRANSIENT_DICT(type_info.get_comp_param()));
  }

  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      new_expr_pair_list;
  for (auto& p : expr_pair_list) {
    new_expr_pair_list.emplace_back(
        std::make_pair(p.first, p.second->deep_copy()->add_cast(ti)));
  }

  if (else_expr != nullptr) {
    else_expr = else_expr->add_cast(ti);
  }
  // Replace the current WHEN THEN pair list once we are sure all casts have succeeded
  expr_pair_list = new_expr_pair_list;

  type_info = ti;
  return shared_from_this();
}

void CaseExpr::group_predicates(std::list<const Expr*>& scan_predicates,
                                std::list<const Expr*>& join_predicates,
                                std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  for (auto p : expr_pair_list) {
    p.first->collect_rte_idx(rte_idx_set);
    p.second->collect_rte_idx(rte_idx_set);
  }
  if (else_expr != nullptr) {
    else_expr->collect_rte_idx(rte_idx_set);
  }
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

std::shared_ptr<Analyzer::Expr> CaseExpr::rewrite_with_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      epair_list;
  for (auto p : expr_pair_list) {
    epair_list.emplace_back(p.first->rewrite_with_targetlist(tlist),
                            p.second->rewrite_with_targetlist(tlist));
  }
  return makeExpr<CaseExpr>(
      type_info,
      contains_agg,
      epair_list,
      else_expr ? else_expr->rewrite_with_targetlist(tlist) : nullptr);
}

std::shared_ptr<Analyzer::Expr> CaseExpr::rewrite_with_child_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      epair_list;
  for (auto p : expr_pair_list) {
    epair_list.emplace_back(p.first->rewrite_with_child_targetlist(tlist),
                            p.second->rewrite_with_child_targetlist(tlist));
  }
  return makeExpr<CaseExpr>(
      type_info,
      contains_agg,
      epair_list,
      else_expr ? else_expr->rewrite_with_child_targetlist(tlist) : nullptr);
}

std::shared_ptr<Analyzer::Expr> CaseExpr::rewrite_agg_to_var(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      epair_list;
  for (auto p : expr_pair_list) {
    epair_list.emplace_back(p.first->rewrite_agg_to_var(tlist),
                            p.second->rewrite_agg_to_var(tlist));
  }
  return makeExpr<CaseExpr>(type_info,
                            contains_agg,
                            epair_list,
                            else_expr ? else_expr->rewrite_agg_to_var(tlist) : nullptr);
}

bool CaseExpr::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(CaseExpr)) {
    return false;
  }
  const CaseExpr& rhs_ce = dynamic_cast<const CaseExpr&>(rhs);
  if (expr_pair_list.size() != rhs_ce.get_expr_pair_list().size()) {
    return false;
  }
  if ((else_expr == nullptr && rhs_ce.get_else_expr() != nullptr) ||
      (else_expr != nullptr && rhs_ce.get_else_expr() == nullptr)) {
    return false;
  }
  auto it = rhs_ce.get_expr_pair_list().cbegin();
  for (auto p : expr_pair_list) {
    if (!(*p.first == *it->first) || !(*p.second == *it->second)) {
      return false;
    }
    ++it;
  }
  return else_expr == nullptr ||
         (else_expr != nullptr && *else_expr == *rhs_ce.get_else_expr());
}

std::string CaseExpr::toString() const {
  std::string str{"CASE "};
  for (auto p : expr_pair_list) {
    str += "(";
    str += p.first->toString();
    str += ", ";
    str += p.second->toString();
    str += ") ";
  }
  if (else_expr) {
    str += "ELSE ";
    str += else_expr->toString();
  }
  str += " END ";
  return str;
}

void CaseExpr::find_expr(bool (*f)(const Expr*),
                         std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  for (auto p : expr_pair_list) {
    p.first->find_expr(f, expr_list);
    p.second->find_expr(f, expr_list);
  }
  if (else_expr != nullptr) {
    else_expr->find_expr(f, expr_list);
  }
}

void CaseExpr::collect_rte_idx(std::set<int>& rte_idx_set) const {
  for (auto p : expr_pair_list) {
    p.first->collect_rte_idx(rte_idx_set);
    p.second->collect_rte_idx(rte_idx_set);
  }
  if (else_expr != nullptr) {
    else_expr->collect_rte_idx(rte_idx_set);
  }
}

void CaseExpr::collect_column_var(
    std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>& colvar_set,
    bool include_agg) const {
  for (auto p : expr_pair_list) {
    p.first->collect_column_var(colvar_set, include_agg);
    p.second->collect_column_var(colvar_set, include_agg);
  }
  if (else_expr != nullptr) {
    else_expr->collect_column_var(colvar_set, include_agg);
  }
}

void CaseExpr::check_group_by(
    const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const {
  for (auto p : expr_pair_list) {
    p.first->check_group_by(groupby);
    p.second->check_group_by(groupby);
  }
  if (else_expr != nullptr) {
    else_expr->check_group_by(groupby);
  }
}

JITExprValue& CaseExpr::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  const auto& expr_pair_list = get_expr_pair_list();
  const auto& else_expr = get_else_ref();
  CHECK_GT(expr_pair_list.size(), 0);
  const auto case_ti = get_type_info();
  if (case_ti.is_integer() || case_ti.is_time() || case_ti.is_decimal() ||
      case_ti.is_fp() || case_ti.is_boolean()) {
    const auto type =
        case_ti.is_decimal() ? decimal_to_int_type(case_ti) : case_ti.get_type();
    JITValuePointer value = func.createVariable(getJITTag(type), "case_when_value_init");
    *value = func.createLiteral(getJITTag(type), 0);
    JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "case_when_null_init");
    *null = func.createLiteral(JITTypeTag::BOOL, false);
    JITValuePointer is_case = func.createVariable(JITTypeTag::BOOL, "is_case_init");
    *is_case = func.createLiteral(JITTypeTag::BOOL, false);
    std::for_each(
        expr_pair_list.rbegin(),
        expr_pair_list.rend(),
        [&func, &context, &value, &null, &is_case](
            const std::pair<std::shared_ptr<Analyzer::Expr>,
                            std::shared_ptr<Analyzer::Expr>>& expr_pair) {
          func.createIfBuilder()
              ->condition([&]() {
                cider::exec::nextgen::utils::FixSizeJITExprValue cond(
                    expr_pair.first->codegen(context));
                auto condition = !cond.getNull() && cond.getValue();
                return condition;
              })
              ->ifTrue([&]() {
                cider::exec::nextgen::utils::FixSizeJITExprValue then_jit_expr_value(
                    expr_pair.second->codegen(context));
                *value = *then_jit_expr_value.getValue();
                *null = *then_jit_expr_value.getNull();
                *is_case = func.createLiteral(JITTypeTag::BOOL, true);
              })
              ->build();
        });
    func.createIfBuilder()
        ->condition([&]() { return !is_case; })
        ->ifTrue([&]() {
          cider::exec::nextgen::utils::FixSizeJITExprValue else_jit_expr_value(
              else_expr->codegen(context));
          *value = *else_jit_expr_value.getValue();
          *null = *else_jit_expr_value.getNull();
        })
        ->build();
    return set_expr_value(null, value);
  } else if (case_ti.is_string()) {
    JITValuePointer value = func.emitRuntimeFunctionCall(
        "create_a_pointer",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .ret_sub_type = JITTypeTag::INT8,
                                  .params_vector = {}});
    //     func.createVariable(JITTypeTag::INT64, "case_when_value_init");
    // *value = func.createLiteral(JITTypeTag::INT64, 0);
    // value = value->castJITValuePrimitiveType(JITTypeTag::POINTER);
    JITValuePointer length =
        func.createVariable(JITTypeTag::INT32, "case_when_length_init");
    *length = func.createLiteral(JITTypeTag::INT32, 0);
    JITValuePointer null = func.createVariable(JITTypeTag::BOOL, "case_when_null_init");
    *null = func.createLiteral(JITTypeTag::BOOL, false);
    JITValuePointer is_case = func.createVariable(JITTypeTag::BOOL, "is_case_init");
    *is_case = func.createLiteral(JITTypeTag::BOOL, false);
    std::for_each(
        expr_pair_list.rbegin(),
        expr_pair_list.rend(),
        [&func, &context, &value, &length, &null, &is_case](
            const std::pair<std::shared_ptr<Analyzer::Expr>,
                            std::shared_ptr<Analyzer::Expr>>& expr_pair) {
          func.createIfBuilder()
              ->condition([&]() {
                cider::exec::nextgen::utils::FixSizeJITExprValue cond(
                    expr_pair.first->codegen(context));
                auto condition = !cond.getNull() && cond.getValue();
                return condition;
              })
              ->ifTrue([&]() {
                cider::exec::nextgen::utils::VarSizeJITExprValue then_jit_expr_value(
                    expr_pair.second->codegen(context));
                value.replace(then_jit_expr_value.getValue());
                *length = *then_jit_expr_value.getLength();
                *null = *then_jit_expr_value.getNull();
                *is_case = func.createLiteral(JITTypeTag::BOOL, true);
              })
              ->build();
        });
    func.createIfBuilder()
        ->condition([&]() { return !is_case; })
        ->ifTrue([&]() {
          cider::exec::nextgen::utils::VarSizeJITExprValue else_jit_expr_value(
              else_expr->codegen(context));
          value.replace(else_jit_expr_value.getValue());
          *length = *else_jit_expr_value.getLength();
          *null = *else_jit_expr_value.getNull();
        })
        ->build();
    return set_expr_value(null, length, value);
  } else {
    UNREACHABLE();
  }
  return get_expr_value();
}

ExprPtrRefVector CaseExpr::get_children_reference() {
  ExprPtrRefVector result;
  for (auto& when_expr_pair : expr_pair_list) {
    result.push_back(&when_expr_pair.first);
    result.push_back(&when_expr_pair.second);
  }
  result.push_back(&else_expr);
  return result;
}

}  // namespace Analyzer
