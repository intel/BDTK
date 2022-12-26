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
#include "type/plan/DateExpr.h"

namespace Analyzer {
using namespace cider::jitlib;

JITValuePointer getScaledIntervalAndFunc(JITValuePointer interval,
                                         SQLTypes from_type,
                                         DateaddField to_type,
                                         std::string& func_name) {
  if (from_type == kINTERVAL_DAY_TIME) {
    func_name = func_name + "Seconds";
    switch (to_type) {
      case daSECOND:
        return interval;
      case daMINUTE:
        return interval * kSecsPerMin;
      case daHOUR:
        return interval * kSecsPerHour;
      case daWEEKDAY:
      case daDAYOFYEAR:
      case daDAY:
        return interval * kSecsPerDay;
      case daWEEK:
        return interval * (7 * kSecsPerDay);
    }
  } else if (from_type == kINTERVAL_YEAR_MONTH) {
    func_name = func_name + "Months";
    switch (to_type) {
      case daMONTH:
        return interval;
      case daQUARTER:
        return interval * 3;
      case daYEAR:
        return interval * 12;
      case daDECADE:
        return interval * 120;
      case daCENTURY:
        return interval * 1200;
      case daMILLENNIUM:
        return interval * 12000;
    }
  }
  CIDER_THROW(
      CiderUnsupportedException,
      fmt::format("date interval type is {}, field is {}", toString(from_type), to_type));
}

JITExprValue& DateaddExpr::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  const SQLTypeInfo& expr_ti = get_type_info();
  CHECK(expr_ti.get_type() == kTIMESTAMP || expr_ti.get_type() == kDATE);
  FixSizeJITExprValue datetime(get_datetime_expr()->codegen(context));
  FixSizeJITExprValue interval(get_number_expr()->codegen(context));
  const SQLTypeInfo& datetime_ti = get_datetime_expr()->get_type_info();
  const SQLTypeInfo& interval_ti = get_number()->get_type_info();

  std::string func_name = datetime_ti.get_type() == kDATE ? "DateAdd" : "TimeAdd";
  JITValuePointer datetime_val = datetime.getValue();
  JITValuePointer interval_val =
      getScaledIntervalAndFunc(JITValuePointer(interval.getValue().get()),
                               interval_ti.get_type(),
                               get_field(),
                               func_name);

  if (datetime_ti.is_high_precision_timestamp()) {
    func_name = func_name + "HighPrecision";
    JITValuePointer dim_val =
        func.createLiteral(JITTypeTag::INT32, datetime_ti.get_dimension());
    JITValuePointer res_val = func.emitRuntimeFunctionCall(
        func_name,
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::INT64,
            .params_vector = {datetime_val.get(), interval_val.get(), dim_val.get()}});
    return set_expr_value(datetime.getNull(), res_val);
  }
  JITValuePointer res_val = func.emitRuntimeFunctionCall(
      func_name,
      JITFunctionEmitDescriptor{
          .ret_type =
              datetime_ti.get_type() == kDATE ? JITTypeTag::INT32 : JITTypeTag::INT64,
          .params_vector = {datetime_val.get(), interval_val.get()}});
  return set_expr_value(datetime.getNull(), res_val);
}

bool DateaddExpr::operator==(const Expr& rhs) const {
  if (typeid(rhs) != typeid(DateaddExpr)) {
    return false;
  }
  const DateaddExpr& rhs_ee = dynamic_cast<const DateaddExpr&>(rhs);
  return field_ == rhs_ee.get_field() && *number_ == *rhs_ee.get_number_expr() &&
         *datetime_ == *rhs_ee.get_datetime_expr();
}

std::string DateaddExpr::toString() const {
  return "DATEADD(" + std::to_string(field_) + " NUMBER " + number_->toString() +
         " DATETIME " + datetime_->toString() + ") ";
}

void DateaddExpr::find_expr(bool (*f)(const Expr*),
                            std::list<const Expr*>& expr_list) const {
  if (f(this)) {
    add_unique(expr_list);
    return;
  }
  number_->find_expr(f, expr_list);
  datetime_->find_expr(f, expr_list);
}

std::shared_ptr<Analyzer::Expr> DateaddExpr::deep_copy() const {
  return makeExpr<DateaddExpr>(
      type_info, field_, number_->deep_copy(), datetime_->deep_copy());
}

std::shared_ptr<Analyzer::Expr> DateaddExpr::rewrite_with_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  return makeExpr<DateaddExpr>(type_info,
                               field_,
                               number_->rewrite_with_targetlist(tlist),
                               datetime_->rewrite_with_targetlist(tlist));
}

std::shared_ptr<Analyzer::Expr> DateaddExpr::rewrite_with_child_targetlist(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  return makeExpr<DateaddExpr>(type_info,
                               field_,
                               number_->rewrite_with_child_targetlist(tlist),
                               datetime_->rewrite_with_child_targetlist(tlist));
}

std::shared_ptr<Analyzer::Expr> DateaddExpr::rewrite_agg_to_var(
    const std::vector<std::shared_ptr<TargetEntry>>& tlist) const {
  return makeExpr<DateaddExpr>(type_info,
                               field_,
                               number_->rewrite_agg_to_var(tlist),
                               datetime_->rewrite_agg_to_var(tlist));
}

void DateaddExpr::collect_rte_idx(std::set<int>& rte_idx_set) const {
  number_->collect_rte_idx(rte_idx_set);
  datetime_->collect_rte_idx(rte_idx_set);
}

void DateaddExpr::collect_column_var(
    std::set<const ColumnVar*, bool (*)(const ColumnVar*, const ColumnVar*)>& colvar_set,
    bool include_agg) const {
  number_->collect_column_var(colvar_set, include_agg);
  datetime_->collect_column_var(colvar_set, include_agg);
}

void DateaddExpr::check_group_by(
    const std::list<std::shared_ptr<Analyzer::Expr>>& groupby) const {
  number_->check_group_by(groupby);
  datetime_->check_group_by(groupby);
}

void DateaddExpr::group_predicates(std::list<const Expr*>& scan_predicates,
                                   std::list<const Expr*>& join_predicates,
                                   std::list<const Expr*>& const_predicates) const {
  std::set<int> rte_idx_set;
  number_->collect_rte_idx(rte_idx_set);
  datetime_->collect_rte_idx(rte_idx_set);
  if (rte_idx_set.size() > 1) {
    join_predicates.push_back(this);
  } else if (rte_idx_set.size() == 1) {
    scan_predicates.push_back(this);
  } else {
    const_predicates.push_back(this);
  }
}

}  // namespace Analyzer
