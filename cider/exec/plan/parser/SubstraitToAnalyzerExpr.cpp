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

/**
 * @file    SubstraitToAnalyzerExpr.cpp
 * @brief   Translate Substrait expression to Analyzer expression
 **/
#include "exec/plan/parser/SubstraitToAnalyzerExpr.h"
#include <cstdint>
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/ParserNode.h"
#include "exec/template/DateTimeTranslator.h"
#include "util/DateTimeParser.h"

namespace generator {
bool getExprUpdatable(std::unordered_map<std::shared_ptr<Analyzer::Expr>, bool> map,
                      std::shared_ptr<Analyzer::Expr> expr) {
  return map.find(expr) == map.end() || !map.find(expr)->second;
}

bool isStringFunction(const std::string& function_name) {
  std::unordered_set<std::string> supportedStrFunctionSet{
      "substring", "substr", "lower", "upper", "trim", "ltrim", "rtrim"};
  return supportedStrFunctionSet.find(function_name) != supportedStrFunctionSet.end();
}

std::shared_ptr<Analyzer::ColumnVar> Substrait2AnalyzerExprConverter::makeColumnVar(
    const SQLTypeInfo& ti,
    int table_id,
    int col_id,
    int nest_level,
    bool updated) {
  auto col_var = std::make_shared<Analyzer::ColumnVar>(ti, table_id, col_id, nest_level);
  // Set col_var not updateable in current visited plan node
  // Will reset to updatable once current visit finishes
  cols_update_stat_.insert(std::pair(col_var, true));
  return col_var;
}

std::shared_ptr<Analyzer::Var> Substrait2AnalyzerExprConverter::makeVar(
    const SQLTypeInfo& ti,
    int r,
    int c,
    int i,
    bool is_virtual,
    Analyzer::Var::WhichRow o,
    int v,
    bool updated) {
  auto var = std::make_shared<Analyzer::Var>(ti,
                                             r,
                                             c,
                                             i,
                                             is_virtual,  // FIXME
                                             o,
                                             v);

  cols_update_stat_.insert(std::pair(var, true));
  return var;
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression& s_expr,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  switch (s_expr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kLiteral:
      return toAnalyzerExpr(s_expr.literal());
    case substrait::Expression::RexTypeCase::kSelection:
      return toAnalyzerExpr(s_expr.selection(), function_map, expr_map_ptr);
    case substrait::Expression::RexTypeCase::kScalarFunction:
      return toAnalyzerExpr(s_expr.scalar_function(), function_map, expr_map_ptr);
    case substrait::Expression::RexTypeCase::kCast:
      return toAnalyzerExpr(s_expr.cast(), function_map, expr_map_ptr);
    case substrait::Expression::RexTypeCase::kIfThen:
      return toAnalyzerExpr(s_expr.if_then(), function_map, expr_map_ptr);
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported expression type {}", s_expr.rex_type_case()));
  }
}

std::list<std::shared_ptr<Analyzer::Expr>>
Substrait2AnalyzerExprConverter::toAnalyzerExprList(
    const substrait::Expression_Literal& s_literal_expr) {
  CHECK(s_literal_expr.has_list());
  std::list<std::shared_ptr<Analyzer::Expr>> c_exprs;
  for (auto literal : s_literal_expr.list().values()) {
    c_exprs.emplace_back(toAnalyzerExpr(literal));
  }
  return c_exprs;
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::updateAnalyzerExpr(
    std::shared_ptr<Analyzer::Expr> expr,
    std::int32_t pre_index,
    std::int32_t cur_index,
    int table_id,
    int rte_idx,
    std::shared_ptr<Analyzer::Expr> cur_expr,
    const substrait::Type& s_type,
    ContextUpdateType update_type,
    int new_table_id) {
  if (!expr) {
    return nullptr;
  }
  if (auto func_oper_expr = std::dynamic_pointer_cast<Analyzer::FunctionOper>(expr)) {
    std::vector<std::shared_ptr<Analyzer::Expr>> args;
    for (size_t i = 0; i < func_oper_expr->getArity(); i++) {
      args.push_back(updateAnalyzerExpr(func_oper_expr->getOwnArg(i),
                                        pre_index,
                                        cur_index,
                                        table_id,
                                        rte_idx,
                                        cur_expr,
                                        s_type,
                                        update_type,
                                        new_table_id));
    }

    return std::make_shared<Analyzer::FunctionOper>(
        func_oper_expr->get_type_info(), func_oper_expr->getName(), args);
  }

  if (auto bin_oper_expr = std::dynamic_pointer_cast<Analyzer::BinOper>(expr)) {
    return std::make_shared<Analyzer::BinOper>(
        bin_oper_expr->get_type_info(),
        false,
        bin_oper_expr->get_optype(),
        bin_oper_expr->get_qualifier(),
        updateAnalyzerExpr(bin_oper_expr->get_own_left_operand(),
                           pre_index,
                           cur_index,
                           table_id,
                           rte_idx,
                           cur_expr,
                           s_type,
                           update_type,
                           new_table_id),
        updateAnalyzerExpr(bin_oper_expr->get_own_right_operand(),
                           pre_index,
                           cur_index,
                           table_id,
                           rte_idx,
                           cur_expr,
                           s_type,
                           update_type,
                           new_table_id));
  }
  // FIXME: pay attention that UOper has different constructor
  if (auto u_oper_expr = std::dynamic_pointer_cast<Analyzer::UOper>(expr)) {
    return std::make_shared<Analyzer::UOper>(
        u_oper_expr->get_type_info(),
        u_oper_expr->get_contains_agg(),
        u_oper_expr->get_optype(),
        updateAnalyzerExpr(u_oper_expr->get_own_operand(),
                           pre_index,
                           cur_index,
                           table_id,
                           rte_idx,
                           cur_expr,
                           s_type,
                           update_type,
                           new_table_id));
  }
  if (auto agg_expr = std::dynamic_pointer_cast<Analyzer::AggExpr>(expr)) {
    if (!agg_expr->get_arg()) {
      return agg_expr;
    }
    return std::make_shared<Analyzer::AggExpr>(agg_expr->get_type_info(),
                                               agg_expr->get_aggtype(),
                                               updateAnalyzerExpr(agg_expr->get_own_arg(),
                                                                  pre_index,
                                                                  cur_index,
                                                                  table_id,
                                                                  rte_idx,
                                                                  cur_expr,
                                                                  s_type,
                                                                  update_type,
                                                                  new_table_id),
                                               agg_expr->get_is_distinct(),
                                               agg_expr->get_arg1());
  }
  // Var need put ahead of ColumnVar which extends from
  if (auto var_expr = std::dynamic_pointer_cast<Analyzer::Var>(expr)) {
    // We only update the exprs with table_id matched
    if (var_expr->get_table_id() != table_id) {
      return var_expr;
    }
    switch (update_type) {
      case UpdateOnlyIndex: {
        // update it when index match and it's not updated yet
        if (var_expr->get_column_id() == pre_index && var_expr->get_table_id() != 0 &&
            getExprUpdatable(cols_update_stat_, var_expr)) {
          // If this expr doesn't exist in this map, insert it
          if (cols_update_stat_.find(var_expr) == cols_update_stat_.end()) {
            cols_update_stat_.insert(std::pair(var_expr, true));
          }
          // make new Analyzer::Var and set it updated to avoid duplicate update
          return makeVar(var_expr->get_type_info(),
                         var_expr->get_table_id(),
                         cur_index,
                         var_expr->get_rte_idx(),
                         false,  // FIXME
                         Analyzer::Var::kGROUPBY,
                         var_expr->get_varno(),
                         true);
        }
        break;
      }
      case UpdateToExpr: {
        if (var_expr->get_column_id() == pre_index && var_expr->get_table_id() != 0 &&
            getExprUpdatable(cols_update_stat_, var_expr)) {
          // if Var is not mapped to column of origin table, set the default properties
          // ref var_ref() in Analyzer.h
          return makeVar(cur_expr->get_type_info(),
                         0,
                         0,
                         -1,
                         false,  // FIXME
                         Analyzer::Var::kGROUPBY,
                         var_expr->get_varno(),
                         true);
        }
        break;
      }
      case UpdateColDesc: {
        // update it when index match and it's not updated yet
        if (var_expr->get_column_id() == pre_index && var_expr->get_table_id() != 0 &&
            getExprUpdatable(cols_update_stat_, var_expr)) {
          // If this expr doesn't exist in this map, insert it
          if (cols_update_stat_.find(var_expr) == cols_update_stat_.end()) {
            cols_update_stat_.insert(std::pair(var_expr, true));
          }
          return makeVar(var_expr->get_type_info(),
                         new_table_id,
                         cur_index,
                         rte_idx,
                         false,  // FIXME
                         Analyzer::Var::kGROUPBY,
                         var_expr->get_varno(),
                         true);
        }
        break;
      }
      case UpdateOnlyType: {
        if (var_expr->get_column_id() == pre_index) {
          // Var is mapped to original table column, set the correct column info
          return std::make_shared<Analyzer::Var>(getSQLTypeInfo(s_type),
                                                 var_expr->get_table_id(),
                                                 var_expr->get_column_id(),
                                                 var_expr->get_rte_idx(),
                                                 false,  // FIXME
                                                 Analyzer::Var::kGROUPBY,
                                                 var_expr->get_varno());
        }
        break;
      }
      default:
        CIDER_THROW(CiderCompileException, "unsupport update type on expression IR.");
    }
    return var_expr;
  }
  if (auto column_var_expr = std::dynamic_pointer_cast<Analyzer::ColumnVar>(expr)) {
    // We only update the exprs with table_id matched
    if (column_var_expr->get_table_id() != table_id) {
      return column_var_expr;
    }
    switch (update_type) {
      case UpdateOnlyIndex: {
        if (column_var_expr->get_column_id() == pre_index &&
            getExprUpdatable(cols_update_stat_, column_var_expr)) {
          if (cols_update_stat_.find(column_var_expr) == cols_update_stat_.end()) {
            cols_update_stat_.insert(std::pair(column_var_expr, true));
          }
          return makeColumnVar(column_var_expr->get_type_info(),
                               column_var_expr->get_table_id(),
                               cur_index,
                               column_var_expr->get_rte_idx(),
                               true);
        }
        break;
      }
      case UpdateToExpr: {
        if (column_var_expr->get_column_id() == pre_index &&
            getExprUpdatable(cols_update_stat_, column_var_expr)) {
          return cur_expr;
        }
        break;
      }
      case UpdateColDesc: {
        // update it when index match and it's not updated yet
        if (column_var_expr->get_column_id() == pre_index &&
            getExprUpdatable(cols_update_stat_, column_var_expr)) {
          if (cols_update_stat_.find(column_var_expr) == cols_update_stat_.end()) {
            cols_update_stat_.insert(std::pair(column_var_expr, true));
          }
          return makeColumnVar(
              column_var_expr->get_type_info(), new_table_id, cur_index, rte_idx, true);
        }
        break;
      }
      case UpdateOnlyType: {
        if (column_var_expr->get_column_id() == pre_index) {
          return std::make_shared<Analyzer::ColumnVar>(getSQLTypeInfo(s_type),
                                                       column_var_expr->get_table_id(),
                                                       column_var_expr->get_column_id(),
                                                       column_var_expr->get_rte_idx());
        }
        break;
      }
      default:
        CIDER_THROW(CiderCompileException, "unsupport update type on expression IR.");
    }
    return column_var_expr;
  }
  if (std::dynamic_pointer_cast<Analyzer::Constant>(expr)) {
    return expr;
  }
  if (auto date_add_expr = std::dynamic_pointer_cast<Analyzer::DateaddExpr>(expr)) {
    return std::make_shared<Analyzer::DateaddExpr>(
        date_add_expr->get_type_info(),
        date_add_expr->get_field(),
        updateAnalyzerExpr(date_add_expr->get_number(),
                           pre_index,
                           cur_index,
                           table_id,
                           rte_idx,
                           cur_expr,
                           s_type,
                           update_type,
                           new_table_id),
        updateAnalyzerExpr(date_add_expr->get_datetime(),
                           pre_index,
                           cur_index,
                           table_id,
                           rte_idx,
                           cur_expr,
                           s_type,
                           update_type,
                           new_table_id));
  }
  if (auto in_expr = std::dynamic_pointer_cast<Analyzer::InValues>(expr)) {
    return makeExpr<Analyzer::InValues>(updateAnalyzerExpr(in_expr->get_own_arg(),
                                                           pre_index,
                                                           cur_index,
                                                           table_id,
                                                           rte_idx,
                                                           cur_expr,
                                                           s_type,
                                                           update_type,
                                                           new_table_id),
                                        in_expr->get_value_list());
  }
  if (auto case_expr = std::dynamic_pointer_cast<Analyzer::CaseExpr>(expr)) {
    std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
        new_expr_pair_list;
    auto expr_pair_list = case_expr->get_expr_pair_list();
    for (auto pair_i : expr_pair_list) {
      auto if_expr = updateAnalyzerExpr(pair_i.first,
                                        pre_index,
                                        cur_index,
                                        table_id,
                                        rte_idx,
                                        cur_expr,
                                        s_type,
                                        update_type,
                                        new_table_id);
      auto then_expr = updateAnalyzerExpr(pair_i.second,
                                          pre_index,
                                          cur_index,
                                          table_id,
                                          rte_idx,
                                          cur_expr,
                                          s_type,
                                          update_type,
                                          new_table_id);
      new_expr_pair_list.emplace_back(if_expr, then_expr);
    }
    auto else_expr = updateAnalyzerExpr(case_expr->get_else_ref(),
                                        pre_index,
                                        cur_index,
                                        table_id,
                                        rte_idx,
                                        cur_expr,
                                        s_type,
                                        update_type,
                                        new_table_id);

    return Parser::CaseExpr::normalize(new_expr_pair_list, else_expr);
  }
  if (auto extract_expr = std::dynamic_pointer_cast<Analyzer::ExtractExpr>(expr)) {
    auto from_expr = updateAnalyzerExpr(extract_expr->get_own_from_expr(),
                                        pre_index,
                                        cur_index,
                                        table_id,
                                        rte_idx,
                                        cur_expr,
                                        s_type,
                                        update_type,
                                        new_table_id);
    auto time_unit = extract_expr->get_field();
    return ExtractExpr::generate(from_expr, time_unit);
  }
  if (auto like_expr = std::dynamic_pointer_cast<Analyzer::LikeExpr>(expr)) {
    return makeExpr<Analyzer::LikeExpr>(updateAnalyzerExpr(like_expr->get_shared_arg(),
                                                           pre_index,
                                                           cur_index,
                                                           table_id,
                                                           rte_idx,
                                                           cur_expr,
                                                           s_type,
                                                           update_type,
                                                           new_table_id),
                                        updateAnalyzerExpr(like_expr->get_shared_Like(),
                                                           pre_index,
                                                           cur_index,
                                                           table_id,
                                                           rte_idx,
                                                           cur_expr,
                                                           s_type,
                                                           update_type,
                                                           new_table_id),
                                        updateAnalyzerExpr(like_expr->get_shared_escape(),
                                                           pre_index,
                                                           cur_index,
                                                           table_id,
                                                           rte_idx,
                                                           cur_expr,
                                                           s_type,
                                                           update_type,
                                                           new_table_id),
                                        like_expr->get_is_ilike(),
                                        like_expr->get_is_simple());
  }
  if (auto string_expr = std::dynamic_pointer_cast<Analyzer::StringOper>(expr)) {
    std::vector<std::shared_ptr<Analyzer::Expr>> args;
    for (int i = 0; i < string_expr->getArity(); i++) {
      args.push_back(updateAnalyzerExpr(string_expr->getOwnArg(i),
                                        pre_index,
                                        cur_index,
                                        table_id,
                                        rte_idx,
                                        cur_expr,
                                        s_type,
                                        update_type,
                                        new_table_id));
    }
    return makeExpr<Analyzer::StringOper>(string_expr->get_kind(), args);
  }
  CIDER_THROW(CiderCompileException, "Failed to update expr.");
}

std::shared_ptr<Analyzer::AggExpr>
Substrait2AnalyzerExprConverter::updateOutputTypeOfAVGPartial(
    std::shared_ptr<Analyzer::Expr> expr,
    const substrait::Type& s_type) {
  auto agg_expr = std::dynamic_pointer_cast<Analyzer::AggExpr>(expr);
  if (!agg_expr) {
    CIDER_THROW(CiderCompileException,
                "output type update only happens in partial AVG case.");
  }
  return std::make_shared<Analyzer::AggExpr>(getSQLTypeInfo(s_type),
                                             agg_expr->get_aggtype(),
                                             agg_expr->get_own_arg(),
                                             false,
                                             agg_expr->get_arg1());
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression_Literal& s_literal_expr) {
  Datum constant_value;
  switch (s_literal_expr.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      auto ti = getSQLTypeInfo(s_literal_expr);
      std::string byteString = s_literal_expr.decimal().value();
      // A byte array of size 16 represents 128 bits(decimal value)
      std::byte byteArray[byteString.length()];
      std::memcpy(byteArray, byteString.data(), byteString.length());
      int64_t* decimalValue = reinterpret_cast<int64_t*>(byteArray);
      // ptr[0] stores lower 64 bits decimal value
      // TODO: support 128 bits decimal (ptr[1] stores higher 64 bits decimal value)
      constant_value.bigintval = decimalValue[0];
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      constant_value.boolval = s_literal_expr.boolean();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI64: {
      constant_value.bigintval = s_literal_expr.i64();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI32: {
      constant_value.intval = s_literal_expr.i32();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI16: {
      constant_value.smallintval = s_literal_expr.i16();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kI8: {
      constant_value.tinyintval = s_literal_expr.i8();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFp32: {
      constant_value.floatval = s_literal_expr.fp32();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      constant_value.doubleval = s_literal_expr.fp64();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kDate: {
      constant_value.intval = s_literal_expr.date();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kTimestamp: {
      constant_value.bigintval = s_literal_expr.timestamp();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kTime: {
      constant_value.bigintval = s_literal_expr.time();
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kFixedChar: {
      constant_value.stringval = new std::string(s_literal_expr.fixed_char());
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kVarChar: {
      constant_value.stringval = new std::string(s_literal_expr.var_char().value());
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kString: {
      constant_value.stringval = new std::string(s_literal_expr.string());
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth: {
      constant_value.bigintval = s_literal_expr.date() * kSecsPerDay;
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond: {
      // TODO: Please take care for velox intergration! divide 10 is due to isthmus
      //  generated plan * 10, not sure about root cause yet.
      constant_value.bigintval =
          s_literal_expr.interval_day_to_second().days() / 10 * kSecsPerDay;
      return std::make_shared<Analyzer::Constant>(
          getSQLTypeInfo(s_literal_expr), false, constant_value);
    }
    case substrait::Expression_Literal::LiteralTypeCase::kNull: {
      // return bool type constant, set null value
      return std::make_shared<Analyzer::Constant>(SQLTypes::kBOOLEAN, true);
    }

    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported literal_type in Omnisci {}",
                              s_literal_expr.literal_type_case()));
  }
}
std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression_FieldReference& s_selection_expr,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  // for fieldreference translated here, will update with right type info and table id
  // direct_reference by expr_map which will cache the right type info and table id +
  // expression --> columnVar
  if (s_selection_expr.has_direct_reference() && s_selection_expr.has_expression()) {
    return toAnalyzerExpr(s_selection_expr.expression(), function_map, expr_map_ptr);
  }
  // direct_reference + root_reference --> ColumnVar
  if (isColumnVar(s_selection_expr)) {
    // before here we have got type and cached into expr_map from root node which have the
    // type info, here we use expr_map to get the right type info
    int col_id = s_selection_expr.direct_reference().struct_field().field();
    SQLTypes t = SQLTypes::kDOUBLE;
    int cur_table_id = cur_table_id_;
    int nest_level = cur_table_id_ - fake_table_id;
    if (expr_map_ptr != nullptr) {
      auto iter = expr_map_ptr->find(col_id);
      if (iter != expr_map_ptr->end()) {
        return iter->second;
      } else {
        CIDER_THROW(CiderCompileException, "Failed to get field reference expr.");
      }
    }
    return makeColumnVar(SQLTypeInfo(t, false), cur_table_id, col_id, nest_level, true);
  }
  CIDER_THROW(CiderCompileException, "Failed to translate field reference expr.");
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildInValuesExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  // For expression like "substring(col_str, 0, 3) in ('0000', '1111', '2222')",
  // it will be parsed into substrait SingleOrList with semantics like
  // col_str in (cast '0000':fixedcahr as varchar, ...)
  if (s_scalar_function.arguments_size() > 2) {
    std::list<std::shared_ptr<Analyzer::Expr>> args;
    for (int i = 1; i < s_scalar_function.arguments_size(); i++) {
      args.push_back(toAnalyzerExpr(
          s_scalar_function.arguments(i).value(), function_map, expr_map_ptr));
    }
    return makeExpr<Analyzer::InValues>(
        toAnalyzerExpr(
            s_scalar_function.arguments(0).value(), function_map, expr_map_ptr),
        args);
  }
  if (s_scalar_function.arguments_size() == 2 &&
      s_scalar_function.arguments(1).value().literal().has_list()) {
    return makeExpr<Analyzer::InValues>(
        toAnalyzerExpr(
            s_scalar_function.arguments(0).value(), function_map, expr_map_ptr),
        toAnalyzerExprList(s_scalar_function.arguments(1).value().literal()));
  }
  // TODO: (yma11) need support "in cast(vector)" expression for velox integration
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildExtractExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    const ExtractField& extract_field,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  if (extract_field == ExtractField::kNONE) {
    CHECK(s_scalar_function.arguments_size() == 2);
    CHECK(s_scalar_function.arguments(1).has_value());
    auto from_expr = toAnalyzerExpr(
        s_scalar_function.arguments(1).value(), function_map, expr_map_ptr);
    CHECK(s_scalar_function.arguments(0).has_enum_());
    std::string time_unit = s_scalar_function.arguments(0).enum_().specified();
    CHECK(time_unit != "");
    return ExtractExpr::generate(from_expr, time_unit);
  } else {
    CHECK(s_scalar_function.arguments_size() == 1);
    auto from_expr = toAnalyzerExpr(
        s_scalar_function.arguments(0).value(), function_map, expr_map_ptr);
    return ExtractExpr::generate(from_expr, extract_field);
  }
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildNotNullExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  CHECK(s_scalar_function.arguments_size() == 1);
  CHECK(s_scalar_function.arguments(0).has_value());
  const std::shared_ptr<Analyzer::Expr> operand_expr =
      toAnalyzerExpr(s_scalar_function.arguments(0).value(), function_map, expr_map_ptr);
  auto is_null = std::make_shared<Analyzer::UOper>(kBOOLEAN, kISNULL, operand_expr);
  return std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, is_null);
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildTimeAddExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::string& function_name,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  CHECK(function_name == "add" || function_name == "subtract");
  const std::shared_ptr<Analyzer::Expr> datetime =
      toAnalyzerExpr(s_scalar_function.arguments(0).value(), function_map, expr_map_ptr);
  SQLTypeInfo datetime_ti = datetime->get_type_info();
  if (s_scalar_function.arguments(1).value().literal().has_interval_year_to_month()) {
    Datum interval_value;
    SQLTypeInfo interval_ti(SQLTypes::kINTERVAL_YEAR_MONTH, true);
    interval_value.bigintval = s_scalar_function.arguments(1)
                                       .value()
                                       .literal()
                                       .interval_year_to_month()
                                       .years() *
                                   12 +
                               s_scalar_function.arguments(1)
                                   .value()
                                   .literal()
                                   .interval_year_to_month()
                                   .months();
    std::shared_ptr<Analyzer::Expr> interval_months =
        std::make_shared<Analyzer::Constant>(interval_ti, false, interval_value);
    auto bigint_ti = SQLTypeInfo(kBIGINT, false);
    auto interval_expr = function_name == "add"
                             ? interval_months
                             : std::make_shared<Analyzer::UOper>(
                                   bigint_ti, false, SQLOps::kUMINUS, interval_months);
    return makeExpr<Analyzer::DateaddExpr>(datetime_ti, daMONTH, interval_expr, datetime);
  } else if (s_scalar_function.arguments(1)
                 .value()
                 .literal()
                 .has_interval_day_to_second()) {
    Datum interval_value;
    // divide 10 is due to isthmus generated plan * 10, not sure about root cause yet.
    interval_value.bigintval =
        s_scalar_function.arguments(1).value().literal().interval_day_to_second().days() *
            kSecsPerDay / 10 +
        s_scalar_function.arguments(1)
            .value()
            .literal()
            .interval_day_to_second()
            .seconds();
    SQLTypeInfo interval_ti(SQLTypes::kINTERVAL_DAY_TIME, true);
    std::shared_ptr<Analyzer::Expr> interval_secs =
        std::make_shared<Analyzer::Constant>(interval_ti, false, interval_value);
    auto bigint_ti = SQLTypeInfo(kBIGINT, false);

    auto interval_expr = function_name == "add"
                             ? interval_secs
                             : std::make_shared<Analyzer::UOper>(
                                   bigint_ti, false, SQLOps::kUMINUS, interval_secs);

    return makeExpr<Analyzer::DateaddExpr>(
        datetime_ti, daSECOND, interval_expr, datetime);
  }
  CIDER_THROW(CiderCompileException, "Unsupported date time function: " + function_name);
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildLikeExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  std::shared_ptr<Analyzer::Expr> arg =
      toAnalyzerExpr(s_scalar_function.arguments(0).value(), function_map, expr_map_ptr);

  std::shared_ptr<Analyzer::Expr> like_literal_expr = nullptr;

  // FIXME: Isthmus will always gen LIKE_FUNC[(Left)COLUMN, (Right)CAST->LITERAL(STRING)]
  // plan node, we bypass cast currently.
  const ::substrait::Expression& value =
      s_scalar_function.arguments(1).value().has_cast()
          ? s_scalar_function.arguments(1).value().cast().input()
          : s_scalar_function.arguments(1).value();

  SQLTypeInfo info(SQLTypes::kCHAR, true);
  Datum v;
  if (value.literal().has_fixed_char()) {
    v.stringval = new std::string(value.literal().fixed_char());
  } else if (value.literal().has_var_char()) {
    v.stringval = new std::string(value.literal().var_char().value());
  } else {
    CIDER_THROW(CiderCompileException, "not supported type for like expr.");
  }
  like_literal_expr = std::make_shared<Analyzer::Constant>(info, false, v);

  std::shared_ptr<Analyzer::Expr> escape_expr = nullptr;  // TODO: escape support
  std::shared_ptr<Analyzer::LikeExpr> likeExpr = std::make_shared<Analyzer::LikeExpr>(
      arg, like_literal_expr, escape_expr, false, false);
  return likeExpr;
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildStrExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  std::vector<std::shared_ptr<Analyzer::Expr>> args;
  for (int i = 0; i < s_scalar_function.arguments_size(); i++) {
    auto value = s_scalar_function.arguments(i).value();
    args.push_back(toAnalyzerExpr(value, function_map, expr_map_ptr));
  }
  auto function_name =
      getFunctionName(function_map, s_scalar_function.function_reference());
  std::transform(
      function_name.begin(), function_name.end(), function_name.begin(), ::toupper);
  auto string_op_kind = name_to_string_op_kind(function_name);
  switch (string_op_kind) {
    case SqlStringOpKind::LOWER:
      return makeExpr<Analyzer::LowerStringOper>(args);
    case SqlStringOpKind::UPPER:
      return makeExpr<Analyzer::UpperStringOper>(args);
      //    case SqlStringOpKind::INITCAP:
      //      return makeExpr<Analyzer::InitCapStringOper>(args);
      //    case SqlStringOpKind::REVERSE:
      //      return makeExpr<Analyzer::ReverseStringOper>(args);
      //    case SqlStringOpKind::REPEAT:
      //      return makeExpr<Analyzer::RepeatStringOper>(args);
         case SqlStringOpKind::CONCAT:
           return makeExpr<Analyzer::ConcatStringOper>(args);
      //    case SqlStringOpKind::LPAD:
      //    case SqlStringOpKind::RPAD: {
      //      return makeExpr<Analyzer::PadStringOper>(string_op_kind, args);
      //    }
    case SqlStringOpKind::TRIM:
    case SqlStringOpKind::LTRIM:
    case SqlStringOpKind::RTRIM: {
      return makeExpr<Analyzer::TrimStringOper>(string_op_kind, args);
    }
    case SqlStringOpKind::SUBSTRING: {
      return makeExpr<Analyzer::SubstringStringOper>(args);
    }
      //    case SqlStringOpKind::OVERLAY:
      //      return makeExpr<Analyzer::OverlayStringOper>(args);
      //    case SqlStringOpKind::REPLACE:
      //      return makeExpr<Analyzer::ReplaceStringOper>(args);
      //    case SqlStringOpKind::SPLIT_PART:
      //      return makeExpr<Analyzer::SplitPartStringOper>(args);
      //    case SqlStringOpKind::REGEXP_REPLACE:
      //      return makeExpr<Analyzer::RegexpReplaceStringOper>(args);
      //    case SqlStringOpKind::REGEXP_SUBSTR:
      //      return makeExpr<Analyzer::RegexpSubstrStringOper>(args);
      //    case SqlStringOpKind::JSON_VALUE:
      //      return makeExpr<Analyzer::JsonValueStringOper>(args);
      //    case SqlStringOpKind::BASE64_ENCODE:
      //      return makeExpr<Analyzer::Base64EncodeStringOper>(args);
      //    case SqlStringOpKind::BASE64_DECODE:
      //      return makeExpr<Analyzer::Base64DecodeStringOper>(args);
    default:
      CIDER_THROW(CiderCompileException, "Unsupported string function.");
  }
  return nullptr;
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::buildCoalesceExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  CHECK(s_scalar_function.arguments_size() > 0);
  std::shared_ptr<Analyzer::Expr> else_expr;
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      expr_list;
  for (int i = 0; i < s_scalar_function.arguments_size(); i++) {
    CHECK(s_scalar_function.arguments(i).has_value());
    const auto value_expr = toAnalyzerExpr(
        s_scalar_function.arguments(i).value(), function_map, expr_map_ptr);
    auto is_null = std::make_shared<Analyzer::UOper>(kBOOLEAN, kISNULL, value_expr);
    auto if_expr = std::make_shared<Analyzer::UOper>(kBOOLEAN, kNOT, is_null);

    auto then_expr = toAnalyzerExpr(
        s_scalar_function.arguments(i).value(), function_map, expr_map_ptr);

    expr_list.emplace_back(if_expr, then_expr);
  }
  else_expr = std::make_shared<Analyzer::Constant>(SQLTypes::kBOOLEAN, true);
  return Parser::CaseExpr::normalize(expr_list, else_expr);
}

bool isNotPrimitiveType(const SQLTypeInfo& type) {
  return type.is_time() || type.is_string();
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression_ScalarFunction& s_scalar_function,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  auto function = getFunctionName(function_map, s_scalar_function.function_reference());
  // If it's an InValues expr, like "in (1, 2, 3)" or "in cast(vector)", build
  // Analyzer::InValues and return
  if (function == "in") {
    return buildInValuesExpr(s_scalar_function, function_map, expr_map_ptr);
  }
  if (auto field = ExtractExpr::try_map_presto_extract_function(function);
      field != ExtractField::kNONE || function == "extract") {
    return buildExtractExpr(s_scalar_function, function_map, field, expr_map_ptr);
  }
  if (function == "is_not_null") {
    return buildNotNullExpr(s_scalar_function, function_map, expr_map_ptr);
  }
  if (function == "coalesce") {
    return buildCoalesceExpr(s_scalar_function, function_map, expr_map_ptr);
  }
  if (isStringFunction(function)) {
    return buildStrExpr(s_scalar_function, function_map, expr_map_ptr);
  }
  std::vector<std::shared_ptr<Analyzer::Expr>> args;
  for (size_t i = 0; i < s_scalar_function.arguments_size(); i++) {
    args.push_back(toAnalyzerExpr(
        s_scalar_function.arguments(i).value(), function_map, expr_map_ptr));
  }
  // For "between_and_" function, cider defined type will go through this rewrite_expr
  // branch while primitive type will go through ExtensionFunction branch below.
  if (function == "between" && s_scalar_function.arguments_size() == 3 &&
      isNotPrimitiveType(args[0]->get_type_info())) {
    auto ge_oper = std::make_shared<Analyzer::BinOper>(
        getSQLTypeInfo(s_scalar_function.output_type()),
        false,
        SQLOps::kGE,
        SQLQualifier::kONE,
        args[0],
        args[1]);
    auto le_oper = std::make_shared<Analyzer::BinOper>(
        getSQLTypeInfo(s_scalar_function.output_type()),
        false,
        SQLOps::kLE,
        SQLQualifier::kONE,
        args[0],
        args[2]);

    auto bwt_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(
        Parser::OperExpr::normalize(kAND, kONE, ge_oper, le_oper));

    return bwt_oper;
  }

  if (isExtensionFunction(function, args)) {
    return std::make_shared<Analyzer::FunctionOper>(
        getSQLTypeInfo(s_scalar_function.output_type()), function, args);
  }

  // translate Date type function (DateAdd/DateSubtract)
  if (s_scalar_function.has_output_type() &&
      isTimeType(s_scalar_function.output_type())) {
    return buildTimeAddExpr(s_scalar_function, function, function_map, expr_map_ptr);
  }

  if (function == "like") {
    return buildLikeExpr(s_scalar_function, function_map, expr_map_ptr);
  }

  // translate UOper
  if (s_scalar_function.arguments_size() == 1) {
    return std::make_shared<Analyzer::UOper>(
        getSQLTypeInfo(s_scalar_function.output_type()),
        false,
        getCiderSqlOps(function),
        args[0]);
  }

  // translate BinOp
  if (s_scalar_function.arguments_size() == 2) {
    SQLQualifier qualifier = SQLQualifier::kONE;
    return std::make_shared<Analyzer::BinOper>(
        getSQLTypeInfo(s_scalar_function.output_type()),
        false,
        getCiderSqlOps(function),
        qualifier,
        args[0],
        args[1]);
  }
  // for cases like A or B or C, A and B and C
  if (s_scalar_function.arguments_size() > 2) {
    // TODO: may need to consider other properties of scalar_function
    substrait::Expression seperated_expression;
    // FIXME: what about not scalar_function case
    substrait::Expression_ScalarFunction* scalar_function =
        seperated_expression.mutable_scalar_function();
    for (int i = 1; i < s_scalar_function.arguments_size(); i++) {
      substrait::FunctionArgument* argument = scalar_function->add_arguments();
      argument->CopyFrom(s_scalar_function.arguments(i));
    }
    substrait::Type* output_type = scalar_function->mutable_output_type();
    output_type->CopyFrom(s_scalar_function.output_type());
    scalar_function->set_function_reference(s_scalar_function.function_reference());
    SQLQualifier qualifier = SQLQualifier::kONE;
    return std::make_shared<Analyzer::BinOper>(
        getSQLTypeInfo(s_scalar_function.output_type()),
        false,
        getCiderSqlOps(function),
        qualifier,
        args[0],
        toAnalyzerExpr(seperated_expression, function_map, expr_map_ptr));
  }
  CIDER_THROW(CiderCompileException, "Failed to transfer scalar function expr.");
}

int64_t dateToInt64(const std::string& string_val) {
  DateTimeParser parser;
  parser.setFormatType(DateTimeParser::FormatType::Date);
  auto res = parser.parse(string_val, 0);
  if (res.has_value()) {
    return res.value();
  }
  CIDER_THROW(CiderCompileException, "Not a valid date string!");
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression_Cast& s_cast_expr,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  // TODO: (spevenhe) this branch should only cover cast_string_to_date case.
  // encapsulate and support nested function like cast(cast(xx as String) AS Date).
  // CAST(SUBSTRING(col_string, 0, 5) AS DATE
  // Now only supports cast(col_string AS Date)
  if (s_cast_expr.type().kind_case() == substrait::Type::kDate) {
    SQLTypeInfo sqlTypeInfo = getSQLTypeInfo(s_cast_expr.type());
    if (!s_cast_expr.input().literal().fixed_char().empty()) {
      // Default is none encoding
      // sqlTypeInfo.set_compression(EncodingType::kENCODING_NONE);
      Datum v;
      v.intval = parseDateInDays(s_cast_expr.input().literal().fixed_char());
      return std::make_shared<Analyzer::Constant>(sqlTypeInfo, false, v);
    } else if (isColumnVar(s_cast_expr.input().selection())) {
      int col_id =
          s_cast_expr.input().selection().direct_reference().struct_field().field();
      if (expr_map_ptr != nullptr) {
        auto iter = expr_map_ptr->find(col_id);
        if (iter != expr_map_ptr->end()) {
          auto type = iter->second->get_type_info().get_type();
          if (type == SQLTypes::kTEXT || type == SQLTypes::kVARCHAR) {
            std::vector<std::shared_ptr<Analyzer::Expr>> args;
            args.push_back(toAnalyzerExpr(
                s_cast_expr.input().selection(), function_map, expr_map_ptr));

            return makeExpr<Analyzer::TryStringCastOper>(SQLTypes::kDATE, args);
          } else if (sqlTypeInfo.is_time()) {
            return std::make_shared<Analyzer::UOper>(
                sqlTypeInfo,
                false,
                SQLOps::kCAST,
                toAnalyzerExpr(s_cast_expr.input(), function_map, expr_map_ptr));
          } else {
            CIDER_THROW(CiderCompileException,
                        "Not supported date cast type other than string.");
          }
        } else {
          CIDER_THROW(CiderCompileException, "Failed to get field reference expr.");
        }
      }
      CIDER_THROW(CiderCompileException, "Can not get the origin type of CAST to DATE.");
    }
  }
  // CAST is a normal UOper expr in Analyzer
  return std::make_shared<Analyzer::UOper>(
      getSQLTypeInfo(s_cast_expr.type()),
      false,
      SQLOps::kCAST,
      toAnalyzerExpr(s_cast_expr.input(), function_map, expr_map_ptr));
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::AggregateFunction& s_expr,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  auto function = getFunctionName(function_map, s_expr.function_reference());
  SQLAgg agg_kind = getCiderAggOp(function);
  std::shared_ptr<Analyzer::Constant> arg1;  // 2nd aggregate parameter
  std::shared_ptr<Analyzer::Expr> arg_expr;
  // Aggregate functions like count(*)/count(1) have no arguments, thus arg_expr is
  // nullptr
  if (s_expr.arguments_size() == 1) {
    arg_expr = toAnalyzerExpr(s_expr.arguments(0).value(), function_map, expr_map_ptr);
  }
  if (s_expr.has_output_type()) {
    auto agg_type = getSQLTypeInfo(s_expr.output_type());
    return std::make_shared<Analyzer::AggExpr>(
        agg_type,
        agg_kind,
        arg_expr,
        s_expr.invocation() ==
                ::substrait::AggregateFunction_AggregationInvocation::
                    AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT
            ? true
            : false,
        arg1);
  } else {
    CIDER_THROW(CiderCompileException,
                "Cannot find output type for function: " + function);
  }
}

std::shared_ptr<Analyzer::Expr> Substrait2AnalyzerExprConverter::toAnalyzerExpr(
    const substrait::Expression_IfThen& s_if_then_expr,
    const std::unordered_map<int, std::string> function_map,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        expr_map_ptr) {
  std::shared_ptr<Analyzer::Expr> else_expr;
  std::list<std::pair<std::shared_ptr<Analyzer::Expr>, std::shared_ptr<Analyzer::Expr>>>
      expr_list;
  for (size_t i = 0; i < s_if_then_expr.ifs_size(); ++i) {
    const ::substrait::Expression_IfThen_IfClause& ifThen = s_if_then_expr.ifs(i);
    if (ifThen.has_if_() && ifThen.has_then()) {
      auto if_expr = toAnalyzerExpr(ifThen.if_(), function_map, expr_map_ptr);
      auto then_expr = toAnalyzerExpr(ifThen.then(), function_map, expr_map_ptr);

      expr_list.emplace_back(if_expr, then_expr);
    } else {
      CIDER_THROW(CiderCompileException, "case when is incompleted: lack case or when");
    }
  }
  if (s_if_then_expr.has_else_()) {
    else_expr = toAnalyzerExpr(s_if_then_expr.else_(), function_map, expr_map_ptr);
  }

  return Parser::CaseExpr::normalize(expr_list, else_expr);
}

bool Substrait2AnalyzerExprConverter::isColumnVar(
    const substrait::Expression_FieldReference& selection_expr) {
  if (selection_expr.has_direct_reference() && selection_expr.has_root_reference()) {
    return true;
  }
  return false;
}

bool Substrait2AnalyzerExprConverter::isTimeType(const substrait::Type& type) {
  if (type.has_date() || type.has_time() || type.has_timestamp() ||
      type.has_timestamp_tz()) {
    return true;
  }
  return false;
}

}  // namespace generator
