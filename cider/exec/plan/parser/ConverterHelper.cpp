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
 * @file    ConverterHelper.cpp
 * @brief   Provide utils methods for Substrait plan Translation
 **/

#include "ConverterHelper.h"
#include "TypeUtils.h"
#include "cider/CiderTypes.h"
#include "function/ExtensionFunctionsBinding.h"

namespace generator {
void registerExtensionFunctions() {
  // TODO: Adds modules for collecting all supported extension functions
  const std::string json_func_sigs =
      "[{\"name\":\"Between\",\"ret\":\"bool\",\"args\":[\"double\",\"double\","
      "\"double\"]},{\"name\":\"Between__3\",\"ret\":\"bool\",\"args\":[\"i64\",\"i64\","
      "\"i64\"]}]";

  ExtensionFunctionsWhitelist::add(json_func_sigs);
}

std::unordered_map<int, std::string> getFunctionMap(const substrait::Plan& plan) {
  std::unordered_map<int, std::string> function_map;
  for (int i = 0; i < plan.extensions_size(); i++) {
    auto extension = plan.extensions(i);
    if (extension.has_extension_function()) {
      auto function = extension.extension_function().name();
      auto function_name = function.substr(0, function.find_first_of(':'));
      function_map.emplace(extension.extension_function().function_anchor(),
                           function_name);
    }
  }
  return function_map;
}

std::string getFunctionName(const std::unordered_map<int, std::string> function_map,
                            int function_reference) {
  auto function = function_map.find(function_reference);
  if (function == function_map.end()) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format("Failed to find function with id in map: {}", function_reference));
  }
  return function->second;
}

SQLTypeInfo getSQLTypeInfo(const substrait::Type& s_type) {
  bool is_nullable = TypeUtils::getIsNullable(s_type);
  bool not_null = !is_nullable;
  switch (s_type.kind_case()) {
    case substrait::Type::kBool:
      return SQLTypeInfo(SQLTypes::kBOOLEAN, not_null);
    case substrait::Type::kDecimal:
      return SQLTypeInfo(SQLTypes::kDECIMAL,
                         s_type.decimal().precision(),
                         s_type.decimal().scale(),
                         not_null);
    case substrait::Type::kFp32:
      return SQLTypeInfo(SQLTypes::kFLOAT, not_null);
    case substrait::Type::kFp64:
      return SQLTypeInfo(SQLTypes::kDOUBLE, not_null);
    case substrait::Type::kI8:
      return SQLTypeInfo(SQLTypes::kTINYINT, not_null);
    case substrait::Type::kI16:
      return SQLTypeInfo(SQLTypes::kSMALLINT, not_null);
    case substrait::Type::kI32:
      return SQLTypeInfo(SQLTypes::kINT, not_null);
    case substrait::Type::kI64:
      return SQLTypeInfo(SQLTypes::kBIGINT, not_null);
    case substrait::Type::kTimestamp:
      return SQLTypeInfo(SQLTypes::kTIMESTAMP,
                         DateAndTimeType::getTypeDimension(DateAndTimeType::Type::Timestamp),
                         0,
                         not_null);
    case substrait::Type::kVarchar:
      return SQLTypeInfo(SQLTypes::kVARCHAR, not_null);
    case substrait::Type::kFixedChar:
      // todo: parse length?
      return SQLTypeInfo(SQLTypes::kCHAR, not_null);
    case substrait::Type::kDate:
      return SQLTypeInfo(SQLTypes::kDATE,
                         DateAndTimeType::getTypeDimension(DateAndTimeType::Type::Date),
                         0,
                         not_null);
    case substrait::Type::kTime:
      return SQLTypeInfo(SQLTypes::kTIME,
                         DateAndTimeType::getTypeDimension(DateAndTimeType::Type::Time),
                         0,
                         not_null);
    // FIXME: struct type support, currently we return a faked value
    // for partial avg expr with correct type updated later
    case substrait::Type::kStruct:
      return SQLTypeInfo(SQLTypes::kDOUBLE, not_null);
    case substrait::Type::kString:
      return SQLTypeInfo(SQLTypes::kTEXT, not_null);
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported type {}", s_type.kind_case()));
  }
}

SQLTypeInfo getSQLTypeInfo(const substrait::Expression_Literal& s_literal_expr) {
  switch (s_literal_expr.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return SQLTypeInfo(SQLTypes::kBOOLEAN, true);
    case substrait::Expression_Literal::LiteralTypeCase::kDecimal:
      return SQLTypeInfo(kDECIMAL,
                         s_literal_expr.decimal().precision(),
                         s_literal_expr.decimal().scale(),
                         true);
    case substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return SQLTypeInfo(SQLTypes::kFLOAT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return SQLTypeInfo(SQLTypes::kDOUBLE, true);
    case substrait::Expression_Literal::LiteralTypeCase::kI8:
      return SQLTypeInfo(SQLTypes::kTINYINT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kI16:
      return SQLTypeInfo(SQLTypes::kSMALLINT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kI32:
      return SQLTypeInfo(SQLTypes::kINT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kI64:
      return SQLTypeInfo(SQLTypes::kBIGINT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kDate:
      return SQLTypeInfo(SQLTypes::kDATE, true);
    case substrait::Expression_Literal::LiteralTypeCase::kTime:
      return SQLTypeInfo(SQLTypes::kTIME, true);
    case substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
      return SQLTypeInfo(SQLTypes::kTIMESTAMP, true);
    case substrait::Expression_Literal::LiteralTypeCase::kFixedChar:
      return SQLTypeInfo(SQLTypes::kCHAR, true);
    case substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return SQLTypeInfo(SQLTypes::kVARCHAR, true);
    case substrait::Expression_Literal::LiteralTypeCase::kString:
      return SQLTypeInfo(SQLTypes::kTEXT, true);
    case substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth:
      return SQLTypeInfo(SQLTypes::kINTERVAL_YEAR_MONTH);
    case substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond:
      return SQLTypeInfo(SQLTypes::kINTERVAL_DAY_TIME);
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported type {}", s_literal_expr.literal_type_case()));
  }
}

// TODO: get output type for agg functions, but this may violate rules of velox?
SQLTypeInfo getCiderAggType(const SQLAgg agg_kind, const Analyzer::Expr* arg_expr) {
  bool g_bigint_count{false};
  switch (agg_kind) {
    case SQLAgg::kCOUNT:
      return SQLTypeInfo(g_bigint_count ? SQLTypes::kBIGINT : SQLTypes::kINT, false);
    case SQLAgg::kMIN:
    case SQLAgg::kMAX:
      return arg_expr->get_type_info();
    case SQLAgg::kSUM:
      return arg_expr->get_type_info().is_integer()
                 ? SQLTypeInfo(SQLTypes::kBIGINT, false)
                 : arg_expr->get_type_info();
    case SQLAgg::kAVG:
      return SQLTypeInfo(SQLTypes::kDOUBLE, false);
    default:
      CIDER_THROW(CiderCompileException, "unsupported agg.");
  }
  CHECK(false);
  return SQLTypeInfo();
}

substrait::Type getSubstraitType(const SQLTypeInfo& type_info) {
  substrait::Type s_type;
  substrait::Type_Nullability nullalbility =
      TypeUtils::getSubstraitTypeNullability(!type_info.get_notnull());
  switch (type_info.get_type()) {
    case SQLTypes::kBOOLEAN: {
      auto s_bool = new substrait::Type_Boolean();
      s_bool->set_nullability(nullalbility);
      s_type.set_allocated_bool_(s_bool);
      return s_type;
    }
    case SQLTypes::kDECIMAL: {
      auto s_decimal = new substrait::Type_Decimal();
      s_decimal->set_nullability(nullalbility);
      s_decimal->set_precision(type_info.get_precision());
      s_decimal->set_scale(type_info.get_scale());
      s_type.set_allocated_decimal(s_decimal);
      return s_type;
    }
    case SQLTypes::kTINYINT: {
      auto s_tinyint = new substrait::Type_I8();
      s_tinyint->set_nullability(nullalbility);
      s_type.set_allocated_i8(s_tinyint);
      return s_type;
    }
    case SQLTypes::kSMALLINT: {
      auto s_smallint = new substrait::Type_I16();
      s_smallint->set_nullability(nullalbility);
      s_type.set_allocated_i16(s_smallint);
      return s_type;
    }
    case SQLTypes::kINT: {
      auto s_int = new substrait::Type_I32();
      s_int->set_nullability(nullalbility);
      s_type.set_allocated_i32(s_int);
      return s_type;
    }
    case SQLTypes::kBIGINT: {
      auto s_bigint = new substrait::Type_I64();
      s_bigint->set_nullability(nullalbility);
      s_type.set_allocated_i64(s_bigint);
      return s_type;
    }
    case SQLTypes::kFLOAT: {
      auto s_float = new substrait::Type_FP32();
      s_float->set_nullability(nullalbility);
      s_type.set_allocated_fp32(s_float);
      return s_type;
    }
    case SQLTypes::kDOUBLE: {
      auto s_double = new substrait::Type_FP64();
      s_double->set_nullability(nullalbility);
      s_type.set_allocated_fp64(s_double);
      return s_type;
    }
    case SQLTypes::kDATE: {
      auto s_date = new substrait::Type_Date();
      s_date->set_nullability(nullalbility);
      s_type.set_allocated_date(s_date);
      return s_type;
    }
    case SQLTypes::kTIME: {
      auto s_time = new substrait::Type_Time();
      s_time->set_nullability(nullalbility);
      s_type.set_allocated_time(s_time);
      return s_type;
    }
    case SQLTypes::kTIMESTAMP: {
      auto s_timestamp = new substrait::Type_Timestamp();
      s_timestamp->set_nullability(nullalbility);
      s_type.set_allocated_timestamp(s_timestamp);
      return s_type;
    }
    case SQLTypes::kVARCHAR: {
      auto s_varchar = new substrait::Type_VarChar();
      s_varchar->set_nullability(nullalbility);
      s_type.set_allocated_varchar(s_varchar);
      return s_type;
    }
    case SQLTypes::kCHAR: {
      auto s_char = new substrait::Type_FixedChar();
      s_char->set_nullability(nullalbility);
      s_type.set_allocated_fixed_char(s_char);
      return s_type;
    }
    case SQLTypes::kTEXT: {
      auto s_string = new substrait::Type_String();
      s_string->set_nullability(nullalbility);
      s_type.set_allocated_string(s_string);
      return s_type;
    }
    default:
      CIDER_THROW(CiderCompileException,
                  "CiderTableSchema: fails to translate Cider type to Substrait type.");
  }
}

SQLOps getCiderSqlOps(const std::string op) {
  if (op == "lt") {
    return SQLOps::kLT;
  } else if (op == "and") {
    return SQLOps::kAND;
  } else if (op == "or") {
    return SQLOps::kOR;
  } else if (op == "not") {
    return SQLOps::kNOT;
  } else if (op == "gt") {
    return SQLOps::kGT;
  } else if (op == "eq" or op == "equal") {
    return SQLOps::kEQ;
  } else if (op == "neq" or op == "ne" or op == "not_equal") {
    return SQLOps::kNE;
  } else if (op == "gte") {
    return SQLOps::kGE;
  } else if (op == "lte") {
    return SQLOps::kLE;
  } else if (op == "multiply") {
    return SQLOps::kMULTIPLY;
  } else if (op == "divide") {
    return SQLOps::kDIVIDE;
  } else if (op == "plus" || op == "add") {
    return SQLOps::kPLUS;
  } else if (op == "subtract" || op == "minus") {
    return SQLOps::kMINUS;
  } else if (op == "modulus") {
    return SQLOps::kMODULO;
  } else if (op == "is_not_null") {
    return SQLOps::kISNOTNULL;
  } else if (op == "is_null") {
    return SQLOps::kISNULL;
  } else if (op == "is_not_distinct_from") {
    return SQLOps::kBW_EQ;
  } else if (op == "is_distinct_from") {
    return SQLOps::kBW_NE;
  } else {
    CIDER_THROW(CiderCompileException, op + " is not yet supported");
  }
}

SQLAgg getCiderAggOp(const std::string op) {
  if (op == "sum") {
    return SQLAgg::kSUM;
  } else if (op == "min") {
    return SQLAgg::kMIN;
  } else if (op == "max") {
    return SQLAgg::kMAX;
  } else if (op == "avg") {
    return SQLAgg::kAVG;
  } else if (op == "count") {
    return SQLAgg::kCOUNT;
  } else {
    CIDER_THROW(CiderCompileException, op + " is not yet supported");
  }
}

bool isExtensionFunction(const std::string& function,
                         std::vector<std::shared_ptr<Analyzer::Expr>> args) {
  try {
    // TODO: This part of the logic needs to be reconsidered.
    // 1. The logic should be simplified, such as checking CPU.
    // 2. To determine the function binding machanism: If using function `bind_function`,
    //    incomplete matches are allowed. For example, with between(i64, i64, i64) being
    //    registered, even if between(i32, i32, i32) is unregistered, data can be cast to
    //    fit between(i64, i64, i64). If complete matches are needed, the logic can be
    //    futher simplified, such as calculating penalty score of func implementation.
    auto extension_function = bind_function(function, args);
    return true;
  } catch (const CiderCompileException& e) {
    return false;
  }
}

int getSizeOfOutputColumns(const substrait::Rel& rel_node) {
  switch (rel_node.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead:
      return rel_node.read().base_schema().names_size();
    case substrait::Rel::RelTypeCase::kFilter:
      return getSizeOfOutputColumns(rel_node.filter().input());
    case substrait::Rel::RelTypeCase::kProject:
      if (rel_node.project().common().has_emit()) {
        return rel_node.project().common().emit().output_mapping_size();
      }
      if (rel_node.project().common().has_direct()) {
        return getSizeOfOutputColumns(rel_node.project().input()) +
               rel_node.project().expressions_size();
      }
    case substrait::Rel::RelTypeCase::kAggregate:
      if (!rel_node.aggregate().common().has_direct()) {
        CIDER_THROW(CiderCompileException,
                    "Only support direct output mapping for AggregateRel.");
      }
      // only support single grouping and multi measures
      return rel_node.aggregate().groupings(0).grouping_expressions_size() +
             rel_node.aggregate().measures_size();
    case substrait::Rel::RelTypeCase::kJoin:
      if (!rel_node.join().common().has_direct()) {
        CIDER_THROW(CiderCompileException,
                    "Only support direct output mapping for Join.");
      }
      return getSizeOfOutputColumns(rel_node.join().left()) +
             getSizeOfOutputColumns(rel_node.join().right());
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Couldn't get output column size for {}",
                              rel_node.rel_type_case()));
  }
}

int getLeftJoinDepth(const substrait::Plan& plan) {
  if (plan.relations_size() == 0) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  if (!plan.relations(0).has_root()) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  substrait::Rel rel_node = plan.relations(0).root().input();
  int join_depth = 0;
  while (!rel_node.has_read()) {
    switch (rel_node.rel_type_case()) {
      case substrait::Rel::RelTypeCase::kFilter: {
        auto input = rel_node.filter().input();
        rel_node = input;
        continue;
      }
      case substrait::Rel::RelTypeCase::kProject: {
        auto input = rel_node.project().input();
        rel_node = input;
        continue;
      }
      case substrait::Rel::RelTypeCase::kAggregate: {
        auto input = rel_node.aggregate().input();
        rel_node = input;
        continue;
      }
      case substrait::Rel::RelTypeCase::kJoin: {
        // only cover left deeper join
        auto input = rel_node.join().left();
        rel_node = input;
        ++join_depth;
        continue;
      }
      default:
        CIDER_THROW(
            CiderCompileException,
            fmt::format("Unsupported substrait rel type {}", rel_node.rel_type_case()));
    }
  }
  return join_depth;
}

JoinType getCiderJoinType(const substrait::JoinRel_JoinType& s_join_type) {
  switch (s_join_type) {
    case substrait::JoinRel_JoinType_JOIN_TYPE_ANTI:
      return JoinType::ANTI;
    case substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
      return JoinType::INNER;
    case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
      return JoinType::LEFT;
    case substrait::JoinRel_JoinType_JOIN_TYPE_SEMI:
      return JoinType::SEMI;
    default:
      return JoinType::INVALID;
  }
}

Analyzer::Expr* getExpr(std::shared_ptr<Analyzer::Expr> expr, bool is_partial_avg) {
  if (auto bin_oper_expr = std::dynamic_pointer_cast<Analyzer::BinOper>(expr)) {
    return new Analyzer::BinOper(bin_oper_expr->get_type_info(),
                                 false,
                                 bin_oper_expr->get_optype(),
                                 bin_oper_expr->get_qualifier(),
                                 bin_oper_expr->get_non_const_left_operand(),
                                 bin_oper_expr->get_non_const_right_operand());
  }
  // FIXME: pay attention that UOper has different constructor
  if (auto u_oper_expr = std::dynamic_pointer_cast<Analyzer::UOper>(expr)) {
    return new Analyzer::UOper(u_oper_expr->get_type_info(),
                               u_oper_expr->get_contains_agg(),
                               u_oper_expr->get_optype(),
                               u_oper_expr->get_non_const_own_operand());
  }
  if (auto agg_expr = std::dynamic_pointer_cast<Analyzer::AggExpr>(expr)) {
    // A special case for sum() in avg partial, following cider's rules
    // This will only affect the RelAlgExecutionUnit, output table schema still keeps
    // consistent with substrait plan
    if (is_partial_avg && agg_expr->get_aggtype() == SQLAgg::kSUM) {
      auto arg_expr = agg_expr->get_arg();
      auto agg_type_info = arg_expr->get_type_info().is_integer()
                               ? SQLTypeInfo(SQLTypes::kBIGINT, false)
                               : arg_expr->get_type_info();
      return new Analyzer::AggExpr(agg_type_info,
                                   agg_expr->get_aggtype(),
                                   agg_expr->get_own_arg(),
                                   false,
                                   agg_expr->get_arg1());
    }
    return new Analyzer::AggExpr(agg_expr->get_type_info(),
                                 agg_expr->get_aggtype(),
                                 agg_expr->get_own_arg(),
                                 agg_expr->get_is_distinct(),
                                 agg_expr->get_arg1());
  }
  if (auto var_expr = std::dynamic_pointer_cast<Analyzer::Var>(expr)) {
    return new Analyzer::Var(var_expr->get_type_info(),
                             var_expr->get_table_id(),
                             var_expr->get_column_id(),
                             var_expr->get_rte_idx(),
                             false,
                             var_expr->get_which_row(),
                             var_expr->get_varno());
  }
  if (auto column_var_expr = std::dynamic_pointer_cast<Analyzer::ColumnVar>(expr)) {
    return new Analyzer::ColumnVar(column_var_expr->get_type_info(),
                                   column_var_expr->get_table_id(),
                                   column_var_expr->get_column_id(),
                                   column_var_expr->get_rte_idx());
  }
  if (auto case_expr = std::dynamic_pointer_cast<Analyzer::CaseExpr>(expr)) {
    return new Analyzer::CaseExpr(case_expr->get_type_info(),
                                  case_expr->get_contains_agg(),
                                  case_expr->get_expr_pair_list(),
                                  case_expr->get_else_ref());
  }
  if (auto constant_expr = std::dynamic_pointer_cast<Analyzer::Constant>(expr)) {
    return new Analyzer::Constant(constant_expr->get_type_info(),
                                  constant_expr->get_is_null(),
                                  constant_expr->get_constval());
  }
  if (auto column_var_expr = std::dynamic_pointer_cast<Analyzer::DateaddExpr>(expr)) {
    return new Analyzer::DateaddExpr(column_var_expr->get_type_info(),
                                     column_var_expr->get_field(),
                                     column_var_expr->get_number(),
                                     column_var_expr->get_datetime());
  }
  if (auto extract_expr = std::dynamic_pointer_cast<Analyzer::ExtractExpr>(expr)) {
    return new Analyzer::ExtractExpr(
        extract_expr->get_type_info(),
        extract_expr->get_own_from_expr()->get_contains_agg(),
        extract_expr->get_field(),
        extract_expr->get_own_from_expr()->decompress());
  }
  if (auto string_expr = std::dynamic_pointer_cast<Analyzer::TryStringCastOper>(expr)) {
    return new Analyzer::TryStringCastOper(SQLTypes::kDATE, string_expr->getOwnArgs());
  }
  if (auto string_expr = std::dynamic_pointer_cast<Analyzer::StringOper>(expr)) {
    return new Analyzer::StringOper(string_expr->get_kind(), string_expr->getOwnArgs());
  }
  CIDER_THROW(CiderCompileException, "Failed to get target expr.");
}

std::unordered_map<int, std::string> getFunctionMap(
    const std::vector<
        substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
        func_infos) {
  // Generates func map based on func_infos
  std::unordered_map<int, std::string> function_map;
  for (auto function : func_infos) {
    auto ext_func_name = function->name();
    auto func_name = ext_func_name.substr(0, ext_func_name.find_first_of(':'));
    function_map.emplace(function->function_anchor(), func_name);
  }
  return function_map;
}
}  // namespace generator
