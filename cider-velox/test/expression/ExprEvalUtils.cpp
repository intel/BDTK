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

#include "ExprEvalUtils.h"
#include "exec/plan/parser/Translator.h"
#include "exec/template/ExpressionRewrite.h"
#include "exec/template/InputMetadata.h"
#include "exec/template/RelAlgExecutionUnit.h"
#include "type/data/sqltypes.h"
#include "util/memory/Fragmenter.h"

namespace facebook::velox::plugin {

RelAlgExecutionUnit ExprEvalUtils::getMockedRelAlgEU(
    std::shared_ptr<const core::ITypedExpr> v_expr,
    RowTypePtr row_type,
    std::string eu_group) {
  int fake_table_id = 100;
  int fake_db_id = 100;
  int cur_nest_level = 0;
  InputDescriptor input_desc(fake_table_id, cur_nest_level);
  std::vector<InputDescriptor> input_descs;
  input_descs.insert(input_descs.begin(), input_desc);
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  std::unordered_map<std::string, int> col_info;
  std::vector<Analyzer::Expr*> target_exprs;
  std::list<std::shared_ptr<Analyzer::Expr>> quals;
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  for (int i = 0; i < row_type->size(); i++) {
    auto col_desc = std::make_shared<ColumnInfo>(fake_db_id,
                                                 fake_table_id,
                                                 i,
                                                 row_type->names()[i],
                                                 getCiderType(row_type->childAt(i), true),
                                                 false);
    input_col_descs.push_back(
        std::make_shared<const InputColDescriptor>(col_desc, cur_nest_level));
    col_info.emplace(row_type->names()[i], i);
    auto col_expr = std::make_shared<const Analyzer::ColumnVar>(col_desc, cur_nest_level);
    target_exprs.emplace_back(getExpr(col_expr));
  }
  VeloxToCiderExprConverter expr_converter;
  if (eu_group == "comparison") {
    auto c_expr = expr_converter.toCiderExpr(v_expr, col_info);
    auto quals_filter = qual_to_conjunctive_form(fold_expr(getExpr(c_expr)));
    quals = quals_filter.quals;
    simple_quals = quals_filter.simple_quals;
  }
  if (eu_group == "arithmetic") {
    target_exprs.clear();
    auto c_expr = expr_converter.toCiderExpr(v_expr, col_info);
    target_exprs.emplace_back(getExpr(c_expr));
  }
  RelAlgExecutionUnit rel_alg_eu{input_descs,
                                 input_col_descs,
                                 simple_quals,
                                 quals,
                                 {},
                                 {nullptr},
                                 target_exprs,
                                 nullptr,
                                 {{}, SortAlgorithm::Default, 0, 0},
                                 0,
                                 RegisteredQueryHint::defaults(),
                                 EMPTY_QUERY_PLAN,
                                 {},
                                 {},
                                 false,
                                 std::nullopt};
  return rel_alg_eu;
}

std::vector<InputTableInfo> ExprEvalUtils::buildInputTableInfo() {
  std::vector<InputTableInfo> queryInfos;
  Fragmenter_Namespace::FragmentInfo fragmentInfo;
  fragmentInfo.fragmentId = 0;
  fragmentInfo.shadowNumTuples = 1024;
  // use 100 as the faked table id
  fragmentInfo.physicalTableId = 100;
  fragmentInfo.setPhysicalNumTuples(1024);

  Fragmenter_Namespace::TableInfo tableInfo;
  tableInfo.fragments = {fragmentInfo};
  tableInfo.setPhysicalNumTuples(1024);
  InputTableInfo inputTableInfo{100, -1};
  queryInfos.push_back(inputTableInfo);

  return queryInfos;
}

AggregatedColRange ExprEvalUtils::buildDefaultColRangeCache(RowTypePtr row_type) {
  int fakeTableId = 100;
  AggregatedColRange colRangeCache;
  for (int i = 0; i < row_type->size(); i++) {
    PhysicalInput physicalInput{i, fakeTableId};
    auto ciderType = getCiderType(row_type->childAt(i), true);
    switch (ciderType.get_type()) {
      case SQLTypes::kDOUBLE:
      case SQLTypes::kDECIMAL: {
        colRangeCache.setColRange(physicalInput,
                                  ExpressionRange::makeDoubleRange(1.2, 100.2, false));
        break;
      }
      case SQLTypes::kFLOAT: {
        colRangeCache.setColRange(physicalInput,
                                  ExpressionRange::makeFloatRange(1.2, 100.2, false));
        break;
      }
      case SQLTypes::kBIGINT:
      case SQLTypes::kINT: {
        colRangeCache.setColRange(physicalInput,
                                  ExpressionRange::makeIntRange(1, 100, 0, false));
        break;
      }
      default:
        colRangeCache.setColRange(physicalInput, ExpressionRange::makeInvalidRange());
        break;
    }
  }
  return colRangeCache;
}

SQLTypeInfo ExprEvalUtils::getCiderType(
    const std::shared_ptr<const velox::Type> expr_type,
    bool isNullable) {
  // SQLTypeInfo notnull has a opposite value against isNullable
  bool notNull = !isNullable;
  switch (expr_type->kind()) {
    case TypeKind::BOOLEAN:
      return SQLTypeInfo(SQLTypes::kBOOLEAN, notNull);
    case TypeKind::DOUBLE:
      return SQLTypeInfo(SQLTypes::kDOUBLE, notNull);
    case TypeKind::INTEGER:
      return SQLTypeInfo(SQLTypes::kINT, notNull);
    case TypeKind::BIGINT:
      return SQLTypeInfo(SQLTypes::kBIGINT, notNull);
    case TypeKind::TIMESTAMP:
      return SQLTypeInfo(SQLTypes::kTIMESTAMP, notNull);
    default:
      VELOX_UNSUPPORTED(expr_type->toString() + " is not yet supported.");
  }
}

Analyzer::Expr* ExprEvalUtils::getExpr(std::shared_ptr<const Analyzer::Expr> expr) {
  if (auto bin_oper_expr = std::dynamic_pointer_cast<const Analyzer::BinOper>(expr)) {
    return new Analyzer::BinOper(bin_oper_expr->get_type_info(),
                                 false,
                                 bin_oper_expr->get_optype(),
                                 bin_oper_expr->get_qualifier(),
                                 bin_oper_expr->get_non_const_left_operand(),
                                 bin_oper_expr->get_non_const_right_operand());
  }
  // Note that UOper has different constructors that be careful when create it
  if (auto u_oper_expr = std::dynamic_pointer_cast<const Analyzer::UOper>(expr)) {
    return new Analyzer::UOper(u_oper_expr->get_type_info(),
                               u_oper_expr->get_contains_agg(),
                               u_oper_expr->get_optype(),
                               u_oper_expr->get_non_const_own_operand());
  }
  if (auto agg_expr = std::dynamic_pointer_cast<const Analyzer::AggExpr>(expr)) {
    return new Analyzer::AggExpr(agg_expr->get_type_info(),
                                 agg_expr->get_aggtype(),
                                 agg_expr->get_own_arg(),
                                 false,
                                 agg_expr->get_arg1());
  }
  if (auto var_expr = std::dynamic_pointer_cast<const Analyzer::Var>(expr)) {
    return new Analyzer::Var(var_expr->get_type_info(),
                             var_expr->get_table_id(),
                             var_expr->get_column_id(),
                             var_expr->get_rte_idx(),
                             false,
                             var_expr->get_which_row(),
                             var_expr->get_varno());
  }
  // For current expr evaluation, we don't consider aggregation, groupby and join cases
  if (auto column_var_expr = std::dynamic_pointer_cast<const Analyzer::ColumnVar>(expr)) {
    return new Analyzer::ColumnVar(column_var_expr->get_type_info(),
                                   column_var_expr->get_table_id(),
                                   column_var_expr->get_column_id(),
                                   column_var_expr->get_rte_idx());
  }
  VELOX_UNSUPPORTED("unsupported column type for target expr.");
}
}  // namespace facebook::velox::plugin
