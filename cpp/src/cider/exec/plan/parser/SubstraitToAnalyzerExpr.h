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

/**
 * @file    SubstraitToAnalyzerExpr.h
 * @brief   Translate Substrait expression to Analyzer expression
 **/

#pragma once

#include <cstdint>
#include "ConverterHelper.h"
#include "include/cider/CiderSupportPlatType.h"
#include "substrait/algebra.pb.h"
#include "type/plan/SqlTypes.h"
#include "type/plan/Analyzer.h"

namespace generator {

enum ContextUpdateType {
  UpdateOnlyIndex,  // update pre_index to cur_index for field reference
  UpdateColDesc,    // update pre_index to <table_id, cur_index, rte_idx>
  UpdateOnlyType,   // update field reference with correct type if table_id is matched
  UpdateToExpr      // update field reference index to corresponding expression
};

class Substrait2AnalyzerExprConverter {
 public:
  explicit Substrait2AnalyzerExprConverter(PlatformType from_platform)
      : cols_update_stat_{}
      , cur_table_id_{fake_table_id}
      , from_platform_{from_platform} {}
  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression& s_expr,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::AggregateFunction& s_expr,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr,
      std::string return_type = "");

  // new_table_id is used when updating a ColumnVar to new index in join node
  std::shared_ptr<Analyzer::Expr> updateAnalyzerExpr(
      std::shared_ptr<Analyzer::Expr> expr,
      std::int32_t pre_index,
      std::int32_t cur_index,
      int table_id,
      int rte_idx,
      std::shared_ptr<Analyzer::Expr> cur_expr,
      const substrait::Type& s_type,
      ContextUpdateType update_type,
      int new_table_id = 0);

  std::shared_ptr<Analyzer::AggExpr> updateOutputTypeOfAVGPartial(
      std::shared_ptr<Analyzer::Expr> expr,
      const substrait::Type& s_type);

  std::shared_ptr<Analyzer::ColumnVar> makeColumnVar(const SQLTypeInfo& ti,
                                                     int table_id,
                                                     int col_id,
                                                     int nest_level,
                                                     bool updatable);

  std::shared_ptr<Analyzer::Var> makeVar(const SQLTypeInfo& ti,
                                         int r,
                                         int c,
                                         int i,
                                         bool is_virtual,
                                         Analyzer::Var::WhichRow o,
                                         int v,
                                         bool updatable);

  // The stat will reset after each project node and join node
  void resetColsUpdateStat() {
    for (auto it = cols_update_stat_.begin(); it != cols_update_stat_.end(); it++) {
      it->second = false;
    }
  }

  void setTableIdForNewColVar(int table_id) { cur_table_id_ = table_id; }

  int getCurTableId() const { return cur_table_id_; }

 private:
  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression_Literal& s_literal_expr);

  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression_FieldReference& s_selection_expr,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression_Cast& s_cast_expr,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::list<std::shared_ptr<Analyzer::Expr>> toAnalyzerExprList(
      const substrait::Expression_Literal& s_literal_expr);

  std::shared_ptr<Analyzer::Expr> toAnalyzerExpr(
      const substrait::Expression_IfThen& s_if_then_expr,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildTimeAddExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::string& function_name,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildInValuesExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildNotNullExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildExtractExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      const std::string& function_name,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildLikeExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildStrExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::string function_name,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildStrExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      int list_ref_offset,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  std::shared_ptr<Analyzer::Expr> buildCoalesceExpr(
      const substrait::Expression_ScalarFunction& s_scalar_function,
      const std::unordered_map<int, std::string> function_map,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          expr_map_ptr = nullptr);

  bool isColumnVar(const substrait::Expression_FieldReference& selection_expr);

  bool isTimeType(const substrait::Type& type);

  // This cols update stat is used for avoiding duplicated index update in ColumnVar/Var
  std::unordered_map<std::shared_ptr<Analyzer::Expr>, bool> cols_update_stat_;

  // When consider creating a ColumnVar in a project node before join, need to know
  // what's the current table id
  // TODO: remove it to assure expression convertor is stateless
  int cur_table_id_;

  PlatformType from_platform_;
};
}  // namespace generator
