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
 * @file    GeneratorContext.h
 * @brief   GeneratorContext struct
 **/

#pragma once

#include <cstdint>
#include "ConverterHelper.h"
#include "SubstraitToAnalyzerExpr.h"
#include "cider/CiderException.h"
#include "cider/CiderTableSchema.h"
#include "exec/template/RelAlgExecutionUnit.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

namespace generator {

/**
 * since some fields like groupby_exprs, sort_inofo in RelAlgeExecutionUnit are immutable
 * whcih conflicts with our generation approach, create a similar class and then generate
 * the final RelAlgExecutionUnit
 */
struct GeneratorContext {
  std::vector<InputDescriptor> input_descs_;
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs_;
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals_;
  std::list<std::shared_ptr<Analyzer::Expr>> quals_;
  std::vector<std::shared_ptr<Analyzer::Expr>> target_exprs_;
  std::vector<std::shared_ptr<Analyzer::Expr>> groupby_exprs_;
  std::list<Analyzer::OrderEntry> orderby_collation_;
  size_t limit_;
  size_t offset_;
  bool has_agg_;
  JoinQualsPerNestingLevel join_quals_;
  int cur_join_depth_;
  bool is_target_exprs_collected_;
  bool is_partial_avg_;
  // Records hints of the col data: case name and the amount of corresponding flatten
  // types
  // (case 1) <"normal", 1> represents a normal case, such as fp64, bool, ...
  // (case 2) <"avg_partial", 2> represents avg partial case,
  //          which is composed of 2 kinds of flatten types, e.g., <fp64, i64>
  std::vector<std::pair<ColumnHint, int>> col_hint_records_;

  std::string toString() const {
    return ::typeName(this) +
           "(InputDescriptor size=" + std::to_string(input_descs_.size()) +
           ", InputColDescriptor size=" + std::to_string(input_col_descs_.size()) +
           ", SimpleQuals size=" + std::to_string(simple_quals_.size()) +
           ", Quals size=" + std::to_string(quals_.size()) +
           ", TargetExprs size=" + std::to_string(target_exprs_.size()) +
           ", GroupbyExprs size=" + std::to_string(groupby_exprs_.size()) +
           ", OrderbyCollation size=" + std::to_string(orderby_collation_.size()) +
           ", JoinQuals size=" + std::to_string(join_quals_.size()) + ")";
  }

  SortInfo getSortInfoFromCtx() {
    if (orderby_collation_.size() == 0) {
      return {{}, SortAlgorithm::Default, 0, 0};
    } else {
      return {orderby_collation_, SortAlgorithm::SpeculativeTopN, limit_, offset_};
    }
  }

  RelAlgExecutionUnit getExeUnitBasedOnContext() {
    if (!is_target_exprs_collected_) {
      CIDER_THROW(CiderCompileException, "No target expressions collected.");
    }
    // change target_expr to normal pointer for RelAlgeExecutionUnit
    std::vector<Analyzer::Expr*> target_exprs;
    for (auto target_expr : target_exprs_) {
      target_exprs.emplace_back(getExpr(target_expr, is_partial_avg_));
    }
    std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
    for (auto groupby_expr : groupby_exprs_) {
      groupby_exprs.emplace_back(groupby_expr);
    }

    if (!has_agg_ && !groupby_exprs.size()) {
      std::shared_ptr<Analyzer::Expr> empty;
      groupby_exprs.emplace_back(empty);
    }

    RelAlgExecutionUnit rel_alg_eu{input_descs_,
                                   input_col_descs_,
                                   simple_quals_,
                                   quals_,
                                   join_quals_,
                                   groupby_exprs,
                                   target_exprs,
                                   nullptr,
                                   getSortInfoFromCtx(),
                                   0,
                                   RegisteredQueryHint::defaults(),
                                   EMPTY_QUERY_PLAN,
                                   {},
                                   {},
                                   false,
                                   std::nullopt};
    return rel_alg_eu;
  }
};

}  // namespace generator
