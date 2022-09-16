/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include "CardinalityEstimator.h"
#include "ErrorHandling.h"
#include "ExpressionRewrite.h"

int64_t g_large_ndv_threshold = 10000000;
size_t g_large_ndv_multiplier = 256;

namespace Analyzer {

size_t LargeNDVEstimator::getBufferSize() const {
  return 1024 * 1024 * g_large_ndv_multiplier;
}

}  // namespace Analyzer

RelAlgExecutionUnit create_ndv_execution_unit(const RelAlgExecutionUnit& ra_exe_unit,
                                              const int64_t range) {
  const bool use_large_estimator = range > g_large_ndv_threshold;
  return {ra_exe_unit.input_descs,
          ra_exe_unit.input_col_descs,
          ra_exe_unit.simple_quals,
          ra_exe_unit.quals,
          ra_exe_unit.join_quals,
          {},
          {},
          use_large_estimator
              ? makeExpr<Analyzer::LargeNDVEstimator>(ra_exe_unit.groupby_exprs)
              : makeExpr<Analyzer::NDVEstimator>(ra_exe_unit.groupby_exprs),
          SortInfo{{}, SortAlgorithm::Default, 0, 0},
          0,
          ra_exe_unit.query_hint,
          ra_exe_unit.query_plan_dag,
          ra_exe_unit.hash_table_build_plan_dag,
          ra_exe_unit.table_id_to_node_map,
          false,
          ra_exe_unit.union_all};
}

RelAlgExecutionUnit create_count_all_execution_unit(
    const RelAlgExecutionUnit& ra_exe_unit,
    std::shared_ptr<Analyzer::Expr> replacement_target) {
  return {ra_exe_unit.input_descs,
          ra_exe_unit.input_col_descs,
          ra_exe_unit.simple_quals,
          strip_join_covered_filter_quals(ra_exe_unit.quals, ra_exe_unit.join_quals),
          ra_exe_unit.join_quals,
          {},
          {replacement_target.get()},
          nullptr,
          SortInfo{{}, SortAlgorithm::Default, 0, 0},
          0,
          ra_exe_unit.query_hint,
          ra_exe_unit.query_plan_dag,
          ra_exe_unit.hash_table_build_plan_dag,
          ra_exe_unit.table_id_to_node_map,
          false,
          ra_exe_unit.union_all};
}