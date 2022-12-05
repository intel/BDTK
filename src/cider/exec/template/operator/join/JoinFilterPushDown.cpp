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

#include "JoinFilterPushDown.h"
#include "exec/template/DeepCopyVisitor.h"

namespace {

class BindFilterToOutermostVisitor : public DeepCopyVisitor {
  std::shared_ptr<Analyzer::Expr> visitColumnVar(
      const Analyzer::ColumnVar* col_var) const override {
    return makeExpr<Analyzer::ColumnVar>(col_var->get_column_info(), 0);
  }
};

class CollectInputColumnsVisitor
    : public ScalarExprVisitor<std::unordered_set<InputColDescriptor>> {
  std::unordered_set<InputColDescriptor> visitColumnVar(
      const Analyzer::ColumnVar* col_var) const override {
    return {InputColDescriptor(col_var->get_column_info(), 0)};
  }

 public:
  std::unordered_set<InputColDescriptor> aggregateResult(
      const std::unordered_set<InputColDescriptor>& aggregate,
      const std::unordered_set<InputColDescriptor>& next_result) const override {
    auto result = aggregate;
    result.insert(next_result.begin(), next_result.end());
    return result;
  }
};

}  // namespace

/**
 * The main purpose of this function is to prevent going through extra overhead of
 * computing required statistics for finding the right candidates and then the actual
 * push-down, unless the problem is large enough that such effort is potentially helpful.
 */
bool to_gather_info_for_filter_selectivity(
    const std::vector<InputTableInfo>& table_infos) {
  if (table_infos.size() < 2) {
    return false;
  }
  // we currently do not support filter push down when there is a self-join involved:
  // TODO(Saman): prevent Calcite from optimizing self-joins to remove this exclusion
  std::unordered_set<int> table_ids;
  for (auto ti : table_infos) {
    if (table_ids.find(ti.table_id) == table_ids.end()) {
      table_ids.insert(ti.table_id);
    } else {
      // a self-join is involved
      return false;
    }
  }
  // TODO(Saman): add some extra heuristics to avoid preflight count and push down if it
  // is not going to be helpful.
  return true;
}

/**
 * Go through all tables involved in the relational algebra plan, and select potential
 * candidates to be pushed down by calcite. For each filter we store a set of
 * intermediate indices (previous, current, and next table) based on the column
 * indices in their query string.
 */
std::vector<PushedDownFilterInfo> find_push_down_filters(
    const RelAlgExecutionUnit& ra_exe_unit,
    const std::vector<size_t>& input_permutation,
    const std::vector<size_t>& left_deep_join_input_sizes) {
  std::vector<PushedDownFilterInfo> result;
  if (left_deep_join_input_sizes.empty()) {
    return result;
  }
  std::vector<size_t> input_size_prefix_sums(left_deep_join_input_sizes.size());
  std::partial_sum(left_deep_join_input_sizes.begin(),
                   left_deep_join_input_sizes.end(),
                   input_size_prefix_sums.begin());
  std::vector<int> to_original_rte_idx(ra_exe_unit.input_descs.size(),
                                       ra_exe_unit.input_descs.size());
  if (!input_permutation.empty()) {
    CHECK_EQ(to_original_rte_idx.size(), input_permutation.size());
    for (size_t i = 0; i < input_permutation.size(); ++i) {
      CHECK_LT(input_permutation[i], to_original_rte_idx.size());
      CHECK_EQ(static_cast<size_t>(to_original_rte_idx[input_permutation[i]]),
               to_original_rte_idx.size());
      to_original_rte_idx[input_permutation[i]] = i;
    }
  } else {
    std::iota(to_original_rte_idx.begin(), to_original_rte_idx.end(), 0);
  }
  std::unordered_map<int, std::vector<std::shared_ptr<Analyzer::Expr>>>
      filters_per_nesting_level;
  for (const auto& level_conditions : ra_exe_unit.join_quals) {
    AllRangeTableIndexVisitor visitor;
    for (const auto& cond : level_conditions.quals) {
      const auto rte_indices = visitor.visit(cond.get());
      if (rte_indices.size() > 1) {
        continue;
      }
      const int rte_idx = (!rte_indices.empty()) ? *rte_indices.cbegin() : 0;
      if (!rte_idx) {
        continue;
      }
      CHECK_GE(rte_idx, 0);
      CHECK_LT(static_cast<size_t>(rte_idx), to_original_rte_idx.size());
      filters_per_nesting_level[to_original_rte_idx[rte_idx]].push_back(cond);
    }
  }
  for (const auto& kv : filters_per_nesting_level) {
    CHECK_GE(kv.first, 0);
    CHECK_LT(static_cast<size_t>(kv.first), input_size_prefix_sums.size());
    size_t input_prev = (kv.first > 1) ? input_size_prefix_sums[kv.first - 2] : 0;
    size_t input_start = kv.first ? input_size_prefix_sums[kv.first - 1] : 0;
    size_t input_next = input_size_prefix_sums[kv.first];
    result.emplace_back(
        PushedDownFilterInfo{kv.second, input_prev, input_start, input_next});
  }
  return result;
}
