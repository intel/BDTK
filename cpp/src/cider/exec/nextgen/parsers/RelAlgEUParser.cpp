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

#include "exec/nextgen/parsers/Parser.h"

#include "exec/nextgen/operators/AggregationNode.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/HashJoinNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/QueryFuncInitializer.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::parsers {

using namespace cider::exec::nextgen::operators;

static bool isParseable(const RelAlgExecutionUnit& eu) {
  if (eu.groupby_exprs.size() > 1 || *eu.groupby_exprs.begin() != nullptr) {
    LOG(ERROR) << "GroupBy is not supported in RelAlgExecutionUnitParser.";
    return false;
  }

  return true;
}

// Used for input columnVar collection and combination
class InputAnalyzer {
 public:
  explicit InputAnalyzer(RelAlgExecutionUnit& eu)
      : eu_(eu), input_exprs_(eu.input_col_descs.size(), nullptr) {}

  ExprPtrVector& run() {
    size_t index = 0;
    bool tag = true;
    for (auto iter = eu_.input_col_descs.begin(); iter != eu_.input_col_descs.end();
         ++iter) {
      // record build table first column index
      if (iter->get()->getTableId() == 101 && tag) {
        build_table_offset_ = index;
        tag = false;
      }
      input_desc_to_index_.insert({**iter, index++});
    }

    if (!eu_.simple_quals.empty()) {
      for (auto& expr : eu_.simple_quals) {
        traverse(&expr);
      }
    }

    if (!eu_.quals.empty()) {
      for (auto& expr : eu_.quals) {
        traverse(&expr);
      }
    }

    if (!eu_.join_quals.empty()) {
      for (auto& join_condition : eu_.join_quals) {
        for (auto& join_expr : join_condition.quals) {
          traverse(&const_cast<ExprPtr&>(join_expr));
        }
      }
    }

    if (!eu_.shared_target_exprs.empty()) {
      for (auto& expr : eu_.shared_target_exprs) {
        traverse(&expr);
      }
    }

    input_exprs_.erase(
        std::remove_if(input_exprs_.begin(),
                       input_exprs_.end(),
                       [](const ExprPtr& input_expr) { return input_expr == nullptr; }),
        input_exprs_.end());

    return input_exprs_;
  }

  ExprPtrVector& getInputExprs() { return input_exprs_; }

  std::map<ExprPtr, size_t>& getBuildTableMap() { return build_table_map_; }

 private:
  void traverse(ExprPtr* curr) {
    if (auto col_var_ptr = dynamic_cast<Analyzer::ColumnVar*>(curr->get())) {
      auto iter = input_desc_to_index_.find(
          InputColDescriptor(col_var_ptr->get_column_info(), col_var_ptr->get_rte_idx()));
      CHECK(iter != input_desc_to_index_.end());

      size_t input_exprs_index = iter->second;
      if (input_exprs_[input_exprs_index]) {
        *curr = input_exprs_[input_exprs_index];
      } else {
        // insert build table expr
        if (col_var_ptr->get_table_id() == 101) {
          build_table_map_.insert({*curr, input_exprs_index - build_table_offset_});
        }
        input_exprs_[input_exprs_index] = *curr;
      }
    } else {
      auto children = (*curr)->get_children_reference();
      for (ExprPtr* child : children) {
        if (!child || !child->get()) {
          // optional args in some exprs returns nullptr in child->get(),
          // (e.g. escape in LIKE) check and skip them to prevent segfaults
          continue;
        }
        traverse(child);
      }
    }
  }

  RelAlgExecutionUnit& eu_;
  ExprPtrVector input_exprs_;
  std::map<ExprPtr, size_t> build_table_map_;
  // record build table index
  size_t build_table_offset_;
  std::unordered_map<InputColDescriptor, size_t> input_desc_to_index_;
};

OpPipeline toOpPipeline(RelAlgExecutionUnit& eu) {
  // TODO (bigPYJ1151): Only support filter and project now.
  // TODO (bigPYJ1151): Only naive expression dispatch now, need to analyze expression
  // trees and dispatch more fine-grained.
  if (!isParseable(eu)) {
    return {};
  }
  OpPipeline ops;

  InputAnalyzer analyzer(eu);
  auto&& input_exprs = analyzer.run();

  // Relpace ColumnVar in target_exprs with OutputColumnVar to distinguish input cols and
  // output cols.
  for (auto& expr : eu.shared_target_exprs) {
    if (auto column_var_ptr = std::dynamic_pointer_cast<Analyzer::ColumnVar>(expr)) {
      expr = std::make_shared<Analyzer::OutputColumnVar>(column_var_ptr);
    }
  }

  ops.emplace_back(
      createOpNode<QueryFuncInitializer>(input_exprs, eu.shared_target_exprs));

  ExprPtrVector join_quals;
  for (auto& join_condition : eu.join_quals) {
    for (auto& join_expr : join_condition.quals) {
      join_quals.push_back(join_expr);
    }
  }

  if (!join_quals.empty()) {
    ops.emplace_back(createOpNode<HashJoinNode>(
        analyzer.getInputExprs(), join_quals, analyzer.getBuildTableMap()));
  }

  ExprPtrVector filters;
  for (auto& filter_expr : eu.simple_quals) {
    filters.push_back(filter_expr);
  }
  for (auto& filter_expr : eu.quals) {
    filters.push_back(filter_expr);
  }
  if (filters.size() > 0) {
    ops.emplace_back(createOpNode<operators::FilterNode>(filters));
  }

  ExprPtrVector projs;
  ExprPtrVector aggs;
  ExprPtrVector groupbys;

  for (auto& targets_expr : eu.shared_target_exprs) {
    if (targets_expr->get_contains_agg()) {
      aggs.push_back(targets_expr);
    } else {
      projs.push_back(targets_expr);
    }
  }
  if (projs.size() > 0) {
    ops.emplace_back(createOpNode<operators::ProjectNode>(projs));
  }

  if (!eu.groupby_exprs.empty() && eu.groupby_exprs.front()) {
    for (auto& groupby_expr : eu.groupby_exprs) {
      groupbys.push_back(groupby_expr);
    }
  }

  if (!groupbys.empty() || !aggs.empty()) {
    ops.emplace_back(createOpNode<operators::AggNode>(groupbys, aggs));
  }

  return ops;
}

}  // namespace cider::exec::nextgen::parsers
