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
#include "exec/nextgen/operators/ArrowSourceNode.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/HashJoinNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
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

  void run() {
    size_t index = 0;
    for (auto iter = eu_.input_col_descs.begin(); iter != eu_.input_col_descs.end();
         ++iter) {
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

    if (!eu_.shared_target_exprs.empty()) {
      for (auto& expr : eu_.shared_target_exprs) {
        traverse(&expr);
      }
    }

    if (!eu_.join_quals.empty()) {
      for (auto& join_condition : eu_.join_quals) {
        for (auto join_expr : join_condition.quals) {
          traverse(&join_expr);
        }
      }
    }

    input_exprs_.erase(
        std::remove_if(input_exprs_.begin(),
                       input_exprs_.end(),
                       [](const ExprPtr& input_expr) { return input_expr == nullptr; }),
        input_exprs_.end());

    for (auto expr : input_exprs_) {
      if (expr != nullptr) {
        auto col_var = dynamic_cast<Analyzer::ColumnVar*>(expr.get());
        if (col_var->get_table_id() == 100) {
          left_exprs_.push_back(expr);
        } else {
          right_exprs_.push_back(expr);
        }
      }
    }
  }

  ExprPtrVector& getInputExprs() { return input_exprs_; }

  ExprPtrVector& getLeftExprs() { return left_exprs_; }

  ExprPtrVector& getRightExprs() { return right_exprs_; }

 private:
  void traverse(ExprPtr* curr) {
    if (auto col_var_ptr = dynamic_cast<Analyzer::ColumnVar*>(curr->get())) {
      auto iter = input_desc_to_index_.find(
          InputColDescriptor(col_var_ptr->get_column_info(), col_var_ptr->get_rte_idx()));
      CHECK(iter != input_desc_to_index_.end());

      size_t index = iter->second;
      if (input_exprs_[index]) {
        *curr = input_exprs_[index];
      } else {
        input_exprs_[index] = *curr;
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
  ExprPtrVector left_exprs_;
  ExprPtrVector right_exprs_;
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
  analyzer.run();

  ops.insert(ops.begin(), createOpNode<ArrowSourceNode>(analyzer.getLeftExprs()));

  ExprPtrVector join_quals;
  for (auto& join_condition : eu.join_quals) {
    for (auto& join_expr : join_condition.quals) {
      join_quals.push_back(join_expr);
    }
  }
  if (join_quals.size() > 0) {
    ops.emplace_back(createOpNode<HashJoinNode>(
        analyzer.getLeftExprs(), analyzer.getRightExprs(), join_quals));
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
