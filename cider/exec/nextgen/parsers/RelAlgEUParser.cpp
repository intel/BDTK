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

#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/SourceNode.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::parsers {
using namespace cider::exec::nextgen::operators;

static bool isParseable(const RelAlgExecutionUnit& eu) {
  if (!eu.join_quals.empty()) {
    LOG(ERROR) << "JOIN is not supported in RelAlgExecutionUnitParser.";
    return false;
  }
  if (eu.groupby_exprs.empty()) {
    LOG(ERROR) << "Aggregation is not supported in RelAlgExecutionUnitParser.";
    return false;
  }
  if (eu.groupby_exprs.size() > 1 || *eu.groupby_exprs.begin() != nullptr) {
    LOG(ERROR) << "GroupBy is not supported in RelAlgExecutionUnitParser.";
    return false;
  }

  return true;
}

// Used for input columnVar collection and combination
class InputAnalyzer {
 public:
  InputAnalyzer(const std::vector<InputColDescriptor>& input_desc, OpPipeline& pipeline)
      : input_desc_(input_desc)
      , pipeline_(pipeline)
      , input_exprs_(input_desc.size(), nullptr) {}

  void run() {
    if (pipeline_.empty()) {
      return;
    }

    for (size_t i = 0; i < input_desc_.size(); ++i) {
      input_desc_to_index_.insert({input_desc_[i], i});
    }

    for (auto& op : pipeline_) {
      auto exprs = op->getExprs();
      for (auto& expr : exprs) {
        traverse(&expr);
      }
    }

    for (size_t i = 0; i < input_exprs_.size(); ++i) {
      if (nullptr == input_exprs_[i]) {
        LOG(FATAL) << "Input column expr missed. Column descriptor = "
                   << input_desc_[i].toString();
      }
    }

    pipeline_.insert(pipeline_.begin(), createOpNode<SourceNode>(input_exprs_));
  }

 private:
  void traverse(ExprPtr* curr) {
    if (auto col_var_ptr = dynamic_cast<Analyzer::ColumnVar*>(curr->get()); col_var_ptr) {
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
        traverse(child);
      }
    }
  }

  const std::vector<InputColDescriptor>& input_desc_;
  OpPipeline& pipeline_;
  ExprPtrVector input_exprs_;
  std::unordered_map<InputColDescriptor, size_t> input_desc_to_index_;
};

static void insertSourceNode(const RelAlgExecutionUnit& eu, OpPipeline& pipeline) {
  std::vector<InputColDescriptor> input_desc;
  input_desc.reserve(eu.input_col_descs.size());
  for (auto& desc : eu.input_col_descs) {
    input_desc.push_back(*desc);
  }

  InputAnalyzer analyzer(input_desc, pipeline);
  analyzer.run();
}

OpPipeline toOpPipeline(const RelAlgExecutionUnit& eu) {
  // TODO (bigPYJ1151): Only support filter and project now.
  // TODO (bigPYJ1151): Only naive expression dispatch now, need to analyze expression
  // trees and dispatch more fine-grained.
  if (!isParseable(eu)) {
    return {};
  }

  OpPipeline ops;

  ExprPtrVector filters;
  for (auto& filter_expr : eu.quals) {
    filters.push_back(filter_expr);
  }
  ops.emplace_back(createOpNode<operators::FilterNode>(filters));

  ExprPtrVector projs;
  for (auto& targets_expr : eu.shared_target_exprs) {
    projs.push_back(targets_expr);
  }
  ops.emplace_back(createOpNode<operators::ProjectNode>(projs));

  insertSourceNode(eu, ops);

  return ops;
}
}  // namespace cider::exec::nextgen::parsers