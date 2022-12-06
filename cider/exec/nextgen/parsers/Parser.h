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

#ifndef NEXTGEN_PARSERS_PARSER_H
#define NEXTGEN_PARSERS_PARSER_H

#include "exec/nextgen/operators/OpNode.h"
#include "exec/template/RelAlgExecutionUnit.h"

namespace cider::exec::nextgen::parsers {
using namespace cider::exec::nextgen::operators;

/// \brief A parser convert from the plan fragment to an OpPipeline
// source--> filter --> project --> agg --> ... -->sink
operators::OpPipeline toOpPipeline(const RelAlgExecutionUnit& eu);

/// \brief A parser convert from the expression to an OpPipeline
// source--> project -->sink
operators::OpPipeline toOpPipeline(const operators::ExprPtrVector& expr,
                                   const std::vector<InputColDescriptor>& input_descs);

// Used for input columnVar collection and combination
template <typename SourceNodeType>
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
      auto&& [type, exprs] = op->getOutputExprs();
      for (auto& expr : exprs) {
        traverse(&expr);
      }
    }

    input_exprs_.erase(
        std::remove_if(input_exprs_.begin(),
                       input_exprs_.end(),
                       [](const ExprPtr& input_expr) { return input_expr == nullptr; }),
        input_exprs_.end());

    pipeline_.insert(pipeline_.begin(), createOpNode<SourceNodeType>(input_exprs_));
  }

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
        traverse(child);
      }
    }
  }

  const std::vector<InputColDescriptor>& input_desc_;
  OpPipeline& pipeline_;
  ExprPtrVector input_exprs_;
  std::unordered_map<InputColDescriptor, size_t> input_desc_to_index_;
};
}  // namespace cider::exec::nextgen::parsers

#endif  // NEXTGEN_PARSERS_PARSER_H
