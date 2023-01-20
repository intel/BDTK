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

#include "exec/nextgen/operators/VectorizedProjectNode.h"
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <type_traits>
#include <vector>

#include "exec/nextgen/utils/ExprUtils.h"

namespace cider::exec::nextgen::operators {

class UnionFindSet {
 public:
  UnionFindSet(size_t init_union_num) : parent_(init_union_num) {
    std::iota(parent_.begin(), parent_.end(), 0);
  }

  size_t find(size_t index) {
    size_t parent = index;
    while (parent != parent_[parent]) {
      parent = parent_[parent];
    }
    parent_[index] = parent;
    return parent;
  }

  // bool Criteria(size_t elem1, size_t elem2), returns true if elem1 has higher priority
  // to become a root.
  template <
      typename Criteria,
      typename std::enable_if_t<std::is_invocable_r_v<bool, Criteria, size_t, size_t>,
                                bool> = true>
  void connect(size_t elem1, size_t elem2, Criteria&& criteria) {
    elem1 = find(elem1);
    elem2 = find(elem2);
    if (elem1 != elem2) {
      if (!criteria(elem1, elem2)) {
        std::swap(elem1, elem2);
      }
      parent_[elem2] = elem1;
    }
  }

 private:
  std::vector<size_t> parent_;
};

TranslatorPtr VectorizedProjectNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<VectorizedProjectTranslator>(shared_from_this(), succ);
}

void VectorizedProjectTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void VectorizedProjectTranslator::codegen(context::CodegenContext& context) {
  auto&& [_, exprs] = node_->getOutputExprs();

  // Collect input ColumnVars of each output exprs respectively.
  std::vector<ExprPtrVector> input_columnvars(exprs.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    ExprPtrVector columnvars = utils::collectColumnVars({exprs[i]});
    std::sort(columnvars.begin(), columnvars.end());
    input_columnvars[i] = std::move(columnvars);
  }

  // Group output exprs and generate their code in same loops. If a input ColumnVars set
  // of a output expr is a subset of another one, the output exprs should be in a group.
  for (size_t i = 0; i < exprs.size(); ++i) {
    for (size_t j = i + 1; j < exprs.size(); ++j) {
    }
  }

  for (auto& expr : exprs) {
    expr->codegen(context);
  }
  successor_->consume(context);
}

}  // namespace cider::exec::nextgen::operators
