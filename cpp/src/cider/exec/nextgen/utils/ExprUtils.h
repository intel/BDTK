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
#ifndef NEXTGEN_UTILS_EXPRUTILS_H
#define NEXTGEN_UTILS_EXPRUTILS_H

#include <cstddef>
#include <unordered_set>

#include "exec/nextgen/utils/FunctorUtils.h"
#include "type/plan/Analyzer.h"
#include "type/plan/Expr.h"

namespace cider::exec::nextgen::operators {
using ExprPtr = std::shared_ptr<Analyzer::Expr>;
using ExprPtrVector = std::vector<ExprPtr>;
}  // namespace cider::exec::nextgen::operators

namespace cider::exec::nextgen::utils {

// Collect all leaf ColumnVar Exprs in a expr tree.
inline operators::ExprPtrVector collectColumnVars(const operators::ExprPtrVector& exprs) {
  operators::ExprPtrVector outputs;
  outputs.reserve(8);
  std::unordered_set<Analyzer::Expr*> expr_record;

  RecursiveFunctor traverser{
      [&outputs, &expr_record](auto&& traverser, const operators::ExprPtr& expr) {
        CHECK(expr);
        if (dynamic_cast<Analyzer::ColumnVar*>(expr.get())) {
          if (expr_record.find(expr.get()) == expr_record.end()) {
            expr_record.insert(expr.get());
            outputs.emplace_back(expr);
          }
          return;
        }

        auto&& children = expr->get_children_reference();
        for (auto&& child : children) {
          traverser(*child);
        }
        return;
      }};
  std::for_each(exprs.begin(), exprs.end(), traverser);

  return outputs;
}

// Check whether a is a subset of b. Both inputs should be sorted.
inline bool isSubsetExprVector(const operators::ExprPtrVector& a,
                               const operators::ExprPtrVector& b) {
  if (a.size() <= b.size()) {
    size_t a_index = 0;
    for (size_t b_index = 0; b_index < b.size(); ++b_index) {
      if (a[a_index] == b[b_index]) {
        ++a_index;
        if (a.size() == a_index) {
          return true;
        }
      }
    }
  }
  return false;
}
}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_EXPRUTILS_H
