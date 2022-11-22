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

#ifndef NEXTGEN_OPERATORS_ColumnToRowNODE_H
#define NEXTGEN_OPERATORS_ColumnToRowNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class ColumnToRowNode : public OpNode {
 public:
  template <typename T, IsVecOf<T, ExprPtr> = true>
  ColumnToRowNode(T&& exprs) : exprs_(std::forward<T>(exprs)) {}

  template <typename... T>
  ColumnToRowNode(T&&... exprs) {
    (exprs_.emplace_back(std::forward<T>(exprs)), ...);
  }

  ExprPtrVector getExprs() override { return exprs_; }

  ExprPtrVector exprs_;
};

class ColumnToRowTranslator : public Translator {
 public:
  template <typename T>
  ColumnToRowTranslator(T&& exprs, std::unique_ptr<Translator> succ) {
    node_ = ColumnToRowNode(std::forward<T>(exprs));
    successor_.swap(succ);
  }

  template <typename... T>
  ColumnToRowTranslator(T&&... exprs, std::unique_ptr<Translator> successor) {
    node_ = ColumnToRowNode(std::forward<T>(exprs)...);
    successor_.swap(successor);
  }

  ColumnToRowTranslator(ColumnToRowNode&& node, std::unique_ptr<Translator>&& succ)
      : node_(std::move(node)), successor_(std::move(succ)) {}

  void consume(Context& context) override;

 private:
  void codegen(Context& context);

  void col2RowConvert(ExprPtrVector inputs, JITFunction* func, JITValuePointer index);

  ColumnToRowNode node_;
  std::unique_ptr<Translator> successor_;
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_ColumnToRowNODE_H
