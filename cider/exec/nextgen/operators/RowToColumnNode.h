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

#ifndef NEXTGEN_OPERATORS_ROWTOCOLUMNNODE_H
#define NEXTGEN_OPERATORS_ROWTOCOLUMNNODE_H

#include "exec/nextgen/operators/OpNode.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "type/plan/Expr.h"

namespace cider::exec::nextgen::operators {

class RowToColumnNode : public OpNode {
 public:
  template <typename T, IsVecOf<T, ExprPtr> = true>
  RowToColumnNode(T&& exprs) : exprs_(std::forward<T>(exprs)) {}

  template <typename... T>
  RowToColumnNode(T&&... exprs) {
    (exprs_.emplace_back(std::forward<T>(exprs)), ...);
  }

  ExprPtrVector getOutputExprs() override { return exprs_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

  ExprPtrVector exprs_;
};

class RowToColumnTranslator : public Translator {
 public:
  template <typename T>
  RowToColumnTranslator(T&& exprs, std::unique_ptr<Translator> succ) {
    node_ = RowToColumnNode(std::forward<T>(exprs));
    successor_.swap(succ);
  }

  template <typename... T>
  RowToColumnTranslator(T&&... exprs, std::unique_ptr<Translator>&& succ) {
    node_ = RowToColumnNode(std::forward<T>(exprs)...);
    successor_.swap(succ);
  }

  RowToColumnTranslator(const OpNodePtr& node, const TranslatorPtr& succ = nullptr)
      : Translator(node, succ) {}

  void consume(Context& context) override;

 private:
  void codegen(Context& context);

  // Row to ArrowArray sink
  RowToColumnNode node_;
  std::unique_ptr<Translator> successor_;
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_ROWTOCOLUMNNODE_H
