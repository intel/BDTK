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
#ifndef NEXTGEN_OPERATORS_SOURCENODE_H
#define NEXTGEN_OPERATORS_SOURCENODE_H

#include <vector>

#include "exec/nextgen/operators/OpNode.h"
#include "util/Logger.h"

class InputColDescriptor;
namespace cider::exec::nextgen::operators {

class SourceNode : public OpNode {
 public:
  template <typename T, IsVecOf<T, ExprPtr> = true>
  SourceNode(T&& exprs) : input_cols_(std::forward<T>(exprs)) {}

  template <typename... T>
  SourceNode(T&&... exprs) {
    (input_cols_.emplace_back(std::forward<T>(exprs)), ...);
  }

  ExprPtrVector getOutputExprs() override { return input_cols_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

 private:
  ExprPtrVector input_cols_;
};

class SourceTranslator : public Translator {
 public:
  template <typename T>
  SourceTranslator(T&& exprs, std::unique_ptr<Translator> succ) {
    node_ = SourceNode(std::forward<T>(exprs));
    successor_.swap(succ);
  }

  template <typename... T>
  SourceTranslator(T&&... exprs, std::unique_ptr<Translator> successor) {
    node_ = SourceNode(std::forward<T>(exprs)...);
    successor_.swap(successor);
  }

  SourceTranslator(const OpNodePtr& node, const TranslatorPtr& succ = nullptr)
      : Translator(node, succ) {
    CHECK(isa<SourceNode>(node));
  }

  void consume(Context& context) override;

 private:
  void codegen(Context& context);

  SourceNode node_;
  std::unique_ptr<Translator> successor_;
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_SOURCENODE_H
