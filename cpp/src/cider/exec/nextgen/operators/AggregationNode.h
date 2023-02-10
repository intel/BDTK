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

#ifndef NEXTGEN_OPERATORS_AGGNODE_H
#define NEXTGEN_OPERATORS_AGGNODE_H

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
using AggExprsInfoVector = std::vector<context::AggExprsInfo>;

class AggNode : public OpNode {
 public:
  AggNode(ExprPtrVector&& groupby_exprs, ExprPtrVector&& output_exprs)
      : OpNode("AggNode", std::move(output_exprs), JITExprValueType::ROW)
      , groupby_exprs_(std::move(groupby_exprs)) {}

  AggNode(const ExprPtrVector& groupby_exprs, const ExprPtrVector& output_exprs)
      : OpNode("AggNode", output_exprs, JITExprValueType::ROW)
      , groupby_exprs_(groupby_exprs) {}

  ExprPtrVector& getGroupByExprs() { return groupby_exprs_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

 private:
  ExprPtrVector groupby_exprs_;
};

class AggTranslator : public Translator {
 public:
  explicit AggTranslator(const OpNodePtr& node, const TranslatorPtr& succ = nullptr)
      : Translator(node, succ) {}

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_FILTERNODE_H
