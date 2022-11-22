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

#ifndef NEXTGEN_OPERATORS_FILTERNODE_H
#define NEXTGEN_OPERATORS_FILTERNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class FilterNode : public OpNode {
 public:
  FilterNode(ExprPtrVector&& output_exprs)
      : OpNode("FilterNode", std::move(output_exprs), JITExprValueType::ROW) {}

  FilterNode(const ExprPtrVector& output_exprs)
      : OpNode("FilterNode", output_exprs, JITExprValueType::ROW) {}

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;
};

class FilterTranslator : public Translator {
 public:
  FilterTranslator(const OpNodePtr& node, const TranslatorPtr& succ = nullptr)
      : Translator(node, succ) {}

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_FILTERNODE_H
