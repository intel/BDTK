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

#ifndef NEXTGEN_OPERATORS_COLUMNTOROWNODE_H
#define NEXTGEN_OPERATORS_COLUMNTOROWNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class ColumnToRowNode : public OpNode {
 public:
  explicit ColumnToRowNode(ExprPtrVector&& output_exprs, bool vectorizable = false)
      : OpNode("ColumnToRowNode", std::move(output_exprs), JITExprValueType::ROW)
      , vectorizable_(vectorizable) {}

  explicit ColumnToRowNode(const ExprPtrVector& output_exprs, bool vectorizable = false)
      : OpNode("ColumnToRowNode", output_exprs, JITExprValueType::ROW)
      , vectorizable_(vectorizable) {}

  TranslatorPtr toTranslator(const TranslatorPtr& successor = nullptr) override;

  bool isVectorizable() const { return vectorizable_; }

 private:
  jitlib::JITValuePointer column_row_num_;
  std::vector<std::function<void()>> defer_func_list_;
  bool vectorizable_;
};

class ColumnToRowTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;

 private:
  void codegenImpl(SuccessorEmitter successor_wrapper,
                   context::CodegenContext& context,
                   void* successor) override;
};
}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_COLUMNTOROWNODE_H
