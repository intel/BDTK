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

#ifndef NEXTGEN_OPERATORS_COLUMNTOROWNODE_H
#define NEXTGEN_OPERATORS_COLUMNTOROWNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class ColumnToRowNode : public OpNode {
 public:
  explicit ColumnToRowNode(ExprPtrVector&& output_exprs)
      : OpNode("ColumnToRowNode", std::move(output_exprs), JITExprValueType::ROW) {}

  explicit ColumnToRowNode(const ExprPtrVector& output_exprs)
      : OpNode("ColumnToRowNode", output_exprs, JITExprValueType::ROW) {}

  TranslatorPtr toTranslator(const TranslatorPtr& successor = nullptr) override;

  jitlib::JITValuePointer getColumnRowNum() { return column_row_num_; }

  void setColumnRowNum(jitlib::JITValuePointer& row_num) {
    CHECK(column_row_num_.get() == nullptr);
    column_row_num_.replace(row_num);
  }

  using DeferFunc = void (*)(void*);

  template <typename FuncT>
  void registerDeferFunc(FuncT&& func) {
    defer_func_list_.emplace_back(func);
  }

  std::vector<std::function<void()>>& getDeferFunctions() { return defer_func_list_; }

 private:
  jitlib::JITValuePointer column_row_num_;
  std::vector<std::function<void()>> defer_func_list_;
};

class ColumnToRowTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;
  void consumeNull(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context, bool for_null = false);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_COLUMNTOROWNODE_H
