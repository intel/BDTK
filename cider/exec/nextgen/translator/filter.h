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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_FILTER_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_FILTER_H

#include <initializer_list>
#include <memory>

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/translator/dummy.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::translator {
class FilterNode : public OpNode {
 public:
  FilterNode() = default;
  template <typename T>
  FilterNode(T&& exprs) : exprs_(std::forward<T>(exprs)) {}
  // FilterNode(std::initializer_list<ExprPtr> exprs) : exprs_(exprs) {}

  std::vector<ExprPtr> exprs_;
};

class FilterTranslator : public Translator {
 public:
  template <typename T>
  FilterTranslator(T&& exprs, std::unique_ptr<Translator> successor) {
    node_ = FilterNode(std::forward<T>(exprs));
    successor_.swap(successor);
  }
  // FilterTranslator(std::initializer_list<ExprPtr> exprs,
  //                  std::unique_ptr<Translator> successor) {
  //   node_ = FilterNode(exprs);
  //   successor_.swap(successor);
  // }
  FilterTranslator(FilterNode&& node, std::unique_ptr<Translator>&& successor)
      : node_(std::move(node)), successor_(std::move(successor)) {}

  void consume(Context& context) override;

 private:
  void codegen(Context& context);

  FilterNode node_;
  std::unique_ptr<Translator> successor_;
};

}  // namespace cider::exec::nextgen::translator
#endif
