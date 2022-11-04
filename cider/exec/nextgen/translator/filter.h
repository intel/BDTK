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

#include "dummy.h"
#include "type/plan/Analyzer.h"

class FilterNode : public OpNode {
 public:
  //   FilterNode(const std::vector<ExprPtr>& exprs) : exprs_(exprs) {}
  //   FilterNode(std::vector<ExprPtr>&& exprs) : exprs_(std::move(exprs)) {}
  FilterNode(std::initializer_list<ExprPtr> exprs) : exprs_(exprs) {}
  template <typename T>
  FilterNode(T&& exprs) : exprs_(std::forward<T>(exprs)) {}

  std::vector<ExprPtr> exprs_;
};

class FilterTranslator : public Translator {
 public:
  template <typename T>
  FilterTranslator(T&& exprs) {
    filterNode_ = std::make_unique<FilterNode>(std::forward<T>(exprs));
  }
  //   FilterTranslator(const std::vector<ExprPtr>& exprs) {
  //     filterNode_ = std::make_unique<FilterNode>(exprs);
  //   }
  //   FilterTranslator(std::vector<ExprPtr>&& exprs) {
  //     filterNode_ = std::make_unique<FilterNode>(std::move(exprs));
  //   }
  FilterTranslator(std::initializer_list<ExprPtr> exprs) {
    filterNode_ = std::make_unique<FilterNode>(exprs);
  }

  void consume(Context& context, const JITTuple& input) override;

 private:
  JITTuple codegen(Context& context, const JITTuple& input);

  std::unique_ptr<FilterNode> filterNode_;
};

#endif
