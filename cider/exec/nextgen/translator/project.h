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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_PROJECT_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_PROJECT_H

#include <initializer_list>
#include <memory>

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/translator/dummy.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::translator {
class ProjectNode : public OpNode {
 public:
  ProjectNode() = default;
  template <typename T>
  ProjectNode(T&& exprs) : exprs_(std::forward<T>(exprs)) {}
  // ProjectNode(std::initializer_list<ExprPtr> exprs) : exprs_(exprs) {}

  std::vector<ExprPtr> exprs_;
};

class ProjectTranslator : public Translator {
 public:
  template <typename T>
  ProjectTranslator(T&& exprs) {
    node_ = ProjectNode(std::forward<T>(exprs));
  }
  // ProjectTranslator(std::initializer_list<ExprPtr> exprs) { node_ = ProjectNode(exprs);
  // }

  void consume(Context& context) override;

 private:
  void codegen(Context& context);

  ProjectNode node_;
};

}  // namespace cider::exec::nextgen::translator
#endif
