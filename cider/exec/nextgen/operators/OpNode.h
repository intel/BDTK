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

#ifndef NEXTGEN_TRANSLATOR_OPNODE_H
#define NEXTGEN_TRANSLATOR_OPNODE_H

#include "exec/nextgen/Context.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::operators {
class Translator;
class OpNode;

using OpNodePtr = std::shared_ptr<OpNode>;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;
using ExprPtrVector = std::vector<ExprPtr>;

/// \brief A OpNode is a relational operation in a plan
///
/// Note: Each OpNode has zero or one source
class OpNode : protected std::enable_shared_from_this<OpNode> {
 public:
  OpNode(const char* name = "None", const OpNodePtr& prev = nullptr)
      : input_(prev), name_(name) {}

  virtual ~OpNode() = default;

  /// \brief The name of the operator node
  const char* name() const { return name_; }

  void setInputOpNode(const OpNodePtr& prev) { input_ = prev; }

  virtual ExprPtrVector getExprs() = 0;

  /// \brief Transform the operator to a translator
  // virtual std::shared_ptr<Translator> toTranslator() const = 0;

 protected:
  OpNodePtr input_;
  const char* name_;
  // schema
};

class Translator {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

  Translator(const OpNodePtr& op_node = nullptr) : op_node_(op_node) {}

  virtual ~Translator() = default;

  virtual void consume(Context& context) = 0;

 protected:
  OpNodePtr op_node_;
};

using OpPipeline = std::vector<OpNodePtr>;

template <typename OpNodeT, typename... Args>
OpNodePtr createOpNode(Args&&... args) {
  return std::make_shared<OpNodeT>(std::forward<Args>(args)...);
}

template <typename OpNodeT>
bool isa(const OpNodePtr& op) {
  return dynamic_cast<OpNodeT*>(op.get());
}

}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_TRANSLATOR_OPNODE_H
