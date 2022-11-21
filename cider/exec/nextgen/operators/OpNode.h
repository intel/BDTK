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

#include <type_traits>

#include "exec/nextgen/Context.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::operators {
class Translator;
class OpNode;

using OpNodePtr = std::shared_ptr<OpNode>;
using OpPipeline = std::vector<OpNodePtr>;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;
using ExprPtrVector = std::vector<ExprPtr>;
using TranslatorPtr = std::shared_ptr<Translator>;

enum class JITExprValueType { ROW, BATCH };

/// \brief A OpNode is a relational operation in a plan
///
/// Note: Each OpNode has zero or one source
class OpNode : public std::enable_shared_from_this<OpNode> {
 public:
  OpNode(const char* name = "None",
         const OpNodePtr& prev = nullptr,
         const ExprPtrVector& output_exprs = {},
         JITExprValueType output_type = JITExprValueType::ROW)
      : input_(prev)
      , name_(name)
      , output_exprs_(output_exprs)
      , output_type_(output_type) {}

  virtual ~OpNode() = default;

  /// \brief The name of the operator node
  const char* name() const { return name_; }

  void setInputOpNode(const OpNodePtr& prev) { input_ = prev; }

  std::pair<JITExprValueType, ExprPtrVector&> getOutputExprs() {
    return {output_type_, output_exprs_};
  }

  /// \brief Transform the operator to a translator
  virtual TranslatorPtr toTranslator(const TranslatorPtr& succ) = 0;

 protected:
  OpNodePtr input_;
  const char* name_;
  ExprPtrVector output_exprs_;
  JITExprValueType output_type_;
};

class Translator {
 public:
  Translator(const OpNodePtr& op_node, const TranslatorPtr& successor = nullptr)
      : op_node_(op_node), new_successor_(successor) {}

  virtual ~Translator() = default;

  virtual void consume(Context& context) = 0;

  OpNodePtr getOpNode() { return op_node_; }

  TranslatorPtr setSuccessor(const TranslatorPtr& successor) {
    new_successor_ = successor;
    return new_successor_;
  }

  TranslatorPtr getSuccessor() const { return new_successor_; }

 protected:
  OpNodePtr op_node_;
  TranslatorPtr new_successor_;
};

template <typename OpNodeT, typename... Args>
OpNodePtr createOpNode(Args&&... args) {
  return std::make_shared<OpNodeT>(std::forward<Args>(args)...);
}

template <typename OpTranslatorT, typename... Args>
TranslatorPtr createOpTranslator(Args&&... args) {
  return std::make_shared<OpTranslatorT>(std::forward<Args>(args)...);
}

template <typename OpNodeT>
bool isa(const OpNodePtr& op) {
  return dynamic_cast<OpNodeT*>(op.get());
}
}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_TRANSLATOR_OPNODE_H
