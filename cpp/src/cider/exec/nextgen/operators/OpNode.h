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

#ifndef NEXTGEN_OPERATORS_OPNODE_H
#define NEXTGEN_OPERATORS_OPNODE_H

#include "exec/nextgen/context/CodegenContext.h"
#include "type/plan/Analyzer.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
using utils::JITExprValueType;

class Translator;
class OpNode;

using OpNodePtr = std::shared_ptr<OpNode>;
using OpPipeline = std::list<OpNodePtr>;
using ExprPtr = std::shared_ptr<Analyzer::Expr>;
using ExprPtrVector = std::vector<ExprPtr>;
using TranslatorPtr = std::shared_ptr<Translator>;

/// \brief A OpNode is a relational operation in a plan
///
/// Note: Each OpNode has zero or one input
class OpNode : public std::enable_shared_from_this<OpNode> {
 public:
  template <typename OutputVecT>
  OpNode(const char* name = "None",
         OutputVecT&& output_exprs = {},
         JITExprValueType output_type = JITExprValueType::ROW)
      : name_(name)
      , output_exprs_(std::forward<OutputVecT>(output_exprs))
      , output_type_(output_type) {}

  virtual ~OpNode() = default;

  /// \brief The name of the operator node
  const char* name() const { return name_; }

  // void setInputOpNode(const OpNodePtr& input) { input_ = input; }

  std::pair<JITExprValueType, ExprPtrVector&> getOutputExprs() {
    return {output_type_, output_exprs_};
  }

  /// \brief Transform the operator to a translator
  virtual TranslatorPtr toTranslator(const TranslatorPtr& successor) = 0;

 protected:
  // OpNodePtr input_;
  const char* name_;
  ExprPtrVector output_exprs_;
  JITExprValueType output_type_;
};

// TBD: Combine OpNode and Translator
class Translator {
 public:
  explicit Translator(const OpNodePtr& node, const TranslatorPtr& successor = nullptr)
      : node_(node), successor_(successor) {}

  virtual ~Translator() = default;

  virtual void consume(context::CodegenContext& context) = 0;

  OpNodePtr getOpNode() { return node_; }

  TranslatorPtr setSuccessor(const TranslatorPtr& successor) {
    successor_ = successor;
    return successor_;
  }

  TranslatorPtr getSuccessor() const { return successor_; }

  using SuccessorEmitter = void(void*, context::CodegenContext&);

  template <typename T,
            typename std::enable_if_t<std::is_invocable_v<T, context::CodegenContext&>,
                                      bool> = true>
  void codegen(context::CodegenContext& context, T&& successor) {
    auto successor_wrapper = [](void* successor_ptr, context::CodegenContext& context) {
      auto actual_builder = reinterpret_cast<T*>(successor_ptr);
      (*actual_builder)(context);
    };

    return codegenImpl(successor_wrapper, context, (void*)&successor);
  }

 private:
  // TODO (bigPYJ1151) : Change to pure virtual function.
  virtual void codegenImpl(SuccessorEmitter successor_wrapper,
                           context::CodegenContext& context,
                           void* successor) {
    UNREACHABLE();
  }

 protected:
  OpNodePtr node_;
  TranslatorPtr successor_;
};

template <typename OpNodeT, typename... Args>
inline typename std::enable_if_t<std::is_base_of<OpNode, OpNodeT>::value,
                                 std::shared_ptr<OpNodeT>>
createOpNode(Args&&... args) {
  return std::make_shared<OpNodeT>(std::forward<Args>(args)...);
}

template <typename OpTranslatorT, typename... Args>
inline typename std::enable_if_t<std::is_base_of<Translator, OpTranslatorT>::value,
                                 std::shared_ptr<Translator>>
createOpTranslator(Args&&... args) {
  return std::make_shared<OpTranslatorT>(std::forward<Args>(args)...);
}

template <typename OpNodeT>
bool isa(const OpNodePtr& op) {
  return dynamic_cast<OpNodeT*>(op.get());
}

}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_OPERATORS_OPNODE_H
