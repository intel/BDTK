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
#ifndef NEXTGEN_OPERATORS_STAGETNODE_H
#define NEXTGEN_OPERATORS_STAGETNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class StageNode : public OpNode {
 public:
  // explicit StageNode(OpPipeline pipeline, OpNodePtr prev, OpNodePtr next)
  explicit StageNode(OpPipeline pipeline)
      : OpNode("StageNode", ExprPtrVector{}, JITExprValueType::BATCH)
      , stage_pipeline_(pipeline) {}
  // , prev_(prev)
  // , next_(next) {}

  // explicit StageNode(const ExprPtrVector& output_exprs)
  //     : OpNode("StageNode", ExprPtrVector{}, JITExprValueType::BATCH) {}

  TranslatorPtr toTranslator(const TranslatorPtr& successor = nullptr) override;
  OpPipeline::iterator begin() { return stage_pipeline_.begin(); }
  OpPipeline::iterator end() { return stage_pipeline_.end(); }
  TranslatorPtr getStageTranslator() { return stage_translator_; }

 private:
  OpPipeline stage_pipeline_;
  TranslatorPtr stage_translator_ = nullptr;
  // OpNodePtr prev_;
  // OpNodePtr next_;
};

class StageTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
  ExprPtrVector collectColumnVar();
};
}  // namespace cider::exec::nextgen::operators
#endif