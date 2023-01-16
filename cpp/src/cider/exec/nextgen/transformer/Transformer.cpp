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
#include "exec/nextgen/transformer/Transformer.h"

#include "exec/nextgen/operators/QueryFuncInitializer.h"
#include "exec/nextgen/operators/OpNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/RowToColumnNode.h"

namespace cider::exec::nextgen::transformer {
using namespace operators;

// [Head, Tail], both sides are closed.
class PipelineStage {
 public:
  PipelineStage(const OpPipeline::iterator& head, const OpPipeline::iterator& tail)
      : head_(head), tail_(tail) {}

 private:
  OpPipeline::iterator head_;
  OpPipeline::iterator tail_;
};

static TranslatorPtr generateTranslators(OpPipeline& pipeline) {
  CHECK_GT(pipeline.size(), 0);

  OpNodePtr input = nullptr;
  std::for_each(pipeline.begin(), pipeline.end(), [&input](const OpNodePtr& op) {
    op->setInputOpNode(input);
    input = op;
  });

  TranslatorPtr ptr = nullptr;
  std::for_each(pipeline.rbegin(), pipeline.rend(), [&ptr](const OpNodePtr& op) {
    auto translator = op->toTranslator(ptr);
    ptr = translator;
  });

  return ptr;
}

TranslatorPtr Transformer::toTranslator(OpPipeline& pipeline) {
  CHECK(pipeline.size() > 1);
  CHECK(isa<QueryFuncInitializer>(pipeline.front()));

  std::vector<PipelineStage> stages;
  stages.reserve(pipeline.size());

  auto traverse_pivot = pipeline.begin();

  while (++traverse_pivot != pipeline.end()) {
    // Currently, auto-vectorize will be applied to pure project pipeline only.
    if (!isa<ProjectNode>(*traverse_pivot)) {
      stages.emplace_back(++pipeline.begin(), --pipeline.end());
      break;
    }

    OpNodePtr& curr_op = *traverse_pivot;
    auto&& [_, exprs] = curr_op->getOutputExprs();

    ExprPtrVector vectorizable_exprs;
    vectorizable_exprs.reserve(8);

    for (auto& expr : exprs) {
      if (expr->isAutoVectorizable()) {
        // pipeline.insert(traverse_pivot, );      
      }      
    }
  }

  auto c2r_node =
      createOpNode<ColumnToRowNode>(pipeline.front()->getOutputExprs().second);
  pipeline.insert(++pipeline.begin(), c2r_node);

  auto r2c_node =
      createOpNode<RowToColumnNode>(pipeline.back()->getOutputExprs().second,
                                    static_cast<ColumnToRowNode*>(c2r_node.get()));
  pipeline.emplace_back(r2c_node);

  return generateTranslators(pipeline);
}
}  // namespace cider::exec::nextgen::transformer
