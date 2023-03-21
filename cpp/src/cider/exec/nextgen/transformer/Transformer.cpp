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

#include "exec/nextgen/operators/ColumnToRowNode.h"
#include "exec/nextgen/operators/FilterNode.h"
#include "exec/nextgen/operators/LazyNode.h"
#include "exec/nextgen/operators/OpNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/operators/QueryFuncInitializer.h"
#include "exec/nextgen/operators/RowToColumnNode.h"
#include "exec/nextgen/operators/StageNode.h"
#include "exec/nextgen/operators/VectorizedFilterNode.h"
#include "exec/nextgen/operators/VectorizedProjectNode.h"
#include "exec/nextgen/utils/ExprUtils.h"

namespace cider::exec::nextgen::transformer {
using namespace operators;

static TranslatorPtr generateTranslators(OpPipeline& pipeline) {
  CHECK_GT(pipeline.size(), 0);

  TranslatorPtr ptr = nullptr;
  std::for_each(pipeline.rbegin(), pipeline.rend(), [&ptr](const OpNodePtr& op) {
    auto translator = op->toTranslator(ptr);
    ptr = translator;
  });

  return ptr;
}

TranslatorPtr Transformer::toTranslator(OpPipeline& pipeline, const CodegenOptions& co) {
  CHECK_GT(pipeline.size(), 1);
  CHECK(isa<QueryFuncInitializer>(pipeline.front()));

  OpPipeline stagedPipeline;
  // std::vector<PipelineStage> stages;
  // stages.reserve(pipeline.size());

  // Vectorize Project and Filter Transformation
  // Currently, auto-vectorize will be applied to pure project pipelines or filter
  // pipelines.
  auto traverse_pivot = ++pipeline.begin();
  if (co.enable_vectorize &&
      (isa<ProjectNode>(*traverse_pivot) || isa<FilterNode>(*traverse_pivot))) {
    bool has_filter = isa<FilterNode>(*traverse_pivot);
    OpNodePtr& curr_op = *traverse_pivot;
    auto&& [_, exprs] = curr_op->getOutputExprs();
    ExprPtrVector vectorizable_exprs;
    vectorizable_exprs.reserve(exprs.size());

    for (auto& expr : exprs) {
      if (dynamic_cast<Analyzer::OutputColumnVar*>(expr.get())) {
        // ignore bare columns
        continue;
      }
      if (expr->isAutoVectorizable()) {
        // Move vectorizable exprs out of row-based ProjectNode.
        vectorizable_exprs.emplace_back(expr);
        expr.reset();
      }
    }
    exprs.erase(std::remove_if(exprs.begin(),
                               exprs.end(),
                               [](ExprPtr& expr) -> bool { return expr == nullptr; }),
                exprs.end());
    if (exprs.empty()) {
      traverse_pivot = pipeline.erase(traverse_pivot);
    }

    if (!vectorizable_exprs.empty()) {
      OpNodePtr vec_node;
      if (has_filter) {
        vec_node = createOpNode<VectorizedFilterNode>(vectorizable_exprs);
      } else {
        vec_node = createOpNode<VectorizedProjectNode>(vectorizable_exprs);
      }

      pipeline.insert(traverse_pivot, vec_node);
    }
  }

  if (traverse_pivot != pipeline.end()) {
    auto tail = --pipeline.end();
    if (dynamic_cast<LazyNode*>(tail->get())) {
      // stages.emplace_back(traverse_pivot, --tail);
      OpPipeline stage_pipeline;
      stage_pipeline.splice(stage_pipeline.begin(), pipeline, traverse_pivot, tail);
      auto stage_node = createOpNode<StageNode>(stage_pipeline);
      pipeline.insert(tail, stage_node);
    } else {
      OpPipeline stage_pipeline;
      stage_pipeline.splice(
          stage_pipeline.begin(), pipeline, traverse_pivot, pipeline.end());
      auto stage_node = createOpNode<StageNode>(stage_pipeline);
      pipeline.insert(pipeline.end(), stage_node);
    }
  }

  return generateTranslators(pipeline);
}
}  // namespace cider::exec::nextgen::transformer
