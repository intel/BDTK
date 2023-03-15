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

#include "exec/nextgen/operators/StageNode.h"

#include "exec/nextgen/operators/ColumnToRowNode.h"
#include "exec/nextgen/operators/RowToColumnNode.h"
#include "exec/nextgen/utils/ExprUtils.h"

namespace cider::exec::nextgen::operators {
TranslatorPtr StageNode::toTranslator(const TranslatorPtr& succ) {
  // OpNodePtr input = prev_;
  // std::for_each(begin(), end(), [&input](const OpNodePtr& op) {
  //   op->setInputOpNode(input);
  //   input = op;
  // });

  TranslatorPtr ptr = nullptr;
  std::for_each(
      stage_pipeline_.rbegin(), stage_pipeline_.rend(), [&ptr](const OpNodePtr& op) {
        auto translator = op->toTranslator(ptr);
        ptr = translator;
      });

  stage_translator_ = ptr;
  return createOpTranslator<StageTranslator>(shared_from_this(), succ);
}

void StageTranslator::consume(context::CodegenContext& context) {
  codegen(context);
}

void StageTranslator::codegen(context::CodegenContext& context) {
  auto node = dynamic_cast<StageNode*>(node_.get());

  auto c2r_node = createOpNode<ColumnToRowNode>(collectColumnVar());
  auto c2r_translator = c2r_node->toTranslator();

  auto tail = --node->end();
  auto&& [_, exprs] = tail->get()->getOutputExprs();
  auto r2c_node = createOpNode<RowToColumnNode>(exprs, c2r_node.get());
  auto r2c_translator = r2c_node->toTranslator();

  c2r_translator->codegen(context,
                          [&node, &r2c_translator](context::CodegenContext& context) {
                            node->getStageTranslator()->consume(context);
                            r2c_translator->consume(context);
                          });
  if (successor_) {
    successor_->consume(context);
  }
}

ExprPtrVector StageTranslator::collectColumnVar() {
  auto node = dynamic_cast<StageNode*>(node_.get());

  ExprPtrVector stage_exprs;
  stage_exprs.reserve(8);

  for (auto iter = node->begin(); iter != node->end(); ++iter) {
    auto&& [_, exprs] = iter->get()->getOutputExprs();
    stage_exprs.insert(stage_exprs.end(), exprs.begin(), exprs.end());
  }

  return utils::collectColumnVars(stage_exprs);
}

}  // namespace cider::exec::nextgen::operators
