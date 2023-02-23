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

#ifndef NEXTGEN_OPERATORS_CROSSJOINNODE_H
#define NEXTGEN_OPERATORS_CROSSJOINNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class CrossJoinNode : public OpNode {
 public:
  CrossJoinNode(ExprPtrVector&& output_exprs, std::map<ExprPtr, size_t>&& build_table_map)
      : OpNode("CrossJoinNode", std::move(output_exprs), JITExprValueType::ROW)
      , build_table_map_(std::move(build_table_map)) {}

  CrossJoinNode(const ExprPtrVector& output_exprs,
                std::map<ExprPtr, size_t>& build_table_map)
      : OpNode("CrossJoinNode", output_exprs, JITExprValueType::ROW)
      , build_table_map_(build_table_map) {}

  std::map<ExprPtr, size_t>& getBuildTableMap() { return build_table_map_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

 private:
  std::map<ExprPtr, size_t> build_table_map_;
};

class CrossJoinTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_CROSSJOINNODE_H
