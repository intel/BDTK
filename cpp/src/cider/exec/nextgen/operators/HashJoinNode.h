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

#ifndef NEXTGEN_OPERATORS_HASHJOINNODE_H
#define NEXTGEN_OPERATORS_HASHJOINNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class HashJoinNode : public OpNode {
 public:
  HashJoinNode(ExprPtrVector&& output_exprs,
               ExprPtrVector&& join_quals,
               std::map<ExprPtr, size_t>&& build_table_map)
      : OpNode("HashJoinNode", std::move(output_exprs), JITExprValueType::ROW)
      , join_quals_(std::move(join_quals))
      , build_table_map_(std::move(build_table_map)) {}

  HashJoinNode(const ExprPtrVector& output_exprs,
               const ExprPtrVector& join_quals,
               std::map<ExprPtr, size_t>& build_table_map)
      : OpNode("HashJoinNode", output_exprs, JITExprValueType::ROW)
      , join_quals_(join_quals)
      , build_table_map_(build_table_map) {}

  ExprPtrVector getJoinQuals() { return join_quals_; }

  std::map<ExprPtr, size_t>& getBuildTableMap() { return build_table_map_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

 private:
  ExprPtrVector join_quals_;
  std::map<ExprPtr, size_t> build_table_map_;
};

class HashJoinTranslator : public Translator {
 public:
  using Translator::Translator;

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_HASHJOINNODE_H
