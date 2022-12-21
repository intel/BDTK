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

#ifndef NEXTGEN_OPERATORS_AGGNODE_H
#define NEXTGEN_OPERATORS_AGGNODE_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
struct AggExprsInfo {
  jitlib::JITTypeTag jit_value_type_;
  SQLAgg agg_type_;
  int32_t start_offset_;
  int32_t byte_size_;
  std::string agg_name_;

  AggExprsInfo(SQLTypes sql_type,
               SQLAgg agg_type,
               int32_t start_offset,
               int32_t byte_size)
      : jit_value_type_(getJitValueType(sql_type))
      , agg_type_(agg_type)
      , start_offset_(start_offset)
      , byte_size_(byte_size)
      , agg_name_(getAggName(agg_type, sql_type)) {}

  jitlib::JITTypeTag getJitValueType(SQLTypes sql_type);

  std::string getAggName(SQLAgg agg_type, SQLTypes sql_type);
};

using AggExprsInfoVector = std::vector<AggExprsInfo>;

class AggNode : public OpNode {
 public:
  AggNode(ExprPtrVector&& groupby_exprs, ExprPtrVector&& output_exprs)
      : OpNode("AggNode", std::move(output_exprs), JITExprValueType::ROW)
      , groupby_exprs_(std::move(groupby_exprs)) {}

  AggNode(const ExprPtrVector& groupby_exprs, const ExprPtrVector& output_exprs)
      : OpNode("AggNode", output_exprs, JITExprValueType::ROW)
      , groupby_exprs_(std::move(groupby_exprs)) {}

  ExprPtrVector& getGroupByExprs() { return groupby_exprs_; }

  TranslatorPtr toTranslator(const TranslatorPtr& succ = nullptr) override;

 private:
  ExprPtrVector groupby_exprs_;
};

class AggTranslator : public Translator {
 public:
  explicit AggTranslator(const OpNodePtr& node, const TranslatorPtr& succ = nullptr)
      : Translator(node, succ) {}

  void consume(context::CodegenContext& context) override;

 private:
  void codegen(context::CodegenContext& context);
};

}  // namespace cider::exec::nextgen::operators
#endif  // NEXTGEN_OPERATORS_FILTERNODE_H
