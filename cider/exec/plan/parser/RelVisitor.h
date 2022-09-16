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

/**
 * @file    RelVisitor.h
 * @brief   Rel visitor
 **/

#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <unordered_map>
#include <vector>
#include "BaseContext.h"
#include "GeneratorContext.h"
#include "SubstraitToAnalyzerExpr.h"
#include "SubstraitToRelAlgExecutionUnit.h"
#include "TypeUtils.h"
#include "VariableContext.h"
#include "cider/CiderTableSchema.h"
#include "exec/plan/parser/Translator.h"
#include "exec/template/QueryHint.h"
#include "exec/template/common/descriptors/ColSlotContext.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "substrait/algebra.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "type/plan/Analyzer.h"
#include "type/schema/ColumnInfo.h"

namespace generator {
class InputDescContext;
class GroupbyContext;
class JoinQualContext;
class TargetContext;
class FilterQualContext;
class OrderEntryContext;

class RelVisitor {
 public:
  RelVisitor(Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
             const std::unordered_map<int, std::string>& function_map,
             std::shared_ptr<VariableContext> variable_context_shared_ptr,
             bool is_right_join_node);
  virtual ~RelVisitor();
  // read
  virtual void visit(InputDescContext* input_desc_context);
  // agg、read、project
  virtual void visit(TargetContext* target_context);
  // agg
  virtual void visit(GroupbyContext* groupby_context);
  // filter
  virtual void visit(FilterQualContext* filter_qual_context);
  // join
  virtual void visit(JoinQualContext* join_qual_context);
  // sort
  virtual void visit(OrderEntryContext* order_entry_context);

 protected:
  Substrait2AnalyzerExprConverter* toAnalyzerExprConverter_ = nullptr;
  std::shared_ptr<VariableContext> variable_context_shared_ptr_;
  const std::unordered_map<int, std::string>& function_map_;
  bool is_right_join_node_ = false;
};

}  // namespace generator
