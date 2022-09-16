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
 * @file    PlanRelVisitor.h
 * @brief   Plan Rel visitor
 **/

#pragma once

#include "PlanContext.h"
#include "RelVisitor.h"

namespace generator {

class AggRelVisitor : public RelVisitor {
 public:
  AggRelVisitor(const substrait::AggregateRel& rel_node,
                Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                const std::unordered_map<int, std::string>& function_map,
                std::shared_ptr<VariableContext> variable_context_shared_ptr,
                bool is_right_join_node);

  virtual ~AggRelVisitor();

  // agg、read、project
  virtual void visit(TargetContext* target_context);
  // agg
  virtual void visit(GroupbyContext* groupby_context);

 private:
  const substrait::AggregateRel& rel_node_;
};

class FilterRelVisitor : public RelVisitor {
 public:
  FilterRelVisitor(const substrait::FilterRel& rel_node,
                   Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                   const std::unordered_map<int, std::string>& function_map,
                   std::shared_ptr<VariableContext> variable_context_shared_ptr,
                   bool is_right_join_node);

  virtual ~FilterRelVisitor();

  // filter
  virtual void visit(FilterQualContext* filter_qual_context);

 private:
  const substrait::FilterRel& rel_node_;
};

class JoinRelVisitor : public RelVisitor {
 public:
  JoinRelVisitor(const substrait::JoinRel& rel_node,
                 Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                 const std::unordered_map<int, std::string>& function_map,
                 std::shared_ptr<VariableContext> variable_context_shared_ptr,
                 bool is_right_join_node);

  virtual ~JoinRelVisitor();

  // join
  virtual void visit(JoinQualContext* join_qual_context);

 private:
  const substrait::JoinRel& rel_node_;
};

class ProjectRelVisitor : public RelVisitor {
 public:
  ProjectRelVisitor(const substrait::ProjectRel& rel_node,
                    Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                    const std::unordered_map<int, std::string>& function_map,
                    std::shared_ptr<VariableContext> variable_context_shared_ptr,
                    bool is_right_join_node);

  virtual ~ProjectRelVisitor();

  // agg、read、project
  virtual void visit(TargetContext* target_context);

 private:
  const substrait::ProjectRel& rel_node_;
};

class ReadRelVisitor : public RelVisitor {
 public:
  ReadRelVisitor(const substrait::ReadRel& rel_node,
                 Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                 const std::unordered_map<int, std::string>& function_map,
                 std::shared_ptr<VariableContext> variable_context_shared_ptr,
                 std::vector<CiderTableSchema>* input_table_schemas_ptr,
                 bool is_right_join_node);
  virtual ~ReadRelVisitor();

  // read
  virtual void visit(InputDescContext* input_desc_context);
  // agg、read、project
  virtual void visit(TargetContext* target_context);

 private:
  const substrait::ReadRel& rel_node_;
  std::vector<CiderTableSchema>* input_table_schemas_ptr_ = nullptr;
};

class SortRelVisitor : public RelVisitor {
 public:
  SortRelVisitor(const substrait::SortRel& rel_node,
                 Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                 const std::unordered_map<int, std::string>& function_map,
                 std::shared_ptr<VariableContext> variable_context_shared_ptr,
                 bool is_right_join_node);
  virtual ~SortRelVisitor();

  // sort
  virtual void visit(OrderEntryContext* order_entry_context);

 private:
  const substrait::SortRel& rel_node_;
};

}  // namespace generator
