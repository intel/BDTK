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
 * @file    PlanContext.h
 * @brief   Plan context element
 **/

#pragma once

#include "BaseContext.h"
#include "RelVisitor.h"

namespace generator {

class FilterQualContext : public BaseContext {
 public:
  virtual ~FilterQualContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  std::list<std::shared_ptr<Analyzer::Expr>>* getSimpleQuals();
  std::list<std::shared_ptr<Analyzer::Expr>>* getQuals();

 private:
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals_;
  std::list<std::shared_ptr<Analyzer::Expr>> quals_;
};

class GroupbyContext : public BaseContext {
 public:
  virtual ~GroupbyContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  std::vector<std::shared_ptr<Analyzer::Expr>>* getGroupbyExprs();
  void setHasAgg(bool has_agg);
  bool getHasAgg() const;
  void setIsPartialAvg(bool is_partial_avg);
  bool getIsPartialAvg() const;

 private:
  std::vector<std::shared_ptr<Analyzer::Expr>> groupby_exprs_;
  bool has_agg_ = false;
  bool is_partial_avg_ = false;
};

class InputDescContext : public BaseContext {
 public:
  virtual ~InputDescContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  std::vector<InputDescriptor>* getInputDescs();
  std::list<std::shared_ptr<const InputColDescriptor>>* getInputColDescs();

 private:
  std::vector<InputDescriptor> input_descs_;
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs_;
};

class JoinQualContext : public BaseContext {
 public:
  virtual ~JoinQualContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  JoinQualsPerNestingLevel* getJoinQuals();

 private:
  JoinQualsPerNestingLevel join_quals_;
};

class TargetContext : public BaseContext {
 public:
  virtual ~TargetContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  std::vector<std::shared_ptr<Analyzer::Expr>>* getTargetExprs();
  std::vector<std::pair<ColumnHint, int>>* getColHintRecords();

 private:
  std::vector<std::shared_ptr<Analyzer::Expr>> target_exprs_;
  std::vector<std::pair<ColumnHint, int>> col_hint_records_;
};

class OrderEntryContext : public BaseContext {
 public:
  virtual ~OrderEntryContext();
  virtual void accept(std::shared_ptr<RelVisitor> rel_visitor_ptr);
  virtual void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  std::list<Analyzer::OrderEntry>* getOrderEntry();
  SortAlgorithm* getSortAlgorithm();
  size_t* getOffset();
  size_t* getLimit();

 private:
  std::list<Analyzer::OrderEntry> orderby_collation_;
  SortAlgorithm sort_algorithm_ = SortAlgorithm::Default;
  size_t offset_ = 0;
  size_t limit_ = 0;
};

}  // namespace generator
