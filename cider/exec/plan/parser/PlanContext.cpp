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
 * @file    PlanContext.cpp
 * @brief   Plan context element
 **/

#include "PlanContext.h"

namespace generator {

FilterQualContext::~FilterQualContext() {}

void FilterQualContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void FilterQualContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->simple_quals_ = this->simple_quals_;
  ctx_ptr->quals_ = this->quals_;
}

std::list<std::shared_ptr<Analyzer::Expr>>* FilterQualContext::getSimpleQuals() {
  return &simple_quals_;
}

std::list<std::shared_ptr<Analyzer::Expr>>* FilterQualContext::getQuals() {
  return &quals_;
}

GroupbyContext::~GroupbyContext() {}

void GroupbyContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void GroupbyContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->groupby_exprs_ = this->groupby_exprs_;
  ctx_ptr->has_agg_ = this->has_agg_;
  ctx_ptr->is_partial_avg_ = this->is_partial_avg_;
}

std::vector<std::shared_ptr<Analyzer::Expr>>* GroupbyContext::getGroupbyExprs() {
  return &groupby_exprs_;
}

void GroupbyContext::setHasAgg(bool has_agg) {
  has_agg_ = has_agg;
}

bool GroupbyContext::getHasAgg() const {
  return has_agg_;
}

void GroupbyContext::setIsPartialAvg(bool is_partial_avg) {
  is_partial_avg_ = is_partial_avg;
}

bool GroupbyContext::getIsPartialAvg() const {
  return is_partial_avg_;
}

InputDescContext::~InputDescContext() {}

void InputDescContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void InputDescContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->input_descs_ = this->input_descs_;
  ctx_ptr->input_col_descs_ = this->input_col_descs_;
}

std::vector<InputDescriptor>* InputDescContext::getInputDescs() {
  return &input_descs_;
}

std::list<std::shared_ptr<const InputColDescriptor>>*
InputDescContext::getInputColDescs() {
  return &input_col_descs_;
}

JoinQualContext::~JoinQualContext() {}

void JoinQualContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void JoinQualContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->join_quals_ = this->join_quals_;
}

JoinQualsPerNestingLevel* JoinQualContext::getJoinQuals() {
  return &join_quals_;
}

TargetContext::~TargetContext() {}

void TargetContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void TargetContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->target_exprs_ = this->target_exprs_;
  ctx_ptr->col_hint_records_ = this->col_hint_records_;
}

std::vector<std::shared_ptr<Analyzer::Expr>>* TargetContext::getTargetExprs() {
  return &target_exprs_;
}

std::vector<std::pair<ColumnHint, int>>* TargetContext::getColHintRecords() {
  return &col_hint_records_;
}

OrderEntryContext::~OrderEntryContext() {}

void OrderEntryContext::accept(std::shared_ptr<RelVisitor> rel_visitor_ptr) {
  rel_visitor_ptr->visit(this);
}

void OrderEntryContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->orderby_collation_ = this->orderby_collation_;
  ctx_ptr->sort_algorithm_ = this->sort_algorithm_;
  ctx_ptr->offset_ = this->offset_;
  ctx_ptr->limit_ = this->limit_;
}

std::list<Analyzer::OrderEntry>* OrderEntryContext::getOrderEntry() {
  return &orderby_collation_;
}

SortAlgorithm* OrderEntryContext::getSortAlgorithm() {
  return &sort_algorithm_;
}

size_t* OrderEntryContext::getOffset() {
  return &offset_;
}

size_t* OrderEntryContext::getLimit() {
  return &limit_;
}

}  // namespace generator
