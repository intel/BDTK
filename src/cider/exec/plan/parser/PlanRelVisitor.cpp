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
 * @file    PlanRelVisitor.cpp
 * @brief   Plan Rel visitor
 **/

#include "PlanRelVisitor.h"
#include "TypeUtils.h"
#include "cider/CiderException.h"

namespace generator {

AggRelVisitor::AggRelVisitor(const substrait::AggregateRel& rel_node,
                             Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                             const std::unordered_map<int, std::string>& function_map,
                             std::shared_ptr<VariableContext> variable_context_shared_ptr,
                             bool is_join_right_node)
    : RelVisitor(toAnalyzerExprConverter,
                 function_map,
                 variable_context_shared_ptr,
                 is_join_right_node)
    , rel_node_(rel_node) {}

AggRelVisitor::~AggRelVisitor() {}

void AggRelVisitor::visit(TargetContext* target_context) {
  std::vector<std::shared_ptr<Analyzer::Expr>>* target_exprs_ptr =
      target_context->getTargetExprs();
  std::vector<std::pair<ColumnHint, int>>* col_hint_records_ptr =
      target_context->getColHintRecords();
  // Aggregate rel defaultly uses direct mapping, reorder mapping should an extra project
  // after visit agg rel node
  if (!rel_node_.common().has_direct()) {
    CIDER_THROW(CiderCompileException, "Only support direct output for AggregateRel.");
  }
  if (rel_node_.groupings_size() == 0 && rel_node_.measures_size() == 0) {
    CIDER_THROW(CiderCompileException,
                "AggregateRel should either has groupings or measures.");
  }

  // if groupby is also target_expr, which means there is project after AggregateRel, we
  // put different Ananlyzer::Expr in target_exprs and groupby_exprs
  // in later update, Analyzer::Var will update type or index but won't change to
  // expression
  // FIXME: only support simple grouping and measure
  // add groupby to both groupby_exprs(Analyzer::ColumnVar) and
  // target_exprs(Analyzer::Var)
  // note that varno starts from 1
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>> expr_map_ptr =
      std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  if (target_exprs_ptr->size() > 0) {
    target_exprs_ptr->clear();
    col_hint_records_ptr->clear();
  }
  int count = 0;
  // FIXME: only support simple grouping and measure
  // add groupby to both groupby_exprs(Analyzer::ColumnVar) and
  // target_exprs(Analyzer::Var)
  // note that varno starts from 1
  for (int i = 0; i < rel_node_.groupings(0).grouping_expressions_size(); i++) {
    auto cider_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
        rel_node_.groupings(0).grouping_expressions(i),
        function_map_,
        variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
    if (auto column_var = std::dynamic_pointer_cast<Analyzer::ColumnVar>(cider_expr)) {
      auto target_expr = toAnalyzerExprConverter_->makeVar(cider_expr->get_type_info(),
                                                           column_var->get_table_id(),
                                                           column_var->get_column_id(),
                                                           column_var->get_rte_idx(),
                                                           false,  // FIXME
                                                           Analyzer::Var::kGROUPBY,
                                                           i + 1,
                                                           true);
      target_exprs_ptr->push_back(target_expr);
      col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
      expr_map_ptr->insert(std::pair(count, target_expr));
      ++count;
    } else {
      target_exprs_ptr->push_back(cider_expr);
      col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
      expr_map_ptr->insert(std::pair(count, cider_expr));
      ++count;
    }
  }
  for (int i = 0; i < rel_node_.measures_size(); i++) {
    // Add the agg expressions in target_exprs
    auto s_expr = rel_node_.measures(i).measure();
    auto function_sig = getFunctionSignature(function_map_, s_expr.function_reference());
    std::string function_name;
    auto pos = function_sig.find_first_of(':');
    if (pos == std::string::npos) {
      // count(*)/count(1), front end maybe just give count as function_signature_str
      if (function_sig == "count") {
        function_name = function_sig;
      } else {
        CIDER_THROW(CiderCompileException, "Invalid function_sig: " + function_sig);
      }
    } else {
      function_name = function_sig.substr(0, pos);
    }
    // Need special handle for partial avg
    if (function_name == "avg" &&
        s_expr.phase() == ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE) {
      if (substrait::Type::kStruct != s_expr.output_type().kind_case()) {
        CIDER_THROW(CiderCompileException, "partial avg should have a struct type.");
      }
      col_hint_records_ptr->push_back(std::make_pair(ColumnHint::PartialAVG, 2));
      std::unordered_map<int, std::string> function_map_fake(function_map_);
      function_map_fake[s_expr.function_reference()] =
          "sum:" + TypeUtils::getStringType(s_expr.output_type().struct_().types(0));
      auto sum_target_expr = toAnalyzerExprConverter_->updateOutputTypeOfAVGPartial(
          toAnalyzerExprConverter_->toAnalyzerExpr(
              s_expr,
              function_map_fake,
              variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_),
              TypeUtils::getStringType(s_expr.output_type().struct_().types(0))),
          s_expr.output_type().struct_().types(0));
      target_exprs_ptr->push_back(sum_target_expr);
      expr_map_ptr->insert(std::pair(count, sum_target_expr));
      ++count;
      function_map_fake[s_expr.function_reference()] =
          "count:" + TypeUtils::getStringType(s_expr.output_type().struct_().types(1));
      auto count_target_expr = toAnalyzerExprConverter_->updateOutputTypeOfAVGPartial(
          toAnalyzerExprConverter_->toAnalyzerExpr(
              s_expr,
              function_map_fake,
              variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_),
              TypeUtils::getStringType(s_expr.output_type().struct_().types(1))),
          s_expr.output_type().struct_().types(1));
      target_exprs_ptr->push_back(count_target_expr);
      expr_map_ptr->insert(std::pair(count, count_target_expr));
      ++count;
    } else {
      auto target_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
          s_expr,
          function_map_,
          variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
      target_exprs_ptr->push_back(target_expr);
      col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
      expr_map_ptr->insert(std::pair(count, target_expr));
      ++count;
    }
  }
  // merge to update expr map
  variable_context_shared_ptr_->mergeOutExprMapsInside(expr_map_ptr, is_join_right_node_);
  variable_context_shared_ptr_->setIsTargetExprsCollected(true);
}

void AggRelVisitor::visit(GroupbyContext* groupby_context) {
  std::vector<std::shared_ptr<Analyzer::Expr>>* groupby_exprs_ptr =
      groupby_context->getGroupbyExprs();
  if (!rel_node_.common().has_direct()) {
    CIDER_THROW(CiderCompileException, "Only support direct output for AggregateRel.");
  }
  if (rel_node_.groupings_size() == 0 && rel_node_.measures_size() == 0) {
    CIDER_THROW(CiderCompileException,
                "AggregateRel should either has groupings or measures.");
  }

  if (rel_node_.measures_size() > 0) {
    groupby_context->setHasAgg(true);
  }

  // FIXME: only support simple grouping and measure
  // add groupby to both groupby_exprs(Analyzer::ColumnVar) and
  // target_exprs(Analyzer::Var)
  // note that varno starts from 1
  for (int i = 0; i < rel_node_.groupings(0).grouping_expressions_size(); i++) {
    auto cider_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
        rel_node_.groupings(0).grouping_expressions(i),
        function_map_,
        variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
    groupby_exprs_ptr->push_back(cider_expr);
  }

  for (int i = 0; i < rel_node_.measures_size(); i++) {
    auto s_expr = rel_node_.measures(i).measure();
    auto function_sig = getFunctionSignature(function_map_, s_expr.function_reference());
    std::string function_name;
    auto pos = function_sig.find_first_of(':');
    if (pos == std::string::npos) {
      // count(*)/count(1), front end maybe just give count as function_signature_str
      if (function_sig == "count") {
        function_name = function_sig;
      } else {
        CIDER_THROW(CiderCompileException, "Invalid function_sig: " + function_sig);
      }
    } else {
      function_name = function_sig.substr(0, pos);
    }
    if (function_name == "avg" &&
        s_expr.phase() == ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE) {
      if (substrait::Type::kStruct != s_expr.output_type().kind_case()) {
        CIDER_THROW(CiderCompileException, "partial avg should have a struct type.");
      }
      groupby_context->setIsPartialAvg(true);
    }
  }
}

FilterRelVisitor::FilterRelVisitor(
    const substrait::FilterRel& rel_node,
    Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
    const std::unordered_map<int, std::string>& function_map,
    std::shared_ptr<VariableContext> variable_context_shared_ptr,
    bool is_join_right_node)
    : RelVisitor(toAnalyzerExprConverter,
                 function_map,
                 variable_context_shared_ptr,
                 is_join_right_node)
    , rel_node_(rel_node) {}

FilterRelVisitor::~FilterRelVisitor() {}

void FilterRelVisitor::visit(FilterQualContext* filter_qual_context) {
  std::list<std::shared_ptr<Analyzer::Expr>>* simple_quals_ptr =
      filter_qual_context->getSimpleQuals();
  std::list<std::shared_ptr<Analyzer::Expr>>* quals_ptr = filter_qual_context->getQuals();
  if (!rel_node_.has_condition()) {
    CIDER_THROW(CiderCompileException, "FilterRel exists but has no condition.");
  }
  // update filter condition
  auto cider_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
      rel_node_.condition(),
      function_map_,
      variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
  auto quals_cf = qual_to_conjunctive_form(fold_expr(cider_expr.get()));
  // some cases like `where` together with `group by having`
  // will get multiple conditions from different places
  if (simple_quals_ptr->size() && true) {
    simple_quals_ptr->insert(simple_quals_ptr->begin(),
                             quals_cf.simple_quals.begin(),
                             quals_cf.simple_quals.end());
  } else {
    *simple_quals_ptr = quals_cf.simple_quals;
  }
  if (quals_ptr->size()) {
    quals_ptr->insert(quals_ptr->begin(), quals_cf.quals.begin(), quals_cf.quals.end());
  } else {
    *quals_ptr = quals_cf.quals;
  }
}

JoinRelVisitor::JoinRelVisitor(
    const substrait::JoinRel& rel_node,
    Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
    const std::unordered_map<int, std::string>& function_map,
    std::shared_ptr<VariableContext> variable_context_shared_ptr,
    bool is_join_right_node)
    : RelVisitor(toAnalyzerExprConverter,
                 function_map,
                 variable_context_shared_ptr,
                 is_join_right_node)
    , rel_node_(rel_node) {}

JoinRelVisitor::~JoinRelVisitor() {}

void JoinRelVisitor::visit(JoinQualContext* join_qual_context) {
  JoinQualsPerNestingLevel* join_quals_ptr = join_qual_context->getJoinQuals();
  if (!rel_node_.common().has_direct()) {
    CIDER_THROW(CiderCompileException, "Only support direct output for JoinRel.");
  }
  CHECK(rel_node_.has_expression());
  // use two expr map to records expr info, merge them when visit join rel node
  variable_context_shared_ptr_->mergeLeftAndRightExprMaps();
  JoinCondition join_qual;
  // get join type
  join_qual.type = getCiderJoinType(rel_node_.type());
  // get join quals which can be multi and case
  auto cider_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
      rel_node_.expression(),
      function_map_,
      variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
  auto quals_cf = qual_to_conjunctive_form(fold_expr(cider_expr.get()));
  std::list<std::shared_ptr<Analyzer::Expr>> join_condition_quals;
  join_condition_quals.insert(
      join_condition_quals.end(), quals_cf.quals.begin(), quals_cf.quals.end());
  join_condition_quals.insert(join_condition_quals.end(),
                              quals_cf.simple_quals.begin(),
                              quals_cf.simple_quals.end());
  // postJoinFilter
  if (rel_node_.has_post_join_filter()) {
    auto cider_post_join_filter_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
        rel_node_.post_join_filter(),
        function_map_,
        variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
    auto post_join_filter_quals_cf =
        qual_to_conjunctive_form(fold_expr(cider_post_join_filter_expr.get()));
    join_condition_quals.insert(join_condition_quals.end(),
                                post_join_filter_quals_cf.quals.begin(),
                                post_join_filter_quals_cf.quals.end());
    join_condition_quals.insert(join_condition_quals.end(),
                                post_join_filter_quals_cf.simple_quals.begin(),
                                post_join_filter_quals_cf.simple_quals.end());
  }
  join_qual.quals = join_condition_quals;
  join_quals_ptr->insert(join_quals_ptr->begin(), join_qual);
}

ProjectRelVisitor::ProjectRelVisitor(
    const substrait::ProjectRel& rel_node,
    Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
    const std::unordered_map<int, std::string>& function_map,
    std::shared_ptr<VariableContext> variable_context_shared_ptr,
    bool is_join_right_node)
    : RelVisitor(toAnalyzerExprConverter,
                 function_map,
                 variable_context_shared_ptr,
                 is_join_right_node)
    , rel_node_(rel_node) {}

ProjectRelVisitor::~ProjectRelVisitor() {}

void ProjectRelVisitor::visit(TargetContext* target_context) {
  // update column index of all field/expression based on project Emit info
  // https://substrait.io/relations/logical_relations/#project-operation
  // get parent's output column size for column index updating
  // if project is using direct output, the output is input columns + new expression,
  // only need to update the column index in ctx_ which is actually mapped to the
  // expression
  std::vector<std::shared_ptr<Analyzer::Expr>>* target_exprs_ptr =
      target_context->getTargetExprs();
  std::vector<std::pair<ColumnHint, int>>* col_hint_records_ptr =
      target_context->getColHintRecords();
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>> expr_map_ptr =
      std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  if (rel_node_.common().has_direct()) {
    auto max_index = getSizeOfOutputColumns(rel_node_.input());
    for (int i = max_index; i < max_index + rel_node_.expressions_size(); i++) {
      auto s_expr = rel_node_.expressions(i - max_index);
      auto c_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
          s_expr,
          function_map_,
          variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
      expr_map_ptr->insert(std::pair(i - max_index, c_expr));
    }
    // check whether the project field need to add in target_exprs
    // if there is no agg node or visited project node which already defines the
    // target_exprs, projections should be added to the target_exprs
    if (variable_context_shared_ptr_->getIsTargetExprsCollected()) {
      col_hint_records_ptr->clear();
      target_exprs_ptr->clear();
      variable_context_shared_ptr_->setIsTargetExprsCollected(false);
    }
    for (int i = 0; i < max_index + rel_node_.expressions_size(); i++) {
      // for column index, create Analyzer::ColumnVar
      if (i < max_index) {
        col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
        auto iter = expr_map_ptr->find(i);
        if (iter != expr_map_ptr->end()) {
          // if (auto column_var_expr =
          //         std::dynamic_pointer_cast<Analyzer::ColumnVar>(iter->second)) {
          // }
          target_exprs_ptr->push_back(iter->second);
        } else {
          CIDER_THROW(CiderCompileException, "Failed to get field reference expr.");
        }
      } else {
        auto s_expr = rel_node_.expressions(i - max_index);
        auto c_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
            s_expr,
            function_map_,
            variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
        col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
        target_exprs_ptr->emplace_back(c_expr);
      }
    }
    variable_context_shared_ptr_->setIsTargetExprsCollected(true);
    variable_context_shared_ptr_->mergeOutExprMapsInside(expr_map_ptr,
                                                         is_join_right_node_);
  }
  // if project is actually do a remapping, need to update all column index in ctx_, which
  // may refers an input column or a new expression
  if (rel_node_.common().has_emit()) {
    auto emit = rel_node_.common().emit();
    auto max_index = getSizeOfOutputColumns(rel_node_.input());
    // update expr groups in ctx_
    for (int i = 0; i < emit.output_mapping_size(); i++) {
      if (emit.output_mapping(i) < max_index) {
        continue;
      } else {
        auto s_expr = rel_node_.expressions(emit.output_mapping(i) - max_index);
        // even it's an expression, it still maybe a column reference.
        auto c_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
            s_expr,
            function_map_,
            variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
        expr_map_ptr->insert(std::pair(emit.output_mapping(i) - max_index, c_expr));
      }
    }
    // add projections to target_exprs if needed
    if (variable_context_shared_ptr_->getIsTargetExprsCollected()) {
      col_hint_records_ptr->clear();
      target_exprs_ptr->clear();
      variable_context_shared_ptr_->setIsTargetExprsCollected(false);
    }
    for (int i = 0; i < emit.output_mapping_size(); i++) {
      if (emit.output_mapping(i) < max_index) {
        col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
        auto iter = variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_)
                        ->find(emit.output_mapping(i));
        if (iter !=
            variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_)->end()) {
          target_exprs_ptr->push_back(iter->second);
        } else {
          CIDER_THROW(CiderCompileException, "Failed to get field reference expr.");
        }
      } else {
        auto s_expr = rel_node_.expressions(emit.output_mapping(i) - max_index);
        auto c_expr = toAnalyzerExprConverter_->toAnalyzerExpr(
            s_expr,
            function_map_,
            variable_context_shared_ptr_->getExprMapPtr(is_join_right_node_));
        col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
        target_exprs_ptr->emplace_back(c_expr);
      }
    }
    variable_context_shared_ptr_->setIsTargetExprsCollected(true);
    variable_context_shared_ptr_->mergeOutExprMapsInside(expr_map_ptr,
                                                         is_join_right_node_);
  }
}

ReadRelVisitor::ReadRelVisitor(
    const substrait::ReadRel& rel_node,
    Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
    const std::unordered_map<int, std::string>& function_map,
    std::shared_ptr<VariableContext> variable_context_shared_ptr,
    std::vector<CiderTableSchema>* input_table_schemas_ptr,
    bool is_join_right_node)
    : RelVisitor(toAnalyzerExprConverter,
                 function_map,
                 variable_context_shared_ptr,
                 is_join_right_node)
    , rel_node_(rel_node)
    , input_table_schemas_ptr_(input_table_schemas_ptr) {}

ReadRelVisitor::~ReadRelVisitor() {}

void ReadRelVisitor::visit(InputDescContext* input_desc_context) {
  // add input_descs, etc.
  std::vector<InputDescriptor>* input_descs_ptr = input_desc_context->getInputDescs();
  std::list<std::shared_ptr<const InputColDescriptor>>* input_col_descs_ptr =
      input_desc_context->getInputColDescs();
  // get cur_table_id and cur_nest_level base on join_depth
  int join_depth = variable_context_shared_ptr_->getCurJoinDepth();
  auto cur_table_id = fake_table_id + join_depth;
  auto cur_nest_level = join_depth;
  InputDescriptor input_desc(cur_table_id, cur_nest_level);
  input_descs_ptr->push_back(input_desc);
  // update type info of all field expression
  // TODO: need support unflattened type
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>> expr_map_ptr =
      std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  for (int i = 0; i < rel_node_.base_schema().struct_().types_size(); i++) {
    auto type = rel_node_.base_schema().struct_().types(i);
    auto sti = getSQLTypeInfo(type);
    ColumnInfoPtr colum_info_ptr = std::make_shared<ColumnInfo>(
        fake_db_id, cur_table_id, i, "col_" + std::to_string(i), sti, false);
    input_col_descs_ptr->push_back(
        std::make_shared<const InputColDescriptor>(colum_info_ptr,
                                                   cur_nest_level));  // FIXME
    auto col_var = std::make_shared<Analyzer::ColumnVar>(colum_info_ptr, cur_nest_level);
    expr_map_ptr->insert(std::pair(i, col_var));
  }
  variable_context_shared_ptr_->mergeOutExprMapsInside(expr_map_ptr, is_join_right_node_);
}

void ReadRelVisitor::visit(TargetContext* target_context) {
  // if it's still empty in target_exprs, like case select * w/o project provide,
  // add the columns as the final targets
  std::vector<std::shared_ptr<Analyzer::Expr>>* target_exprs_ptr =
      target_context->getTargetExprs();
  std::vector<std::pair<ColumnHint, int>>* col_hint_records_ptr =
      target_context->getColHintRecords();
  // get cur_table_id and cur_nest_level base on join_depth
  int join_depth = variable_context_shared_ptr_->getCurJoinDepth();
  auto cur_table_id = fake_table_id + join_depth;
  auto cur_nest_level = join_depth;
  if (!variable_context_shared_ptr_->getIsTargetExprsCollected()) {
    for (int i = 0; i < rel_node_.base_schema().struct_().types_size(); i++) {
      // update col_hint_records
      col_hint_records_ptr->push_back(std::make_pair(ColumnHint::Normal, 1));
      // update target_exprs
      auto type = rel_node_.base_schema().struct_().types(i);
      target_exprs_ptr->push_back(std::make_shared<Analyzer::ColumnVar>(
          getSQLTypeInfo(type), cur_table_id, i, cur_nest_level));
    }
    if (join_depth == variable_context_shared_ptr_->getMaxJoinDepth()) {
      variable_context_shared_ptr_->setIsTargetExprsCollected(true);
    }
  }
  // generate input table schema here
  std::vector<std::string> names;
  for (int i = 0; i < rel_node_.base_schema().names_size(); i++) {
    names.push_back(rel_node_.base_schema().names(i));
  }
  std::vector<substrait::Type> column_types;
  for (int i = 0; i < rel_node_.base_schema().struct_().types_size(); i++) {
    column_types.push_back(rel_node_.base_schema().struct_().types(i));
  }
  CiderTableSchema cur_table_schema(names, column_types, std::to_string(cur_table_id));
  input_table_schemas_ptr_->push_back(cur_table_schema);
  variable_context_shared_ptr_->setCurJoinDepth(join_depth + 1);
  // For each col var generation on the left branch, should use the fake_table_id
  toAnalyzerExprConverter_->setTableIdForNewColVar(fake_table_id);
}

}  // namespace generator
