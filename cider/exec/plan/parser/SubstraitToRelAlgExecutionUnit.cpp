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
 * @file    SubstraitToRelAlgExecutionUnit.cpp
 * @brief   Translate Substrait plan to RelAlgExecutionUnit
 **/
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include <cstdint>
#include "PlanContext.h"
#include "PlanRelVisitor.h"
#include "RelVisitor.h"
#include "TypeUtils.h"
#include "VariableContext.h"
#include "exec/plan/parser/Translator.h"
#include "exec/template/QueryHint.h"
#include "exec/template/common/descriptors/ColSlotContext.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "substrait/extensions/extensions.pb.h"
#include "type/plan/Analyzer.h"
#include "type/schema/ColumnInfo.h"
#include "util/measure.h"

namespace generator {
RelAlgExecutionUnit SubstraitToRelAlgExecutionUnit::createRelAlgExecutionUnit() {
  if (plan_.relations_size() == 0) {
    throw std::runtime_error("invalid plan with no root node.");
  }
  if (!plan_.relations(0).has_root()) {
    throw std::runtime_error("invalid plan with no root node.");
  }
  substrait::Rel root = plan_.relations(0).root().input();
  // create an empty context for future update
  std::vector<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  ctx_ = std::make_shared<GeneratorContext>(GeneratorContext{{},
                                                             {},
                                                             {},
                                                             {},
                                                             {},
                                                             {},
                                                             {},
                                                             0,
                                                             0,
                                                             false,
                                                             {},
                                                             getLeftJoinDepth(plan_),
                                                             false,
                                                             false,
                                                             {}});
  updateGeneratorContext(root, getFunctionMap(plan_));
  return ctx_->getExeUnitBasedOnContext();
}

std::shared_ptr<RelAlgExecutionUnit>
SubstraitToRelAlgExecutionUnit::createRelAlgExecutionUnit(
    const std::vector<substrait::Expression*> exprs,
    const substrait::NamedStruct& schema,
    const std::vector<
        substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*> func_infos,
    const ExprType& expr_type) {
  INJECT_TIMER(createRelAlgExecutionUnit);
  // Converts expression to Analyzer::Expr
  std::vector<std::shared_ptr<Analyzer::Expr>> evaluated_exprs;
  for (auto expr : exprs) {
    auto analyzed_expr =
        toAnalyzerExprConverter_.toAnalyzerExpr(*expr, getFunctionMap(func_infos));
    // Updates the ColumnVar to correct type
    for (int i = 0; i < schema.struct_().types_size(); i++) {
      analyzed_expr =
          toAnalyzerExprConverter_.updateAnalyzerExpr(analyzed_expr,
                                                      i,
                                                      dummy_col_index,
                                                      fake_table_id,
                                                      dummy_rte_idx,
                                                      nullptr,
                                                      schema.struct_().types(i),
                                                      UpdateOnlyType);
    }
    evaluated_exprs.push_back(analyzed_expr);
  }
  int cur_nest_level = 0;
  InputDescriptor input_desc(fake_table_id, cur_nest_level);
  std::vector<InputDescriptor> input_descs;
  input_descs.insert(input_descs.begin(), input_desc);
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  std::vector<Analyzer::Expr*> target_exprs;
  std::list<std::shared_ptr<Analyzer::Expr>> quals;
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  std::vector<std::string> names;
  std::vector<substrait::Type> column_types;
  std::vector<ColumnHint> col_hints;
  for (int i = 0; i < schema.struct_().types_size(); i++) {
    names.push_back(schema.names(i));
    column_types.push_back(schema.struct_().types(i));
    col_hints.push_back(ColumnHint::Normal);
    auto col_desc =
        std::make_shared<ColumnInfo>(fake_db_id,
                                     fake_table_id,
                                     i,
                                     schema.names(i),
                                     getSQLTypeInfo(schema.struct_().types(i)),
                                     false);
    input_col_descs.push_back(
        std::make_shared<const InputColDescriptor>(col_desc, cur_nest_level));
    auto col_expr = std::make_shared<Analyzer::ColumnVar>(col_desc, cur_nest_level);
    // Put exprs into target list but may need remove them if it's not filter case
    target_exprs.emplace_back(getExpr(col_expr));
  }
  CiderTableSchema table_schema(names, column_types, "", col_hints);
  // Create input table schema
  input_table_schemas_.push_back(table_schema);
  switch (expr_type) {
    case ExprType::FilterExpr: {
      // For filter case, there should be only one expression
      CHECK_EQ(evaluated_exprs.size(), 1);
      auto quals_filter =
          qual_to_conjunctive_form(fold_expr(getExpr(evaluated_exprs[0])));
      quals = quals_filter.quals;
      simple_quals = quals_filter.simple_quals;
      // Create output table schema
      output_cider_table_schema_ = table_schema;
      break;
    }
    case ExprType::ProjectExpr: {
      // For project, there can be multi targets
      target_exprs.clear();
      // Generate target_exprs as well as output table schema
      std::vector<std::string> o_names;
      std::vector<substrait::Type> o_column_types;
      std::vector<ColumnHint> o_col_hints;
      for (int i = 0; i < evaluated_exprs.size(); i++) {
        // TODO: Here we use faked output_names but should update when use expr evaluation
        // in real op
        o_names.push_back(std::to_string(i));
        auto p_evaluated_expr = getExpr(evaluated_exprs[0]);
        o_column_types.push_back(getSubstraitType(p_evaluated_expr->get_type_info()));
        o_col_hints.push_back(ColumnHint::Normal);
        target_exprs.emplace_back(p_evaluated_expr);
      }
      CiderTableSchema output_schema(o_names, o_column_types, "", o_col_hints);
      output_cider_table_schema_ = output_schema;
      break;
    }
    default:
      throw std::runtime_error("unsupported type " + enum_str[expr_type] +
                               " for expression evaluation.");
  }
  RelAlgExecutionUnit rel_alg_eu{input_descs,
                                 input_col_descs,
                                 simple_quals,
                                 quals,
                                 {},
                                 {nullptr},
                                 target_exprs,
                                 nullptr,
                                 {{}, SortAlgorithm::Default, 0, 0},
                                 0,
                                 RegisteredQueryHint::defaults(),
                                 EMPTY_QUERY_PLAN,
                                 {},
                                 {},
                                 false,
                                 std::nullopt};
  return std::make_shared<RelAlgExecutionUnit>(rel_alg_eu);
}

substrait::Type SubstraitToRelAlgExecutionUnit::reconstructStructType(size_t index,
                                                                      size_t length) {
  substrait::Type s_type;
  auto s_struct = new substrait::Type_Struct();
  bool is_nullable = true;
  for (; length; length--) {
    auto type_info = ctx_->target_exprs_[index++]->get_type_info();
    is_nullable = is_nullable && !type_info.get_notnull();
    auto type = getSubstraitType(type_info);
    auto add_type = s_struct->add_types();
    *add_type = type;
  }
  substrait::Type_Nullability nullalbility =
      TypeUtils::getSubstraitTypeNullability(is_nullable);
  s_struct->set_nullability(nullalbility);
  s_type.set_allocated_struct_(s_struct);
  return s_type;
}

CiderTableSchema SubstraitToRelAlgExecutionUnit::getOutputCiderTableSchema() {
  if (!output_cider_table_schema_.getColumnCount()) {
    if (plan_.relations_size() == 0) {
      throw std::runtime_error("invalid plan with no root node.");
    }
    if (!plan_.relations(0).has_root()) {
      throw std::runtime_error("invalid plan with no root node.");
    }
    if (!ctx_) {
      throw std::runtime_error("RelAlgExecutionUnit not generated yet!");
    }
    // get output column names in depth order
    std::vector<std::string> names;
    for (int i = 0; i < plan_.relations(0).root().names_size(); i++) {
      names.push_back(plan_.relations(0).root().names(i));
    }
    // get output column types
    std::vector<substrait::Type> column_types;
    std::vector<ColumnHint> col_hints;
    for (size_t i_target = 0, i_struct = 0; i_target < ctx_->target_exprs_.size();
         i_struct++) {
      col_hints.push_back(ctx_->col_hint_records_[i_struct].first);
      size_t length = ctx_->col_hint_records_[i_struct].second;
      if (length < 2) {
        column_types.push_back(
            getSubstraitType(ctx_->target_exprs_[i_target]->get_type_info()));
      } else {
        column_types.push_back(reconstructStructType(i_target, length));
      }
      i_target += length;
    }
    CiderTableSchema schema(names, column_types, "", col_hints);
    output_cider_table_schema_ = schema;
  }
  return output_cider_table_schema_;
}

void SubstraitToRelAlgExecutionUnit::updateGeneratorContext(
    const substrait::Rel& rel_node,
    const std::unordered_map<int, std::string>& function_map) {
  // 1. visit whole plan and put each rel node into a Vector: std::vector<substrait::Rel,
  // bool is_right_join_node>
  std::vector<std::pair<substrait::Rel, bool>> rel_vec;
  std::unordered_set<substrait::Rel::RelTypeCase> rel_type_set;
  getRelNodesInPostOder(rel_node, rel_vec, rel_type_set);

  // 2. travel substrait vec to parse
  // build related ctxElements: targets/group_by/filter/join/input
  std::vector<std::shared_ptr<BaseContext>> ctx_elements;
  generateCtxElements(rel_type_set, ctx_elements);
  std::shared_ptr<VariableContext> variable_context_shared_ptr =
      std::make_shared<VariableContext>(ctx_->cur_join_depth_);
  std::shared_ptr<RelVisitor> rel_visitor_ptr;
  for (const auto& rel_node_pair : rel_vec) {
    switch (rel_node_pair.first.rel_type_case()) {
      case substrait::Rel::RelTypeCase::kRead:
        rel_visitor_ptr = std::make_shared<ReadRelVisitor>(rel_node_pair.first.read(),
                                                           &toAnalyzerExprConverter_,
                                                           function_map,
                                                           variable_context_shared_ptr,
                                                           &input_table_schemas_,
                                                           rel_node_pair.second);
        break;
      case substrait::Rel::RelTypeCase::kProject:
        rel_visitor_ptr =
            std::make_shared<ProjectRelVisitor>(rel_node_pair.first.project(),
                                                &toAnalyzerExprConverter_,
                                                function_map,
                                                variable_context_shared_ptr,
                                                rel_node_pair.second);
        break;
      case substrait::Rel::RelTypeCase::kFilter:
        rel_visitor_ptr = std::make_shared<FilterRelVisitor>(rel_node_pair.first.filter(),
                                                             &toAnalyzerExprConverter_,
                                                             function_map,
                                                             variable_context_shared_ptr,
                                                             rel_node_pair.second);
        break;
      case substrait::Rel::RelTypeCase::kAggregate:
        rel_visitor_ptr = std::make_shared<AggRelVisitor>(rel_node_pair.first.aggregate(),
                                                          &toAnalyzerExprConverter_,
                                                          function_map,
                                                          variable_context_shared_ptr,
                                                          rel_node_pair.second);
        break;
      case substrait::Rel::RelTypeCase::kJoin:
        rel_visitor_ptr = std::make_shared<JoinRelVisitor>(rel_node_pair.first.join(),
                                                           &toAnalyzerExprConverter_,
                                                           function_map,
                                                           variable_context_shared_ptr,
                                                           rel_node_pair.second);
        break;
      default:
        throw std::runtime_error("Unsupported substrait rel type " +
                                 std::to_string(rel_node.rel_type_case()));
    }
    for (auto ctx_element : ctx_elements) {
      ctx_element->accept(rel_visitor_ptr);
    }
  }

  // 3. convert ctxElements to ctx struct
  for (auto ctx_element : ctx_elements) {
    ctx_element->convert(ctx_);
  }
  variable_context_shared_ptr->convert(ctx_);
}

void SubstraitToRelAlgExecutionUnit::getRelNodesInPostOder(
    const substrait::Rel& rel_node,
    std::vector<std::pair<substrait::Rel, bool>>& rel_vec,
    std::unordered_set<substrait::Rel::RelTypeCase>& rel_type_set,
    bool is_join_right_node) {
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  rel_type_set.insert(rel_type);
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kRead:
      if (is_join_right_node) {
        rel_vec.push_back(std::pair(rel_node, true));
      } else {
        rel_vec.push_back(std::pair(rel_node, false));
      }
      break;
    case substrait::Rel::RelTypeCase::kFilter:
      getRelNodesInPostOder(
          rel_node.filter().input(), rel_vec, rel_type_set, is_join_right_node);
      if (is_join_right_node) {
        rel_vec.push_back(std::pair(rel_node, true));
      } else {
        rel_vec.push_back(std::pair(rel_node, false));
      }
      break;
    case substrait::Rel::RelTypeCase::kProject:
      getRelNodesInPostOder(
          rel_node.project().input(), rel_vec, rel_type_set, is_join_right_node);
      if (is_join_right_node) {
        rel_vec.push_back(std::pair(rel_node, true));
      } else {
        rel_vec.push_back(std::pair(rel_node, false));
      }
      break;
    case substrait::Rel::RelTypeCase::kAggregate:
      getRelNodesInPostOder(
          rel_node.aggregate().input(), rel_vec, rel_type_set, is_join_right_node);
      if (is_join_right_node) {
        rel_vec.push_back(std::pair(rel_node, true));
      } else {
        rel_vec.push_back(std::pair(rel_node, false));
      }
      break;
    case substrait::Rel::RelTypeCase::kJoin:
      getRelNodesInPostOder(rel_node.join().left(), rel_vec, rel_type_set, false);
      getRelNodesInPostOder(rel_node.join().right(), rel_vec, rel_type_set, true);
      rel_vec.push_back(std::pair(rel_node, is_join_right_node));
      break;
    default:
      throw std::runtime_error("Unsupported substrait rel type " +
                               std::to_string(rel_type));
  }
}

void SubstraitToRelAlgExecutionUnit::generateCtxElements(
    const std::unordered_set<substrait::Rel::RelTypeCase>& rel_type_set,
    std::vector<std::shared_ptr<BaseContext>>& ctx_elements_vec) {
  // use to prevent same type duplicated creation of Ctx elements
  std::unordered_set<ContextElementType> context_type_set;
  for (auto iter = rel_type_set.begin(); iter != rel_type_set.end(); ++iter) {
    const auto& rel_type = *iter;
    switch (rel_type) {
      case substrait::Rel::RelTypeCase::kRead:
        context_type_set.insert(ContextElementType::InputDescContextType);
        context_type_set.insert(ContextElementType::TargetContextType);
        break;
      case substrait::Rel::RelTypeCase::kFilter:
        context_type_set.insert(ContextElementType::FilterQualContextType);
        break;
      case substrait::Rel::RelTypeCase::kProject:
        context_type_set.insert(ContextElementType::TargetContextType);
        break;
      case substrait::Rel::RelTypeCase::kAggregate:
        context_type_set.insert(ContextElementType::GroupbyContextType);
        context_type_set.insert(ContextElementType::TargetContextType);
        break;
      case substrait::Rel::RelTypeCase::kJoin:
        context_type_set.insert(ContextElementType::JoinQualContextType);
        break;
      default:
        throw std::runtime_error("Unsupported substrait rel type " +
                                 std::to_string(rel_type));
    }
  }
  // ctx elements should be order to GroupbyContext, InputDescContext, TargetContext,
  // FilterQualContext, JoinQualContext
  // For example, AggRel vistor should first deal GroupbyContext then deal TargetContext
  // because of expr_map will been updated after visit TargetContext.
  if (context_type_set.find(ContextElementType::GroupbyContextType) !=
      context_type_set.end()) {
    ctx_elements_vec.push_back(std::make_shared<GroupbyContext>());
  }
  if (context_type_set.find(ContextElementType::InputDescContextType) !=
      context_type_set.end()) {
    ctx_elements_vec.push_back(std::make_shared<InputDescContext>());
  }
  if (context_type_set.find(ContextElementType::TargetContextType) !=
      context_type_set.end()) {
    ctx_elements_vec.push_back(std::make_shared<TargetContext>());
  }
  if (context_type_set.find(ContextElementType::FilterQualContextType) !=
      context_type_set.end()) {
    ctx_elements_vec.push_back(std::make_shared<FilterQualContext>());
  }
  if (context_type_set.find(ContextElementType::JoinQualContextType) !=
      context_type_set.end()) {
    ctx_elements_vec.push_back(std::make_shared<JoinQualContext>());
  }
}

}  // namespace generator
