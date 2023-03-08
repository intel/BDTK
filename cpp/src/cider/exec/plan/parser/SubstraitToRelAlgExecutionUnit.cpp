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
#include "cider/CiderException.h"
#include "exec/plan/parser/Translator.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "substrait/extensions/extensions.pb.h"
#include "type/plan/Analyzer.h"
#include "type/schema/ColumnInfo.h"
#include "util/measure.h"

namespace generator {
RelAlgExecutionUnit SubstraitToRelAlgExecutionUnit::createRelAlgExecutionUnit() {
  if (plan_.relations_size() == 0) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
  }
  if (!plan_.relations(0).has_root()) {
    CIDER_THROW(CiderCompileException, "invalid plan with no root node.");
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

RelAlgExecutionUnit SubstraitToRelAlgExecutionUnit::createRelAlgExecutionUnit(
    const substrait::ExtendedExpression* ext_expr) {
  std::vector<InputDescriptor> input_descs;
  int cur_nest_level = 0;
  InputDescriptor input_desc(fake_table_id, cur_nest_level);
  input_descs.emplace_back(input_desc);
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>> expr_map_ptr =
      std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  for (int i = 0; i < ext_expr->base_schema().struct_().types_size(); i++) {
    auto type = ext_expr->base_schema().struct_().types(i);
    auto sti = getSQLTypeInfo(type);
    ColumnInfoPtr colum_info_ptr = std::make_shared<ColumnInfo>(
        fake_db_id, fake_table_id, i, ext_expr->base_schema().names(i), sti, false);
    input_col_descs.emplace_back(
        std::make_shared<const InputColDescriptor>(colum_info_ptr,
                                                   cur_nest_level));  // FIXME
    auto col_var = std::make_shared<Analyzer::ColumnVar>(colum_info_ptr, cur_nest_level);
    expr_map_ptr->insert(std::pair(i, col_var));
  }
  std::unordered_map<int, std::string> func_map;
  for (int i = 0; i < ext_expr->extensions_size(); i++) {
    const auto& extension = ext_expr->extensions(i);
    if (extension.has_extension_function()) {
      const auto& function = extension.extension_function().name();
      // do function lookup verify and function mapping
      // get op type, no need mapping in substraitToAnalyzerExpr
      func_map.emplace(extension.extension_function().function_anchor(), function);
    }
  }
  std::vector<std::shared_ptr<Analyzer::Expr>> shared_target_exprs;
  for (int i = 0; i < ext_expr->referred_expr_size(); i++) {
    // TODO: (yma11) support more expression types besides scalar function and aggregate
    // function
    if (ext_expr->referred_expr(i).has_expression()) {
      shared_target_exprs.emplace_back(toAnalyzerExprConverter_.toAnalyzerExpr(
          ext_expr->referred_expr(i).expression(), func_map, expr_map_ptr));
    } else if (ext_expr->referred_expr(i).has_measure()) {
      shared_target_exprs.emplace_back(toAnalyzerExprConverter_.toAnalyzerExpr(
          ext_expr->referred_expr(i).measure(), func_map, expr_map_ptr));
    } else {
      CIDER_THROW(CiderCompileException,
                  "unsupported expression type for expression evaluation.");
    }
  }
  std::vector<Analyzer::Expr*> target_exprs;
  for (auto target_expr : shared_target_exprs) {
    // TODO: Memleak here, remove it after nextgen ready.
    std::cout << target_expr->toString() << std::endl;
    target_exprs.emplace_back(getExpr(target_expr, false));
  }
  std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  std::shared_ptr<Analyzer::Expr> empty;
  groupby_exprs.emplace_back(empty);
  RelAlgExecutionUnit rel_alg_eu{input_descs,
                                 input_col_descs,
                                 {},
                                 {},
                                 {},
                                 groupby_exprs,
                                 target_exprs,
                                 nullptr,
                                 {{}, SortAlgorithm::Default, 0, 0},
                                 0,
                                 false,
                                 std::nullopt,
                                 shared_target_exprs};
  return rel_alg_eu;
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

void SubstraitToRelAlgExecutionUnit::updateGeneratorContext(
    const substrait::Rel& rel_node,
    const std::unordered_map<int, std::string>& function_map) {
  // 1. visit whole plan and put each rel node into a Vector: std::vector<substrait::Rel,
  // bool is_join_right_node>
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
        CIDER_THROW(
            CiderCompileException,
            fmt::format("Unsupported substrait rel type {}", rel_node.rel_type_case()));
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
      rel_vec.emplace_back(rel_node, is_join_right_node);
      break;
    case substrait::Rel::RelTypeCase::kFilter:
      getRelNodesInPostOder(
          rel_node.filter().input(), rel_vec, rel_type_set, is_join_right_node);
      rel_vec.emplace_back(rel_node, is_join_right_node);
      break;
    case substrait::Rel::RelTypeCase::kProject:
      getRelNodesInPostOder(
          rel_node.project().input(), rel_vec, rel_type_set, is_join_right_node);
      rel_vec.emplace_back(rel_node, is_join_right_node);
      break;
    case substrait::Rel::RelTypeCase::kAggregate:
      getRelNodesInPostOder(
          rel_node.aggregate().input(), rel_vec, rel_type_set, is_join_right_node);
      rel_vec.emplace_back(rel_node, is_join_right_node);
      break;
    case substrait::Rel::RelTypeCase::kJoin:
      getRelNodesInPostOder(rel_node.join().left(), rel_vec, rel_type_set, false);
      getRelNodesInPostOder(rel_node.join().right(), rel_vec, rel_type_set, true);
      rel_vec.emplace_back(rel_node, is_join_right_node);
      break;
    default:
      CIDER_THROW(CiderCompileException,
                  fmt::format("Unsupported substrait rel type {}", rel_type));
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
        CIDER_THROW(CiderCompileException,
                    fmt::format("Unsupported substrait rel type {}", rel_type));
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
