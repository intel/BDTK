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

#pragma once

#include "SingleNodeValidator.h"
#include "cider/CiderException.h"
#include "exec/plan/parser/ConverterHelper.h"
#include "exec/plan/parser/TypeUtils.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "util/Logger.h"

namespace validator {

std::vector<substrait::Type> SingleNodeValidator::getRelOutputTypes(
    const substrait::Rel& rel_node) {
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  std::vector<substrait::Type> output_types;
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kRead: {
      for (int i = 0; i < rel_node.read().base_schema().struct_().types().size(); i++) {
        output_types.emplace_back(rel_node.read().base_schema().struct_().types(i));
      }
      return output_types;
    }
    case substrait::Rel::RelTypeCase::kFilter: {
      return getRelOutputTypes(rel_node.filter().input());
    }
    case substrait::Rel::RelTypeCase::kProject: {
      return getRelOutputTypes(rel_node, getRelOutputTypes(rel_node.project().input()));
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      return getRelOutputTypes(rel_node, getRelOutputTypes(rel_node.aggregate().input()));
    }
    case substrait::Rel::RelTypeCase::kJoin: {
      auto r_types = getRelOutputTypes(rel_node.join().right());
      auto l_types = getRelOutputTypes(rel_node.join().left());
      std::vector<substrait::Type> types;
      types.reserve(l_types.size() + r_types.size());
      types.insert(types.end(), l_types.begin(), l_types.end());
      types.insert(types.end(), r_types.begin(), r_types.end());
      return types;
    }
    default:
      CIDER_THROW(CiderPlanValidateException,
                  fmt::format("Failed to get output types of rel type {}", rel_type));
  }
}

std::vector<substrait::Type> SingleNodeValidator::getRelOutputTypes(
    const substrait::Rel& rel_node,
    const std::vector<substrait::Type>& input_types) {
  std::vector<substrait::Type> output_types;
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kProject: {
      auto& proj = rel_node.project();
      if (proj.common().has_emit()) {
        auto& emit = proj.common().emit();
        auto max_index = generator::getSizeOfOutputColumns(proj.input());
        for (int i = 0; i < emit.output_mapping_size(); i++) {
          if (emit.output_mapping(i) < max_index) {
            output_types.emplace_back(input_types[emit.output_mapping(i)]);
          } else {
            auto& s_expr = proj.expressions(emit.output_mapping(i) - max_index);
            if (!s_expr.has_selection()) {
              output_types.emplace_back(getExprOutputType(s_expr, input_types));
            } else {
              CHECK(s_expr.has_selection());
              auto type = input_types
                  [s_expr.selection().direct_reference().struct_field().field()];
              output_types.emplace_back(
                  input_types
                      [s_expr.selection().direct_reference().struct_field().field()]);
            }
          }
        }
      } else if (proj.common().has_direct()) {
        auto max_index = generator::getSizeOfOutputColumns(proj.input());
        for (int i = 0; i < max_index; i++) {
          output_types.emplace_back(input_types[i]);
        }
        for (int i = 0; i < proj.expressions_size(); i++) {
          auto& s_expr = proj.expressions(i);
          if (!s_expr.has_selection()) {
            output_types.emplace_back(getExprOutputType(s_expr, input_types));
          } else {
            CHECK(s_expr.has_selection());
            output_types.emplace_back(
                input_types
                    [s_expr.selection().direct_reference().struct_field().field()]);
          }
        }
      }

      return output_types;
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      auto& agg = rel_node.aggregate();
      // add groupby expr output types
      for (int i = 0; i < agg.groupings(0).grouping_expressions_size(); i++) {
        auto& groupby_expr = agg.groupings(0).grouping_expressions(i);
        if (groupby_expr.has_selection()) {
          output_types.emplace_back(
              input_types
                  [groupby_expr.selection().direct_reference().struct_field().field()]);
        } else {
          if (!groupby_expr.has_selection()) {
            output_types.emplace_back(getExprOutputType(groupby_expr, input_types));
          }
        }
      }
      // add aggregation function output types
      for (int i = 0; i < agg.measures_size(); i++) {
        output_types.emplace_back(agg.measures(i).measure().output_type());
      }
      return output_types;
    }
    case substrait::Rel::RelTypeCase::kJoin: {
      // For join, we only support direct mapping so just use input_types
      return input_types;
    }
    case substrait::Rel::RelTypeCase::kRead: {
      for (int i = 0; i < rel_node.read().base_schema().struct_().types().size(); i++) {
        output_types.emplace_back(rel_node.read().base_schema().struct_().types(i));
      }
      return output_types;
    }
    default:
      CIDER_THROW(CiderPlanValidateException,
                  fmt::format("Failed to get output types of rel type {}", rel_type));
  }
  return output_types;
}

substrait::Type SingleNodeValidator::getExprOutputType(
    const substrait::Expression& s_expr,
    const std::vector<substrait::Type>& input_types) {
  switch (s_expr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kScalarFunction:
      return s_expr.scalar_function().output_type();
    case substrait::Expression::RexTypeCase::kCast:
      return s_expr.cast().type();
    default:
      CIDER_THROW(CiderPlanValidateException,
                  fmt::format("Unsupported expression type {} for getExprOutputType()",
                              s_expr.rex_type_case()));
  }
}

bool SingleNodeValidator::isSupportedAllTypes(const std::vector<substrait::Type>& types) {
  int i = 0;
  for (; i < types.size(); i++) {
    if (supported_types.find(types[i].kind_case()) == supported_types.end()) {
      break;
    }
  }
  return i != 0 && i == types.size() ? true : false;
}

bool SingleNodeValidator::validate(
    const substrait::Rel& rel_node,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  // Validation on each plan node includes data types check and some basic
  // properties check. Data types contains current node output types and its
  // parents' output types. Actually there should be other data types involved
  // in nested functions, which we don't cover here. This won't cause problem
  // since it will be checked in later function level validation.
  const substrait::Rel::RelTypeCase& rel_type = rel_node.rel_type_case();
  switch (rel_type) {
    case substrait::Rel::RelTypeCase::kRead:
      return validate(rel_node.read(), func_map, func_lookup_ptr) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node));
    case substrait::Rel::RelTypeCase::kFilter: {
      return validate(rel_node.filter(), func_map, func_lookup_ptr) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node.filter().input()));
    }
    case substrait::Rel::RelTypeCase::kProject: {
      return validate(rel_node.project(), func_map, func_lookup_ptr) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node)) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node.project().input()));
    }
    case substrait::Rel::RelTypeCase::kAggregate: {
      return validate(rel_node.aggregate(), func_map, func_lookup_ptr) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node)) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node.aggregate().input()));
    }
    case substrait::Rel::RelTypeCase::kJoin: {
      // For join, we only support direct mapping
      return validate(rel_node.join(), func_map, func_lookup_ptr) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node)) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node.join().left())) &&
             isSupportedAllTypes(getRelOutputTypes(rel_node.join().right()));
    }
    default:
      CIDER_THROW(
          CiderPlanValidateException,
          fmt::format("Unsupported substrait rel type {} for validate", rel_type));
  }
}

void SingleNodeValidator::functionLookup(
    const substrait::AggregateFunction& agg_expr,
    const std::vector<substrait::Type>& input_types,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  std::string func_sig =
      generator::getFunctionSignature(func_map, agg_expr.function_reference());
  std::string function_name;
  auto pos = func_sig.find_first_of(':');
  if (pos == std::string::npos) {
    // count(*)/count(1), front end maybe just give count as function_signature_str
    if (func_sig == "count") {
      function_name = func_sig;
    } else {
      CIDER_THROW(CiderPlanValidateException, "Invalid function_sig: " + func_sig);
    }
  } else {
    function_name = func_sig.substr(0, pos);
  }
  std::string func_return_type = TypeUtils::getStringType(agg_expr.output_type());
  auto function_descriptor = func_lookup_ptr->lookupFunction(func_sig, func_return_type);
  if (!function_descriptor.is_cider_support_function) {
    CIDER_THROW(
        CiderPlanValidateException,
        fmt::format(
            "Not cider support function, function_sig: {}, funtion_return_type: {}",
            func_sig,
            func_return_type));
  }
}

void SingleNodeValidator::functionLookup(
    const substrait::Expression& s_expr,
    const std::vector<substrait::Type>& input_types,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  switch (s_expr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kScalarFunction: {
      std::string func_sig = generator::getFunctionSignature(
          func_map, s_expr.scalar_function().function_reference());
      std::string func_return_type =
          TypeUtils::getStringType(s_expr.scalar_function().output_type());
      auto function_descriptor =
          func_lookup_ptr->lookupFunction(func_sig, func_return_type);
      if (!function_descriptor.is_cider_support_function) {
        CIDER_THROW(
            CiderPlanValidateException,
            fmt::format(
                "Not cider support function, function_sig: {}, funtion_return_type: {}",
                func_sig,
                func_return_type));
      }
      for (auto argument : s_expr.scalar_function().arguments()) {
        functionLookup(argument.value(), input_types, func_map, func_lookup_ptr);
      }
      return;
    }
    case substrait::Expression::RexTypeCase::kCast: {
      std::string func_sig;
      if (s_expr.cast().input().has_literal()) {
        func_sig =
            "cast:" + TypeUtils::getStringType(generator::getSubstraitType(
                          generator::getSQLTypeInfo(s_expr.cast().input().literal())));
      } else if (s_expr.cast().input().has_selection()) {
        func_sig = "cast:" + TypeUtils::getStringType(input_types[s_expr.cast()
                                                                      .input()
                                                                      .selection()
                                                                      .direct_reference()
                                                                      .struct_field()
                                                                      .field()]);
      } else if (s_expr.cast().input().has_scalar_function()) {
        func_sig = "cast:" + TypeUtils::getStringType(
                                 s_expr.cast().input().scalar_function().output_type());
      }
      std::string func_return_type = TypeUtils::getStringType(s_expr.cast().type());
      auto function_descriptor =
          func_lookup_ptr->lookupFunction(func_sig, func_return_type);
      if (!function_descriptor.is_cider_support_function) {
        CIDER_THROW(
            CiderPlanValidateException,
            fmt::format(
                "Not cider support function, function_sig: {}, funtion_return_type: {}",
                func_sig,
                func_return_type));
      }
      if (s_expr.cast().input().has_scalar_function()) {
        for (auto argument : s_expr.cast().input().scalar_function().arguments()) {
          functionLookup(argument.value(), input_types, func_map, func_lookup_ptr);
        }
      }
      return;
    }
    case substrait::Expression::RexTypeCase::kLiteral:
    case substrait::Expression::RexTypeCase::kSelection: {
      return;
    }
    default:
      CIDER_THROW(CiderPlanValidateException,
                  fmt::format("Unsupported expression type {} for function lookup",
                              s_expr.rex_type_case()));
  }
}
bool SingleNodeValidator::validate(
    const substrait::ReadRel& read_rel,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  return read_rel.base_schema().names_size() ==
         read_rel.base_schema().struct_().types_size();
}

bool SingleNodeValidator::validate(
    const substrait::FilterRel& filter_rel,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  if (!filter_rel.has_condition()) {
    return false;
  }
  // do function look up
  try {
    functionLookup(filter_rel.condition(),
                   getRelOutputTypes(filter_rel.input()),
                   func_map,
                   func_lookup_ptr);
  } catch (CiderPlanValidateException& e) {
    LOG(INFO) << e.what();
    return false;
  }
  return true;
}

bool SingleNodeValidator::validate(
    const substrait::ProjectRel& proj_rel,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  try {
    auto input_types = getRelOutputTypes(proj_rel.input());
    if (proj_rel.common().has_emit()) {
      auto& emit = proj_rel.common().emit();
      auto max_index = generator::getSizeOfOutputColumns(proj_rel.input());
      for (int i = 0; i < emit.output_mapping_size(); i++) {
        if (emit.output_mapping(i) >= max_index) {
          auto& s_expr = proj_rel.expressions(emit.output_mapping(i) - max_index);
          // only need look up functions
          if (!s_expr.has_selection()) {
            functionLookup(s_expr, input_types, func_map, func_lookup_ptr);
          }
        }
      }
    } else if (proj_rel.common().has_direct()) {
      for (int i = 0; i < proj_rel.expressions_size(); i++) {
        auto& s_expr = proj_rel.expressions(i);
        if (!s_expr.has_selection()) {
          functionLookup(s_expr, input_types, func_map, func_lookup_ptr);
        }
      }
    }
  } catch (CiderPlanValidateException& e) {
    LOG(INFO) << e.what();
    return false;
  }
  return true;
}

bool SingleNodeValidator::validate(
    const substrait::AggregateRel& agg_rel,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  // Only support partial aggregation
  int i = 0;
  for (; i < agg_rel.measures_size(); i++) {
    if (agg_rel.measures(i).measure().phase() !=
        substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE) {
      break;
    }
  }
  if (!(agg_rel.common().has_direct() && (i != 0 && i == agg_rel.measures_size()))) {
    return false;
  }
  try {
    auto input_types = getRelOutputTypes(agg_rel.input());
    for (int i = 0; i < agg_rel.groupings(0).grouping_expressions_size(); i++) {
      auto& groupby_expr = agg_rel.groupings(0).grouping_expressions(i);
      if (!groupby_expr.has_selection()) {
        functionLookup(groupby_expr, input_types, func_map, func_lookup_ptr);
      }
    }
    // lookup aggregation functions
    for (int i = 0; i < agg_rel.measures_size(); i++) {
      functionLookup(
          agg_rel.measures(i).measure(), input_types, func_map, func_lookup_ptr);
    }
  } catch (CiderPlanValidateException& e) {
    LOG(INFO) << e.what();
    return false;
  }
  return true;
}

bool SingleNodeValidator::validate(
    const substrait::JoinRel& join_rel,
    const std::unordered_map<int, std::string>& func_map,
    std::shared_ptr<const FunctionLookupEngine> func_lookup_ptr) {
  // Only support join with direct. For emit, it uses join with post project
  bool basic_check =
      join_rel.has_expression() && join_rel.common().has_direct() &&
      join_rel.type() !=
          substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_UNSPECIFIED &&
      join_rel.type() != substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER &&
      join_rel.type() != substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT;
  if (!basic_check) {
    return false;
  }
  try {
    auto input_types_l = getRelOutputTypes(join_rel.left());
    auto input_types_r = getRelOutputTypes(join_rel.right());
    std::vector<substrait::Type> input_types;
    input_types.reserve(input_types_l.size() + input_types_r.size());
    input_types.insert(input_types.end(), input_types_l.begin(), input_types_l.end());
    input_types.insert(input_types.end(), input_types_r.begin(), input_types_r.end());
    functionLookup(join_rel.expression(), input_types, func_map, func_lookup_ptr);
  } catch (CiderPlanValidateException& e) {
    LOG(INFO) << e.what();
    return false;
  }
  return true;
}
}  // namespace validator
