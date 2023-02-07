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
 * @file    SubstraitExprBuilder.cpp
 * @brief   Provide utils methods for building subtrait expression
 **/

#include "SubstraitExprBuilder.h"
#include "cider/CiderException.h"
#include "exec/plan/parser/TypeUtils.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"
#include "util/Logger.h"

SubstraitExprBuilder::SubstraitExprBuilder(std::vector<std::string> names,
                                           std::vector<::substrait::Type*> types)
    : func_anchor_(0), names_(names), types_(types), funcs_info_{} {
  schema_ = makeNamedStruct(names, types);
}

::substrait::ExtendedExpression* SubstraitExprBuilder::build(
    std::vector<ScalarExprRef> expr_refs) {
  ::substrait::ExtendedExpression* ext_expr = new ::substrait::ExtendedExpression();
  for (auto expr_ref : expr_refs) {
    auto referred_expr = ext_expr->add_referred_expr();
    referred_expr->set_allocated_expression(expr_ref.expr);
    referred_expr->add_output_names(expr_ref.output_names);
  }
  using SimpleExtensionURI = ::substrait::extensions::SimpleExtensionURI;
  // Currently we don't introduce any substrait extension YAML files, so always
  // only have one dummy URI.
  SimpleExtensionURI* extensionUri = ext_expr->add_extension_uris();
  extensionUri->set_extension_uri_anchor(1);
  for (auto func_info : funcs_info_) {
    ext_expr->add_extensions()->set_allocated_extension_function(func_info);
  }
  ext_expr->set_allocated_base_schema(schema_);
  return ext_expr;
}

::substrait::ExtendedExpression* SubstraitExprBuilder::build(
    std::vector<AggregationExprRef> expr_refs) {
  ::substrait::ExtendedExpression* ext_expr = new ::substrait::ExtendedExpression();
  for (auto expr_ref : expr_refs) {
    auto referred_expr = ext_expr->add_referred_expr();
    referred_expr->set_allocated_measure(expr_ref.expr);
    referred_expr->add_output_names(expr_ref.output_names);
  }
  using SimpleExtensionURI = ::substrait::extensions::SimpleExtensionURI;
  // Currently we don't introduce any substrait extension YAML files, so always
  // only have one dummy URI.
  SimpleExtensionURI* extensionUri = ext_expr->add_extension_uris();
  extensionUri->set_extension_uri_anchor(1);
  for (auto func_info : funcs_info_) {
    ext_expr->add_extensions()->set_allocated_extension_function(func_info);
  }
  ext_expr->set_allocated_base_schema(schema_);
  return ext_expr;
}

::substrait::NamedStruct* SubstraitExprBuilder::makeNamedStruct(
    std::vector<std::string> names,
    std::vector<::substrait::Type*> types) {
  // CHECK_EQ(names.size(), types.size());
  std::unordered_set<std::string> unique_names;
  for (auto name : names) {
    unique_names.emplace(name);
  }
  // CHECK(unique_names.size() == names.size(), "col names should not be duplicated.");
  ::substrait::NamedStruct* named_struct = new ::substrait::NamedStruct();
  for (auto name : names) {
    named_struct->add_names(name);
  }
  auto mutable_struct = named_struct->mutable_struct_();
  for (auto type : types) {
    mutable_struct->add_types()->CopyFrom(*type);
  }
  // TODO: Need set different nuallibilty for struct?
  mutable_struct->set_type_variation_reference(0);
  mutable_struct->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  return named_struct;
}

::substrait::NamedStruct* SubstraitExprBuilder::getSchema() {
  return schema_;
}

::substrait::Expression* SubstraitExprBuilder::makeFieldReference(size_t field) {
  // TODO: (yma11) enable CHECK once glog adopted?
  // CHECK(field < names_.size(), "Col index is out of boundary.");
  auto field_expr = new ::substrait::Expression();
  ::substrait::Expression_FieldReference* s_field_ref = field_expr->mutable_selection();
  ::substrait::Expression_ReferenceSegment_StructField* s_direct_struct =
      s_field_ref->mutable_direct_reference()->mutable_struct_field();
  s_field_ref->mutable_root_reference();
  s_direct_struct->set_field(field);
  return field_expr;
}

::substrait::Expression* SubstraitExprBuilder::makeFieldReference(std::string name) {
  size_t col_index = -1;
  for (int i = 0; i < names_.size(); i++) {
    if (name == names_[i]) {
      col_index = i;
    }
  }
  // CHECK(col_index != -1,
  //       "column with name " + name + " is not found in referred schema.");
  return makeFieldReference(col_index);
}

::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*
SubstraitExprBuilder::makeFunc(std::string func_name,
                               std::vector<::substrait::Expression*> args,
                               ::substrait::Type* output_type) {
  // Function signature has format like "func_name:arg1_type, arg2_type..."
  auto func_sig = func_name + ":opt";
  for (auto arg : args) {
    if (arg->has_scalar_function()) {
      func_sig =
          func_sig + "_" + TypeUtils::getStringType(arg->scalar_function().output_type());
      continue;
    }
    if (arg->has_selection()) {
      // get type from schema for field reference
      auto field = arg->selection().direct_reference().struct_field().field();
      func_sig =
          func_sig + "_" + TypeUtils::getStringType(schema_->struct_().types(field));
    }
    if (arg->has_literal()) {
      func_sig = func_sig + "_" + TypeUtils::getStringType(arg->literal());
    }
  }
  for (auto func : funcs_info_) {
    if (func->name() == func_sig) {
      // return directly if func already exist
      return func;
    }
  }
  auto func = new ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction();
  func->set_name(func_sig);
  func->set_function_anchor(func_anchor_);
  func_anchor_ += 1;
  func->set_extension_uri_reference(1);
  funcs_info_.push_back(func);
  return func;
}

::substrait::Expression* SubstraitExprBuilder::makeScalarExpr(
    std::string func_name,
    std::vector<::substrait::Expression*> args,
    ::substrait::Type* output_type) {
  auto expr = new ::substrait::Expression();
  auto expr_func = expr->mutable_scalar_function();
  expr_func->set_function_reference(
      makeFunc(func_name, args, output_type)->function_anchor());
  for (auto arg : args) {
    expr_func->add_arguments()->mutable_value()->CopyFrom(*arg);
  }
  expr_func->mutable_output_type()->CopyFrom(*output_type);
  return expr;
}

::substrait::AggregateFunction* SubstraitExprBuilder::makeAggExpr(
    std::string func_name,
    std::vector<::substrait::Expression*> args,
    ::substrait::Type* output_type,
    ::substrait::AggregationPhase agg_phase,
    ::substrait::AggregateFunction_AggregationInvocation agg_invoc) {
  auto expr = new ::substrait::AggregateFunction();
  expr->set_function_reference(makeFunc(func_name, args, output_type)->function_anchor());
  for (auto arg : args) {
    expr->add_arguments()->mutable_value()->CopyFrom(*arg);
  }
  expr->set_phase(agg_phase);
  expr->set_invocation(agg_invoc);
  expr->mutable_output_type()->CopyFrom(*output_type);
  return expr;
}
