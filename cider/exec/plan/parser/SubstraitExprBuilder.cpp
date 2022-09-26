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
 * @file    SubstraitExprBuilder.cpp
 * @brief   Provide utils methods for building subtrait expression
 **/

#include "SubstraitExprBuilder.h"
#include "TypeUtils.h"
#include "cider/CiderException.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"
#include "util/Logger.h"

::substrait::NamedStruct* SubstraitExprBuilder::makeNamedStruct(
    SubstraitExprBuilder* builder,
    std::vector<std::string> names,
    std::vector<::substrait::Type*> types) {
  CHECK_EQ(names.size(), types.size());
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
  builder->schema_ = named_struct;
  return named_struct;
}

::substrait::NamedStruct* SubstraitExprBuilder::schema() {
  if (schema_) {
    CIDER_THROW(CiderCompileException, "schema already exists, check your usage");
  }
  CHECK_GT(names_.size(), 0);
  CHECK_EQ(names_.size(), types_.size());
  return makeNamedStruct(this, names_, types_);
}

::substrait::Type* SubstraitExprBuilder::makeType(bool isNullable) {
  ::substrait::Type* type = new ::substrait::Type();
  ::substrait::Type_I64* type_i64 = new ::substrait::Type_I64();
  type_i64->set_nullability(TypeUtils::getSubstraitTypeNullability(isNullable));
  type->set_allocated_i64(type_i64);
  return type;
}

::substrait::Expression* SubstraitExprBuilder::makeFieldReference(size_t field) {
  auto arg = new ::substrait::Expression();
  ::substrait::Expression_FieldReference* s_field_ref = arg->mutable_selection();
  ::substrait::Expression_ReferenceSegment_StructField* s_direct_struct =
      s_field_ref->mutable_direct_reference()->mutable_struct_field();
  s_field_ref->mutable_root_reference();
  s_direct_struct->set_field(field);
  return arg;
}

::substrait::Expression* SubstraitExprBuilder::makeFieldReference(
    SubstraitExprBuilder* builder,
    std::string name,
    ::substrait::Type* type) {
  auto arg = new ::substrait::Expression();
  ::substrait::Expression_FieldReference* s_field_ref = arg->mutable_selection();
  ::substrait::Expression_ReferenceSegment_StructField* s_direct_struct =
      s_field_ref->mutable_direct_reference()->mutable_struct_field();
  s_field_ref->mutable_root_reference();
  s_direct_struct->set_field(builder->names_.size());
  builder->names_.push_back(name);
  builder->types_.push_back(type);
  return arg;
}

::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*
SubstraitExprBuilder::makeFunc(SubstraitExprBuilder* builder,
                               std::string func_name,
                               std::vector<::substrait::Expression*> args,
                               ::substrait::Type* output_type) {
  // FIXME: need detect whether function already exists
  auto func = new ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction();
  // FIXME: func_sig is not always in this pattern
  auto func_sig = func_name + ":opt";
  for (auto arg : args) {
    if (arg->has_scalar_function()) {
      func_sig =
          func_sig + "_" + TypeUtils::getStringType(arg->scalar_function().output_type());
    }
    if (arg->has_selection()) {
      // need names/types or schema availble
      auto field = arg->selection().direct_reference().struct_field().field();
      if (builder->schema_) {
        func_sig = func_sig + "_" +
                   TypeUtils::getStringType(builder->schema_->struct_().types(field));
      } else if (builder->names_.size() > field) {
        func_sig = func_sig + "_" + TypeUtils::getStringType(*builder->types_[field]);
      } else {
        CIDER_THROW(CiderCompileException,
                    "Failed to get type of field reference, check builder names/types or "
                    "schema.");
      }
    }
  }
  func->set_name(func_sig + "_" + TypeUtils::getStringType(*output_type));
  func->set_function_anchor(builder->func_anchor_);
  builder->func_anchor_ += 1;
  // TODO: Lookup the function in yaml file based on func_sig
  func->set_extension_uri_reference(2);
  builder->funcs_info_.push_back(func);
  return func;
}

::substrait::Expression* SubstraitExprBuilder::makeExpr(
    SubstraitExprBuilder* builder,
    std::string func_name,
    std::vector<::substrait::Expression*> args,
    ::substrait::Type* output_type) {
  auto expr = new ::substrait::Expression();
  auto expr_func = expr->mutable_scalar_function();
  expr_func->set_function_reference(
      makeFunc(builder, func_name, args, output_type)->function_anchor());
  for (auto arg : args) {
    expr_func->add_arguments()->mutable_value()->CopyFrom(*arg);
  }
  expr_func->mutable_output_type()->CopyFrom(*output_type);
  return expr;
}
