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
 * @file    SubstraitExprBuilder.h
 * @brief   Provide utils methods for building subtrait expression
 **/

#pragma once
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"

class SubstraitExprBuilder {
 public:
  SubstraitExprBuilder()
      : func_anchor_(0), names_{}, types_{}, schema_(nullptr), funcs_info_{} {}
  static ::substrait::Type* makeType(bool isNullable);

  static ::substrait::NamedStruct* makeNamedStruct(SubstraitExprBuilder* builder,
                                                   std::vector<std::string> names,
                                                   std::vector<::substrait::Type*> types);

  static ::substrait::Expression* makeFieldReference(size_t field);

  static ::substrait::Expression* makeFieldReference(SubstraitExprBuilder* builder,
                                                     std::string name,
                                                     ::substrait::Type* type);
  // TODO: add makeLiteral()

  static ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction* makeFunc(
      SubstraitExprBuilder* builder,
      std::string func_name,
      std::vector<::substrait::Expression*> args,
      ::substrait::Type* output_type);

  static ::substrait::Expression* makeExpr(SubstraitExprBuilder* builder,
                                           std::string func_name,
                                           std::vector<::substrait::Expression*> args,
                                           ::substrait::Type* output_type);

  ::substrait::NamedStruct* schema();

  std::vector<::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
  funcsInfo() {
    return funcs_info_;
  }

 private:
  size_t func_anchor_;
  std::vector<std::string> names_;
  std::vector<::substrait::Type*> types_;
  ::substrait::NamedStruct* schema_;
  std::vector<::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
      funcs_info_;
};
