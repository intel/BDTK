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
 * @file    SubstraitExprBuilder.h
 * @brief   Provide utils methods for building subtrait expression
 **/

#pragma once
#include "substrait/algebra.pb.h"
#include "substrait/extended_expression.pb.h"
#include "substrait/type.pb.h"

struct ScalarExprRef {
  ::substrait::Expression* expr;
  std::string output_names;
};

struct AggregationExprRef {
  ::substrait::AggregateFunction* expr;
  std::string output_names;
};

class SubstraitExprBuilder {
 public:
  SubstraitExprBuilder(std::vector<std::string> names,
                       std::vector<::substrait::Type*> types);

  ::substrait::Expression* makeFieldReference(size_t field);

  ::substrait::Expression* makeFieldReference(std::string name);

  ::substrait::ExtendedExpression* build(std::vector<ScalarExprRef> expr_refs);

  ::substrait::ExtendedExpression* build(std::vector<AggregationExprRef> expr_refs);

  ::substrait::Expression* makeScalarExpr(std::string func_name,
                                          std::vector<::substrait::Expression*> args,
                                          ::substrait::Type* output_type);

  ::substrait::AggregateFunction* makeAggExpr(
      std::string func_name,
      std::vector<::substrait::Expression*> args,
      ::substrait::Type* output_type,
      ::substrait::AggregationPhase agg_phase =
          ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE,
      ::substrait::AggregateFunction_AggregationInvocation agg_invoc =
          ::substrait::AggregateFunction_AggregationInvocation::
              AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_ALL);

  [[deprecated]] ::substrait::NamedStruct* getSchema();

  [[deprecated]] std::vector<
      ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
  funcsInfo() {
    return funcs_info_;
  }

 private:
  ::substrait::NamedStruct* makeNamedStruct(std::vector<std::string> names,
                                            std::vector<::substrait::Type*> types);
  ::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction* makeFunc(
      std::string func_name,
      std::vector<::substrait::Expression*> args,
      ::substrait::Type* output_type);

  size_t func_anchor_;
  std::vector<std::string> names_;
  std::vector<::substrait::Type*> types_;
  std::vector<std::string> func_sigs;
  ::substrait::NamedStruct* schema_;
  std::vector<::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
      funcs_info_;
};
