/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#include "function/FunctionLookup.h"

const FunctionDescriptorPtr FunctionLookup::lookupFunction(
    const FunctionSignature& function_signature) const {
  FunctionDescriptorPtr function_descriptor_ptr = nullptr;
  // add:int_int
  const std::string& func_sig = function_signature.func_sig;
  std::string func_name = func_sig.substr(0, func_sig.find_first_of(':'));
  const std::string& from_platform = function_signature.from_platform;
  function_descriptor_ptr = std::make_shared<FunctionDescriptor>();
  if (from_platform == "presto" || from_platform == "substrait") {
    auto iter = extension_ptr_map_.find(from_platform);
    if (iter == extension_ptr_map_.end()) {
      throw std::runtime_error(from_platform + " function look up is not yet supported");
    }

    // todo, presto find failed, should find substrait
    BasicFunctionLookUpContextPtr function_lookup_context_ptr = iter->second;
    SQLOpsPtr sql_scalar_op_ptr =
        function_lookup_context_ptr->lookupScalarFunctionSQLOp(func_name);
    SQLAggPtr sql_agg_op_ptr =
        function_lookup_context_ptr->lookupAggregateFunctionSQLOp(func_name);
    OpSupportExprTypePtr op_support_expr_type_ptr =
        function_lookup_context_ptr->lookupFunctionSupportType(func_name);
    function_descriptor_ptr->scalar_op_type_ptr = sql_scalar_op_ptr;
    function_descriptor_ptr->agg_op_type_ptr = sql_agg_op_ptr;
    function_descriptor_ptr->op_support_expr_type_ptr = op_support_expr_type_ptr;
    function_descriptor_ptr->func_sig = function_signature;
  } else {
    throw std::runtime_error(from_platform + " function look up is not yet supported");
  }
  return function_descriptor_ptr;
}
