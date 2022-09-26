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

void FunctionLookup::registerFunctionLookUpContext(
    SubstraitFunctionMappingsPtr function_mappings) {
  // internal scalar function
  scalar_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "substrait",
          std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
              cider_internal_function_ptr_, substrait_mappings_)));
  scalar_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "presto",
          std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
              cider_internal_function_ptr_, presto_mappings_)));
  // internal aggregate function
  aggregate_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "substrait",
          std::make_shared<facebook::velox::substrait::SubstraitAggregateFunctionLookup>(
              cider_internal_function_ptr_, substrait_mappings_)));
  aggregate_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "presto",
          std::make_shared<facebook::velox::substrait::SubstraitAggregateFunctionLookup>(
              cider_internal_function_ptr_, presto_mappings_)));
  // extension function
  extension_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "substrait",
          std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
              substrait_extension_function_ptr_, substrait_mappings_)));
  extension_function_look_up_ptr_map_.insert(
      std::make_pair<std::string, SubstraitFunctionLookupPtr>(
          "presto",
          std::make_shared<facebook::velox::substrait::SubstraitScalarFunctionLookup>(
              presto_extension_function_ptr_, presto_mappings_)));
}

const SQLOpsPtr FunctionLookup::getFunctionScalarOp(
    const FunctionSignature& function_signature) const {
  const std::string& from_platform = function_signature.from_platform;
  auto iter = scalar_function_look_up_ptr_map_.find(from_platform);
  if (iter == scalar_function_look_up_ptr_map_.end()) {
    return nullptr;
  }
  SubstraitFunctionLookupPtr scalar_function_look_up_ptr = iter->second;
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      facebook::velox::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.returnType);
  const auto& functionOption =
      scalar_function_look_up_ptr->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionScalarOp(func_name);
  }
  return nullptr;
}

const SQLAggPtr FunctionLookup::getFunctionAggOp(
    const FunctionSignature& function_signature) const {
  const std::string& from_platform = function_signature.from_platform;
  auto iter = aggregate_function_look_up_ptr_map_.find(from_platform);
  if (iter == aggregate_function_look_up_ptr_map_.end()) {
    return nullptr;
  }
  SubstraitFunctionLookupPtr agg_function_look_up_ptr = iter->second;
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      facebook::velox::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.returnType);
  const auto& functionOption =
      agg_function_look_up_ptr->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionAggOp(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getScalarFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const std::string& from_platform = function_signature.from_platform;
  auto iter = scalar_function_look_up_ptr_map_.find(from_platform);
  if (iter == scalar_function_look_up_ptr_map_.end()) {
    return nullptr;
  }
  SubstraitFunctionLookupPtr scalar_function_look_up_ptr = iter->second;
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      facebook::velox::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.returnType);
  const auto& functionOption =
      scalar_function_look_up_ptr->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getAggFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const std::string& from_platform = function_signature.from_platform;
  auto iter = aggregate_function_look_up_ptr_map_.find(from_platform);
  if (iter == aggregate_function_look_up_ptr_map_.end()) {
    return nullptr;
  }
  SubstraitFunctionLookupPtr agg_function_look_up_ptr = iter->second;
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      facebook::velox::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.returnType);
  const auto& functionOption =
      agg_function_look_up_ptr->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getExtensionFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const std::string& from_platform = function_signature.from_platform;
  auto iter = extension_function_look_up_ptr_map_.find(from_platform);
  if (iter == extension_function_look_up_ptr_map_.end()) {
    return nullptr;
  }
  SubstraitFunctionLookupPtr extension_function_look_up_ptr = iter->second;
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      facebook::velox::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.returnType);
  const auto& functionOption =
      extension_function_look_up_ptr->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return std::make_shared<OpSupportExprType>(OpSupportExprType::FunctionOper);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  OpSupportExprTypePtr result_ptr = nullptr;
  result_ptr = getScalarFunctionOpSupportType(function_signature);
  if (result_ptr) {
    return result_ptr;
  }
  result_ptr = getAggFunctionOpSupportType(function_signature);
  if (result_ptr) {
    return result_ptr;
  }
  result_ptr = getExtensionFunctionOpSupportType(function_signature);
  return result_ptr;
}

const FunctionDescriptorPtr FunctionLookup::lookupFunction(
    const FunctionSignature& function_signature) const {
  FunctionDescriptorPtr function_descriptor_ptr = std::make_shared<FunctionDescriptor>();
  const std::string& from_platform = function_signature.from_platform;
  if (from_platform == "presto" || from_platform == "substrait") {
    function_descriptor_ptr->scalar_op_type_ptr = getFunctionScalarOp(function_signature);
    function_descriptor_ptr->agg_op_type_ptr = getFunctionAggOp(function_signature);
    function_descriptor_ptr->op_support_expr_type_ptr =
        getFunctionOpSupportType(function_signature);
    function_descriptor_ptr->func_sig = function_signature;
  } else {
    throw std::runtime_error(from_platform + " function look up is not yet supported");
  }
  return function_descriptor_ptr;
}
