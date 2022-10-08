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

#include "function/FunctionLookup.h"
#include "cider/CiderException.h"

void FunctionLookup::registerFunctionLookUpContext(const EngineType from_platform) {
  // Load cider support function default yaml files first.
  cider::function::substrait::SubstraitExtensionPtr cider_internal_function_ptr =
      cider::function::substrait::SubstraitExtension::loadExtension();
  // Load engine's extension function yaml files second.
  if (from_platform == EngineType::SubstraitEngine) {
    cider::function::substrait::SubstraitExtensionPtr substrait_extension_function_ptr =
        cider::function::substrait::SubstraitExtension::loadExtension(
            {getDataPath() + "/substrait/" + "substrait_extension.yaml"});
    cider::function::substrait::SubstraitFunctionMappingsPtr func_mappings =
        std::make_shared<const cider::function::substrait::SubstraitFunctionMappings>();
    scalar_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            cider_internal_function_ptr, func_mappings);
    aggregate_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitAggregateFunctionLookup>(
            cider_internal_function_ptr, func_mappings);
    extension_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            substrait_extension_function_ptr, func_mappings);
  } else if (from_platform == EngineType::PrestoEngine) {
    cider::function::substrait::SubstraitExtensionPtr presto_extension_function_ptr =
        cider::function::substrait::SubstraitExtension::loadExtension(
            {getDataPath() + "/presto/" + "presto_extension.yaml"});
    cider::function::substrait::SubstraitFunctionMappingsPtr presto_mappings =
        std::make_shared<
            const cider::function::substrait::VeloxToSubstraitFunctionMappings>();
    scalar_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            cider_internal_function_ptr, presto_mappings);
    aggregate_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitAggregateFunctionLookup>(
            cider_internal_function_ptr, presto_mappings);
    extension_function_look_up_ptr_ =
        std::make_shared<cider::function::substrait::SubstraitScalarFunctionLookup>(
            presto_extension_function_ptr, presto_mappings);
  } else {
    CIDER_THROW(CiderCompileException,
                fmt::format("Function lookup unsupported platform {}", from_platform));
  }
}

const SQLOpsPtr FunctionLookup::getFunctionScalarOp(
    const FunctionSignature& function_signature) const {
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      cider::function::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.return_type);
  const auto& functionOption =
      scalar_function_look_up_ptr_->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionScalarOp(func_name);
  }
  return nullptr;
}

const SQLAggPtr FunctionLookup::getFunctionAggOp(
    const FunctionSignature& function_signature) const {
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      cider::function::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.return_type);
  const auto& functionOption =
      aggregate_function_look_up_ptr_->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionAggOp(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getScalarFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      cider::function::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.return_type);
  const auto& functionOption =
      scalar_function_look_up_ptr_->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getAggFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      cider::function::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.return_type);
  const auto& functionOption =
      aggregate_function_look_up_ptr_->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return nullptr;
}

const OpSupportExprTypePtr FunctionLookup::getExtensionFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const auto& functionSignature =
      cider::function::substrait::SubstraitFunctionSignature::of(
          func_name, function_signature.arguments, function_signature.return_type);
  const auto& functionOption =
      extension_function_look_up_ptr_->lookupFunction(functionSignature);
  if (functionOption.has_value()) {
    return std::make_shared<OpSupportExprType>(OpSupportExprType::FunctionOper);
  }
  return nullptr;
}

/// first search extension function, second search internal function
const OpSupportExprTypePtr FunctionLookup::getFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  OpSupportExprTypePtr result_ptr = nullptr;
  result_ptr = getExtensionFunctionOpSupportType(function_signature);
  if (result_ptr) {
    return result_ptr;
  }
  result_ptr = getScalarFunctionOpSupportType(function_signature);
  if (result_ptr) {
    return result_ptr;
  }
  result_ptr = getAggFunctionOpSupportType(function_signature);
  return result_ptr;
}

const FunctionDescriptorPtr FunctionLookup::lookupFunction(
    const FunctionSignature& function_signature) const {
  FunctionDescriptorPtr function_descriptor_ptr = std::make_shared<FunctionDescriptor>();
  const EngineType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  function_descriptor_ptr->func_sig = function_signature;
  function_descriptor_ptr->op_support_expr_type_ptr =
      getFunctionOpSupportType(function_signature);
  if (function_descriptor_ptr->op_support_expr_type_ptr != nullptr &&
      *(function_descriptor_ptr->op_support_expr_type_ptr) ==
          OpSupportExprType::FunctionOper) {
    return function_descriptor_ptr;
  }
  function_descriptor_ptr->scalar_op_type_ptr = getFunctionScalarOp(function_signature);
  if (function_descriptor_ptr->scalar_op_type_ptr != nullptr) {
    return function_descriptor_ptr;
  }
  function_descriptor_ptr->agg_op_type_ptr = getFunctionAggOp(function_signature);
  return function_descriptor_ptr;
}
