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

#include "function/FunctionLookupEngine.h"
#include "cider/CiderException.h"

void FunctionLookupEngine::registerFunctionLookUpContext(
    const PlatformType from_platform) {
  // Load cider support function default yaml files first.
  io::substrait::ExtensionPtr cider_internal_function_ptr =
      io::substrait::Extension::load();
  // Load engine's extension function yaml files second.
  if (from_platform == PlatformType::SubstraitPlatform) {
    loadExtensionYamlAndInitializeFunctionLookup<io::substrait::FunctionMapping>(
        "substrait", "substrait_extension.yaml", cider_internal_function_ptr);
  } else if (from_platform == PlatformType::PrestoPlatform) {
    loadExtensionYamlAndInitializeFunctionLookup<io::substrait::VeloxFunctionMappings>(
        "presto", "presto_extension.yaml", cider_internal_function_ptr);
  } else {
    CIDER_THROW(CiderCompileException,
                fmt::format("Function lookup unsupported platform {}", from_platform));
  }
}

template <typename T>
void FunctionLookupEngine::loadExtensionYamlAndInitializeFunctionLookup(
    std::string platform_name,
    std::string yaml_extension_filename,
    const io::substrait::ExtensionPtr& cider_internal_function_ptr) {
  io::substrait::ExtensionPtr extension_function_ptr = io::substrait::Extension::load(
      {fmt::format("{}/{}/{}", getDataPath(), platform_name, yaml_extension_filename)});
  io::substrait::FunctionMappingPtr func_mappings = std::make_shared<const T>();
  scalar_function_look_up_ptr_ = std::make_shared<io::substrait::ScalarFunctionLookup>(
      cider_internal_function_ptr, func_mappings);
  aggregate_function_look_up_ptr_ =
      std::make_shared<io::substrait::AggregateFunctionLookup>(
          cider_internal_function_ptr, func_mappings);
  extension_function_look_up_ptr_ = std::make_shared<io::substrait::ScalarFunctionLookup>(
      extension_function_ptr, func_mappings);
}

std::tuple<const SQLOps, const std::string> FunctionLookupEngine::getFunctionScalarOp(
    const FunctionSignature& function_signature) const {
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const std::vector<io::substrait::TypePtr>& arguments = function_signature.arguments;
  const io::substrait::TypePtr& return_type = function_signature.return_type;
  const auto& function_variant_ptr =
      scalar_function_look_up_ptr_->lookupFunction({func_name, arguments, return_type});
  if (function_variant_ptr) {
    return {function_mappings_->getFunctionScalarOp(function_variant_ptr->name),
            function_variant_ptr->name};
  }
  return {SQLOps::kUNDEFINED_OP, func_name};
}

std::tuple<const SQLAgg, const std::string> FunctionLookupEngine::getFunctionAggOp(
    const FunctionSignature& function_signature) const {
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const std::vector<io::substrait::TypePtr>& arguments = function_signature.arguments;
  const io::substrait::TypePtr& return_type = function_signature.return_type;
  const auto& function_variant_ptr = aggregate_function_look_up_ptr_->lookupFunction(
      {func_name, arguments, return_type});
  if (function_variant_ptr) {
    return {function_mappings_->getFunctionAggOp(function_variant_ptr->name),
            function_variant_ptr->name};
  }
  return {SQLAgg::kUNDEFINED_AGG, func_name};
}

std::tuple<const OpSupportExprType, const std::string>
FunctionLookupEngine::getScalarFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const std::vector<io::substrait::TypePtr>& arguments = function_signature.arguments;
  const io::substrait::TypePtr& return_type = function_signature.return_type;
  const auto& function_variant_ptr =
      scalar_function_look_up_ptr_->lookupFunction({func_name, arguments, return_type});
  if (function_variant_ptr) {
    return {function_mappings_->getFunctionOpSupportType(function_variant_ptr->name),
            function_variant_ptr->name};
  }
  return {OpSupportExprType::kUNDEFINED_EXPR, func_name};
}

std::tuple<const OpSupportExprType, const std::string>
FunctionLookupEngine::getAggFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const std::vector<io::substrait::TypePtr>& arguments = function_signature.arguments;
  const io::substrait::TypePtr& return_type = function_signature.return_type;
  const auto& function_variant_ptr = aggregate_function_look_up_ptr_->lookupFunction(
      {func_name, arguments, return_type});
  if (function_variant_ptr) {
    return {function_mappings_->getFunctionOpSupportType(function_variant_ptr->name),
            function_variant_ptr->name};
  }
  return {OpSupportExprType::kUNDEFINED_EXPR, func_name};
}

std::tuple<const OpSupportExprType, const std::string>
FunctionLookupEngine::getExtensionFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  const std::string& func_name = function_signature.func_name;
  const std::vector<io::substrait::TypePtr>& arguments = function_signature.arguments;
  const io::substrait::TypePtr& return_type = function_signature.return_type;
  const auto& function_variant_ptr = extension_function_look_up_ptr_->lookupFunction(
      {func_name, arguments, return_type});
  if (function_variant_ptr) {
    return {OpSupportExprType::kFUNCTION_OPER, function_variant_ptr->name};
  }
  return {OpSupportExprType::kUNDEFINED_EXPR, func_name};
}

/// first search extension function, second search internal function
std::tuple<const OpSupportExprType, const std::string>
FunctionLookupEngine::getFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  auto extension_function_op_support_type_result =
      getExtensionFunctionOpSupportType(function_signature);
  if (std::get<0>(extension_function_op_support_type_result) !=
      OpSupportExprType::kUNDEFINED_EXPR) {
    return extension_function_op_support_type_result;
  }
  auto scalar_function_op_support_type_result =
      getScalarFunctionOpSupportType(function_signature);
  if (std::get<0>(scalar_function_op_support_type_result) !=
      OpSupportExprType::kUNDEFINED_EXPR) {
    return scalar_function_op_support_type_result;
  }
  auto agg_function_op_support_type_result =
      getAggFunctionOpSupportType(function_signature);
  return agg_function_op_support_type_result;
}

const FunctionDescriptor FunctionLookupEngine::lookupFunction(
    const FunctionSignature& function_signature) const {
  FunctionDescriptor function_descriptor;
  const PlatformType& from_platform = function_signature.from_platform;
  if (from_platform != from_platform_) {
    CIDER_THROW(
        CiderCompileException,
        fmt::format(
            "Platform of target function is {}, mismatched with registered platform {}",
            from_platform,
            from_platform_));
  }
  function_descriptor.func_sig = function_signature;
  auto funtion_op_support_type_result = getFunctionOpSupportType(function_signature);
  function_descriptor.op_support_expr_type = std::get<0>(funtion_op_support_type_result);
  function_descriptor.func_sig.func_name = std::get<1>(funtion_op_support_type_result);
  if (std::get<0>(funtion_op_support_type_result) == OpSupportExprType::kFUNCTION_OPER) {
    return function_descriptor;
  }
  auto funtion_scalar_op_result = getFunctionScalarOp(function_signature);
  function_descriptor.scalar_op_type = std::get<0>(funtion_scalar_op_result);
  function_descriptor.func_sig.func_name = std::get<1>(funtion_scalar_op_result);
  if (std::get<0>(funtion_scalar_op_result) != SQLOps::kUNDEFINED_OP) {
    return function_descriptor;
  }
  auto funtion_agg_op_result = getFunctionAggOp(function_signature);
  function_descriptor.agg_op_type = std::get<0>(funtion_agg_op_result);
  function_descriptor.func_sig.func_name = std::get<1>(funtion_agg_op_result);
  return function_descriptor;
}

const FunctionDescriptor FunctionLookupEngine::lookupFunction(
    const std::string& function_signature_str,
    const PlatformType& from_platform) const {
  FunctionDescriptor function_descriptor;
  auto function_name =
      function_signature_str.substr(0, function_signature_str.find_first_of(':'));
  auto function_args = function_signature_str.substr(
      function_signature_str.find_first_of(':') + 1, function_signature_str.length());
  std::vector<std::string> function_args_vec = split(function_args, "_");
  FunctionSignature function_signature;
  function_signature.from_platform = from_platform;
  function_signature.func_name = function_name;
  std::vector<io::substrait::TypePtr> arguments_vec;
  // no need to verify return type
  io::substrait::TypePtr return_type =
      std::make_shared<const io::substrait::ScalarType<io::substrait::TypeKind::kBool>>();
  for (const auto& arg_str : function_args_vec) {
    if (arg_str == "req" || arg_str == "opt") {
      continue;
    }
    const auto type_ptr = io::substrait::Type::decode(arg_str);
    arguments_vec.push_back(type_ptr);
  }
  function_signature.arguments = arguments_vec;
  function_signature.return_type = return_type;
  function_descriptor = lookupFunction(function_signature);
  return function_descriptor;
}
