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

#define LOAD_EXTENSION_YAML_AND_INITIALIZE_FUNCTION_LOOK_UP(                            \
    TYPE_NAME, TYPE_YAML_FILE_PATH, FUNCTION_MAPPING_TYPE)                              \
  io::substrait::ExtensionPtr extension_function_ptr = io::substrait::Extension::load(  \
      {getDataPath() + "/" #TYPE_NAME "/" + "" #TYPE_YAML_FILE_PATH ""});               \
  io::substrait::FunctionMappingPtr func_mappings =                                     \
      std::make_shared<const FUNCTION_MAPPING_TYPE>();                                  \
  scalar_function_look_up_ptr_ = std::make_shared<io::substrait::ScalarFunctionLookup>( \
      cider_internal_function_ptr, func_mappings);                                      \
  aggregate_function_look_up_ptr_ =                                                     \
      std::make_shared<io::substrait::AggregateFunctionLookup>(                         \
          cider_internal_function_ptr, func_mappings);                                  \
  extension_function_look_up_ptr_ =                                                     \
      std::make_shared<io::substrait::ScalarFunctionLookup>(extension_function_ptr,     \
                                                            func_mappings);

void FunctionLookupEngine::registerFunctionLookUpContext(
    const PlatformType from_platform) {
  // Load cider support function default yaml files first.
  io::substrait::ExtensionPtr cider_internal_function_ptr =
      io::substrait::Extension::load();
  // Load engine's extension function yaml files second.
  if (from_platform == PlatformType::SubstraitPlatform) {
    LOAD_EXTENSION_YAML_AND_INITIALIZE_FUNCTION_LOOK_UP(
        substrait, substrait_extension.yaml, io::substrait::FunctionMapping)
  } else if (from_platform == PlatformType::PrestoPlatform) {
    LOAD_EXTENSION_YAML_AND_INITIALIZE_FUNCTION_LOOK_UP(
        presto, presto_extension.yaml, io::substrait::FunctionMapping)
  } else {
    CIDER_THROW(CiderCompileException,
                fmt::format("Function lookup unsupported platform {}", from_platform));
  }
}

const SQLOps FunctionLookupEngine::getFunctionScalarOp(
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
    return function_mappings_->getFunctionScalarOp(func_name);
  }
  return SQLOps::kUNDEFINED_OP;
}

const SQLAgg FunctionLookupEngine::getFunctionAggOp(
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
    return function_mappings_->getFunctionAggOp(func_name);
  }
  return SQLAgg::kUNDEFINED_AGG;
}

const OpSupportExprType FunctionLookupEngine::getScalarFunctionOpSupportType(
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
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return OpSupportExprType::kUNDEFINED_EXPR;
}

const OpSupportExprType FunctionLookupEngine::getAggFunctionOpSupportType(
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
    return function_mappings_->getFunctionOpSupportType(func_name);
  }
  return OpSupportExprType::kUNDEFINED_EXPR;
}

const OpSupportExprType FunctionLookupEngine::getExtensionFunctionOpSupportType(
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
    return OpSupportExprType::kFUNCTION_OPER;
  }
  return OpSupportExprType::kUNDEFINED_EXPR;
}

/// first search extension function, second search internal function
const OpSupportExprType FunctionLookupEngine::getFunctionOpSupportType(
    const FunctionSignature& function_signature) const {
  OpSupportExprType result = OpSupportExprType::kUNDEFINED_EXPR;
  result = getExtensionFunctionOpSupportType(function_signature);
  if (result != OpSupportExprType::kUNDEFINED_EXPR) {
    return result;
  }
  result = getScalarFunctionOpSupportType(function_signature);
  if (result != OpSupportExprType::kUNDEFINED_EXPR) {
    return result;
  }
  result = getAggFunctionOpSupportType(function_signature);
  return result;
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
  function_descriptor.op_support_expr_type = getFunctionOpSupportType(function_signature);
  if (function_descriptor.op_support_expr_type == OpSupportExprType::kFUNCTION_OPER) {
    return function_descriptor;
  }
  function_descriptor.scalar_op_type = getFunctionScalarOp(function_signature);
  if (function_descriptor.scalar_op_type != SQLOps::kUNDEFINED_OP) {
    return function_descriptor;
  }
  function_descriptor.agg_op_type = getFunctionAggOp(function_signature);
  return function_descriptor;
}
