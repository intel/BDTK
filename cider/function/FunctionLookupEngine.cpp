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

FunctionLookupEnginePtrMap FunctionLookupEngine::function_lookup_engine_ptr_map_ = {};

const FunctionLookupEngine* FunctionLookupEngine::getInstance(
    const PlatformType from_platform) {
  if (from_platform == PlatformType::SubstraitPlatform ||
      from_platform == PlatformType::PrestoPlatform) {
    if (function_lookup_engine_ptr_map_.find(from_platform) ==
        function_lookup_engine_ptr_map_.end()) {
      function_lookup_engine_ptr_map_.insert(
          std::pair(from_platform, new FunctionLookupEngine(from_platform)));
    }
    return function_lookup_engine_ptr_map_[from_platform];
  } else {
    CIDER_THROW(CiderCompileException,
                fmt::format("Function lookup unsupported platform {}", from_platform));
  }
  return nullptr;
}

void FunctionLookupEngine::registerFunctionLookUpContext(
    const PlatformType from_platform) {
  // Load cider support function default yaml files first.
  /*io::substrait::ExtensionPtr cider_internal_function_ptr =
      io::substrait::Extension::load();*/
  const std::vector<std::string> internal_files = {
      "functions_aggregate_approx.yaml",
      "functions_aggregate_generic.yaml",
      "functions_arithmetic.yaml",
      "functions_arithmetic_decimal.yaml",
      "functions_boolean.yaml",
      "functions_comparison.yaml",
      "functions_datetime.yaml",
      "functions_logarithmic.yaml",
      "functions_rounding.yaml",
      "functions_string.yaml",
      "functions_set.yaml",
      "unknown.yaml",
  };
  std::vector<std::string> internal_files_path_vec;
  internal_files_path_vec.reserve(internal_files.size());
  for (const auto& internal_file : internal_files) {
    internal_files_path_vec.push_back(
        fmt::format("{}/{}/{}", getDataPath(), "internals", internal_file));
  }
  io::substrait::ExtensionPtr cider_internal_function_ptr =
      io::substrait::Extension::load(internal_files_path_vec);
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
  io::substrait::ExtensionPtr extension_function_ptr =
      io::substrait::Extension::load({fmt::format("{}/{}/{}/{}",
                                                  getDataPath(),
                                                  "extensions",
                                                  platform_name,
                                                  yaml_extension_filename)});
  io::substrait::FunctionMappingPtr func_mappings = std::make_shared<const T>();
  scalar_function_look_up_ptr_ = std::make_shared<io::substrait::ScalarFunctionLookup>(
      cider_internal_function_ptr, func_mappings);
  aggregate_function_look_up_ptr_ =
      std::make_shared<io::substrait::AggregateFunctionLookup>(
          cider_internal_function_ptr, func_mappings);
  extension_function_look_up_ptr_ = std::make_shared<io::substrait::ScalarFunctionLookup>(
      extension_function_ptr, func_mappings);
  function_mapping_ptr_ = func_mappings;
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
    return function_mappings_->getFunctionScalarOp(function_variant_ptr->name);
  }
  return SQLOps::kUNDEFINED_OP;
}

const SqlStringOpKind FunctionLookupEngine::getFunctionStringOp(
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
    return function_mappings_->getFunctionStringOp(function_variant_ptr->name);
  }
  return SqlStringOpKind::kUNDEFINED_STRING_OP;
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
    return function_mappings_->getFunctionAggOp(function_variant_ptr->name);
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
    return function_mappings_->getFunctionOpSupportType(function_variant_ptr->name);
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
    return function_mappings_->getFunctionOpSupportType(function_variant_ptr->name);
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
  auto extension_function_op_support_type_result =
      getExtensionFunctionOpSupportType(function_signature);
  if (extension_function_op_support_type_result != OpSupportExprType::kUNDEFINED_EXPR) {
    return extension_function_op_support_type_result;
  }
  auto scalar_function_op_support_type_result =
      getScalarFunctionOpSupportType(function_signature);
  if (scalar_function_op_support_type_result != OpSupportExprType::kUNDEFINED_EXPR) {
    return scalar_function_op_support_type_result;
  }
  auto agg_function_op_support_type_result =
      getAggFunctionOpSupportType(function_signature);
  return agg_function_op_support_type_result;
}

const std::string FunctionLookupEngine::getRealFunctionName(
    const std::string& function_name) const {
  const auto& scalar_function_mappings = function_mapping_ptr_->scalaMapping();
  if (scalar_function_mappings.find(function_name) != scalar_function_mappings.end()) {
    return scalar_function_mappings.at(function_name);
  }
  const auto& aggregate_function_mappings = function_mapping_ptr_->aggregateMapping();
  if (aggregate_function_mappings.find(function_name) !=
      aggregate_function_mappings.end()) {
    return aggregate_function_mappings.at(function_name);
  }
  const auto& window_function_mappings = function_mapping_ptr_->windowMapping();
  if (window_function_mappings.find(function_name) != window_function_mappings.end()) {
    return window_function_mappings.at(function_name);
  }
  return function_name;
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
  const std::string& function_name = function_signature.func_name;
  auto real_function_name = getRealFunctionName(function_name);
  FunctionSignature function_signature_result = function_signature;
  function_signature_result.func_name = real_function_name;
  function_descriptor.func_sig = function_signature_result;
  auto funtion_op_support_type_result = getFunctionOpSupportType(function_signature);
  function_descriptor.op_support_expr_type = funtion_op_support_type_result;
  if (funtion_op_support_type_result != OpSupportExprType::kUNDEFINED_EXPR) {
    function_descriptor.is_cider_support_function = true;
  }
  // extension function, no need to look up internal scalar and agg
  if (funtion_op_support_type_result == OpSupportExprType::kFUNCTION_OPER) {
    return function_descriptor;
  }
  auto funtion_scalar_op_result = getFunctionScalarOp(function_signature);
  function_descriptor.scalar_op_type = funtion_scalar_op_result;
  if (funtion_scalar_op_result != SQLOps::kUNDEFINED_OP) {
    function_descriptor.is_cider_support_function = true;
    return function_descriptor;
  }
  auto funtion_string_op_result = getFunctionStringOp(function_signature);
  function_descriptor.string_op_type = funtion_string_op_result;
  if (funtion_string_op_result != SqlStringOpKind::kUNDEFINED_STRING_OP) {
    function_descriptor.is_cider_support_function = true;
    return function_descriptor;
  }
  auto funtion_agg_op_result = getFunctionAggOp(function_signature);
  function_descriptor.agg_op_type = funtion_agg_op_result;
  if (funtion_agg_op_result != SQLAgg::kUNDEFINED_AGG) {
    function_descriptor.is_cider_support_function = true;
  }
  return function_descriptor;
}

const FunctionDescriptor FunctionLookupEngine::lookupFunction(
    const std::string& function_signature_str,
    const std::string& function_return_type_str,
    const PlatformType& from_platform) const {
  FunctionDescriptor function_descriptor;
  std::string function_name;
  auto pos = function_signature_str.find_first_of(':');
  if (pos == std::string::npos) {
    // count(*)/count(1), front end maybe just give count as function_signature_str
    if (function_signature_str == "count") {
      function_name = function_signature_str;
    } else {
      CIDER_THROW(CiderCompileException,
                  "Invalid function_sig: " + function_signature_str);
    }
  } else {
    function_name = function_signature_str.substr(0, pos);
  }
  auto function_args =
      function_signature_str.substr(pos + 1, function_signature_str.length());
  std::vector<std::string> function_args_vec = split(function_args, "_");
  FunctionSignature function_signature;
  function_signature.from_platform = from_platform;
  function_signature.func_name = function_name;
  std::vector<io::substrait::TypePtr> arguments_vec;
  for (const auto& arg_str : function_args_vec) {
    if (arg_str == "req" || arg_str == "opt") {
      continue;
    }
    arguments_vec.push_back(getArgueTypePtr(arg_str));
  }
  function_signature.arguments = arguments_vec;
  function_signature.return_type = getArgueTypePtr(function_return_type_str);
  function_descriptor = lookupFunction(function_signature);
  return function_descriptor;
}

const io::substrait::TypePtr FunctionLookupEngine::getArgueTypePtr(
    const std::string& argue_type_str) const {
  io::substrait::TypePtr result_ptr = nullptr;
  if (argue_type_str == "varchar" || argue_type_str == "vchar") {
    result_ptr = io::substrait::Type::decode("varchar<L1>");
  } else if (argue_type_str == "fixedchar" || argue_type_str == "fchar") {
    result_ptr = io::substrait::Type::decode("fixedchar<L1>");
  } else if (argue_type_str == "fixedbinary" || argue_type_str == "fbin") {
    result_ptr = io::substrait::Type::decode("fixedbinary<L1>");
  } else if (argue_type_str == "decimal" || argue_type_str == "dec") {
    result_ptr = io::substrait::Type::decode("decimal<P,S>");
  } else if (argue_type_str == "struct") {
    result_ptr = io::substrait::Type::decode("struct<fp64,i64>");
  } else if (argue_type_str == "bool") {
    result_ptr = io::substrait::Type::decode("boolean");
  } else if (argue_type_str == "int8") {
    result_ptr = io::substrait::Type::decode("i8");
  } else if (argue_type_str == "int16") {
    result_ptr = io::substrait::Type::decode("i16");
  } else if (argue_type_str == "int32") {
    result_ptr = io::substrait::Type::decode("i32");
  } else if (argue_type_str == "int64") {
    result_ptr = io::substrait::Type::decode("i64");
  } else if (argue_type_str == "str") {
    result_ptr = io::substrait::Type::decode("string");
  } else if (argue_type_str == "year" || argue_type_str == "iyear") {
    result_ptr = io::substrait::Type::decode("interval_year");
  } else if (argue_type_str == "day" || argue_type_str == "iday") {
    result_ptr = io::substrait::Type::decode("interval_day");
  } else if (argue_type_str == "ts") {
    result_ptr = io::substrait::Type::decode("timestamp");
  } else if (argue_type_str == "tstz") {
    result_ptr = io::substrait::Type::decode("timestamp_tz");
  } else {
    result_ptr = io::substrait::Type::decode(argue_type_str);
  }
  return result_ptr;
}
