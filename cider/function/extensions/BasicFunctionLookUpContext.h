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

#pragma once

#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "function/ExtensionFunction.h"
#include "function/SubstraitFunctionMappings.h"
#include "type/data/sqltypes.h"
#include "type/plan/Analyzer.h"
#include "util/sqldefs.h"

struct FunctionSignature {
  // add:int_int
  std::string func_sig;
  // presto
  std::string from_platform;
};

struct FunctionDescriptor {
  FunctionSignature func_sig;
  SQLOpsPtr scalar_op_type_ptr = nullptr;
  SQLAggPtr agg_op_type_ptr = nullptr;
  OpSupportExprTypePtr op_support_expr_type_ptr = nullptr;
};

using FunctionDescriptorPtr = std::shared_ptr<FunctionDescriptor>;

namespace {

std::string getFunctionLookUpContextAbsolutePath(std::string from_platform) {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/" + from_platform;
}

}  // namespace

/// class used to deserialize Substrait YAML extension files.
class BasicFunctionLookUpContext {
 public:
  virtual ~BasicFunctionLookUpContext(){};
  /// deserialize default Substrait extension.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      SubstraitFunctionMappingsPtr substrait_function_mappings) {
    return nullptr;
  }

  /// deserialize Substrait extension by given basePath and extensionFiles.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      const std::string& basePath,
      const std::vector<std::string>& extensionFiles,
      SubstraitFunctionMappingsPtr substrait_function_mappings) {
    return nullptr;
  }

  /// deserialize Substrait extension by given extensionFiles.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      const std::vector<std::string>& extensionFiles,
      SubstraitFunctionMappingsPtr substrait_function_mappings) {
    return nullptr;
  }

  /// lookup scalar function SQL op by given scalar function signature.
  SQLOpsPtr lookupScalarFunctionSQLOp(const std::string& signature) const;

  /// lookup aggregate function SQL op by given aggregate function signature.
  SQLAggPtr lookupAggregateFunctionSQLOp(const std::string& signature) const;

  /// lookup scalar or aggregate function support type by given function signature.
  OpSupportExprTypePtr lookupFunctionSupportType(const std::string& signature) const;

  /// lookup scalar function by given scalar function signature.
  ExtensionFunctionPtr lookupScalarFunction(const std::string& signature) const;

  /// lookup aggregate function by given aggregate function signature.
  ExtensionFunctionPtr lookupAggFunction(const std::string& signature) const;

  /// a collection of scalar function op loaded from Substrait extension
  /// yaml.
  std::unordered_map<std::string, SQLOps> scalar_op_map_;
  /// a collection of aggregate function op loaded from Substrait
  /// extension yaml.
  std::unordered_map<std::string, SQLAgg> agg_op_map_;
  /// a collection of aggregate function support type loaded from Substrait
  /// extension yaml.
  std::unordered_map<std::string, OpSupportExprType> op_support_type_map_;
  /// a collection of scalar function loaded from Substrait
  /// extension yaml.
  std::unordered_map<std::string, ScalarExtensionFunction> scalar_extension_function_map_;
  /// a collection of aggregate function loaded from Substrait
  /// extension yaml.
  std::unordered_map<std::string, AggregateExtensionFunction> agg_extension_function_map_;
};

using BasicFunctionLookUpContextPtr = std::shared_ptr<BasicFunctionLookUpContext>;

namespace YAML {

static bool decodeExtensionFunction(const Node& node, ScalarExtensionFunction& function) {
  auto& returnType = node["return"];
  if (returnType && returnType.IsScalar()) {
    /// return type can be an expression
    const auto& returnExpr = returnType.as<std::string>();
    std::stringstream ss(returnExpr);
    std::string lastReturnType;
    while (std::getline(ss, lastReturnType, '\n')) {
    }
    function.setRet(deserialize_type(lastReturnType));
    auto& args = node["args"];
    if (args && args.IsSequence()) {
      std::vector<ExtArgumentType> arg_type_vec;
      for (auto& arg : args) {
        if (arg["value"]) {  // value argument
          ExtArgumentType arg_type = arg.as<ExtArgumentType>();
          arg_type_vec.push_back(arg_type);
        }
      }
      function.setArgs(arg_type_vec);
    }
    return true;
  }
  return false;
}

static bool decodeExtensionFunction(const Node& node,
                                    AggregateExtensionFunction& function) {
  auto& returnType = node["return"];
  if (returnType && returnType.IsScalar()) {
    /// return type can be an expression
    const auto& returnExpr = returnType.as<std::string>();
    std::stringstream ss(returnExpr);
    std::string lastReturnType;
    while (std::getline(ss, lastReturnType, '\n')) {
    }
    function.setRet(deserialize_type(lastReturnType));
    auto& args = node["args"];
    if (args && args.IsSequence()) {
      std::vector<ExtArgumentType> arg_type_vec;
      for (auto& arg : args) {
        if (arg["value"]) {  // value argument
          ExtArgumentType arg_type = arg.as<ExtArgumentType>();
          arg_type_vec.push_back(arg_type);
        }
      }
      function.setArgs(arg_type_vec);
    }
    return true;
  }
  return false;
}

template <>
struct convert<ExtArgumentType> {
  static bool decode(const Node& node, ExtArgumentType& arg_type) {
    auto& value = node["value"];
    if (value && value.IsScalar()) {
      auto valueType = value.as<std::string>();
      arg_type = deserialize_type(valueType);
      return true;
    }
    return false;
  }
};

template <>
struct convert<ScalarExtensionFunctions> {
  static bool decode(const Node& node, ScalarExtensionFunctions& functions) {
    auto& name = node["name"];
    if (name && name.IsScalar()) {
      std::string function_name = name.as<std::string>();
      std::transform(
          function_name.begin(), function_name.end(), function_name.begin(), ::tolower);
      functions.setName(function_name);
      auto& impls = node["impls"];
      if (impls && impls.IsSequence() && impls.size() > 0) {
        std::vector<ScalarExtensionFunction> scalar_extensions;
        scalar_extensions.reserve(impls.size());
        for (auto& impl : impls) {
          ScalarExtensionFunction scalarExtensionFunction;
          scalarExtensionFunction.setName(function_name);
          if (!decodeExtensionFunction(impl, scalarExtensionFunction)) {
            return false;
          }
          scalar_extensions.push_back(scalarExtensionFunction);
        }
        functions.setExtensions(scalar_extensions);
      }
      return true;
    }
    return false;
  }
};

template <>
struct convert<AggregateExtensionFunctions> {
  static bool decode(const Node& node, AggregateExtensionFunctions& functions) {
    auto& name = node["name"];
    if (name && name.IsScalar()) {
      std::string function_name = name.as<std::string>();
      std::transform(
          function_name.begin(), function_name.end(), function_name.begin(), ::tolower);
      functions.setName(function_name);
      auto& impls = node["impls"];
      if (impls && impls.IsSequence() && impls.size() > 0) {
        std::vector<AggregateExtensionFunction> agg_extensions;
        agg_extensions.reserve(impls.size());
        for (auto& impl : impls) {
          AggregateExtensionFunction aggregateExtensionFunction;
          aggregateExtensionFunction.setName(function_name);
          if (!decodeExtensionFunction(impl, aggregateExtensionFunction)) {
            return false;
          }
          agg_extensions.push_back(aggregateExtensionFunction);
        }
        functions.setExtensions(agg_extensions);
      }
      return true;
    }
    return false;
  }
};

}  // namespace YAML
