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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "BasicFunctionLookUpContext.h"

/// class used to deserialize Substrait YAML extension files.
class SubstraitFunctionLookUpContext : public BasicFunctionLookUpContext {
 public:
  virtual ~SubstraitFunctionLookUpContext(){};
  /// deserialize default Substrait extension.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      SubstraitFunctionMappingsPtr substrait_function_mappings);

  /// deserialize Substrait extension by given basePath and extensionFiles.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      const std::string& basePath,
      const std::vector<std::string>& extensionFiles,
      SubstraitFunctionMappingsPtr substrait_function_mappings);

  /// deserialize Substrait extension by given extensionFiles.
  static std::shared_ptr<BasicFunctionLookUpContext> loadExtension(
      const std::vector<std::string>& extensionFiles,
      SubstraitFunctionMappingsPtr substrait_function_mappings);
};

using SubstraitFunctionLookUpContextPtr = std::shared_ptr<SubstraitFunctionLookUpContext>;

namespace YAML {

template <>
struct convert<SubstraitFunctionLookUpContext> {
  static bool decode(const Node& node, SubstraitFunctionLookUpContext& extension) {
    auto& scalarFunctions = node["scalar_functions"];
    auto& aggregateFunctions = node["aggregate_functions"];
    const bool scalarFunctionsExists = scalarFunctions && scalarFunctions.IsSequence();
    const bool aggregateFunctionsExists =
        aggregateFunctions && aggregateFunctions.IsSequence();
    if (!scalarFunctionsExists && !aggregateFunctionsExists) {
      return false;
    }

    if (scalarFunctionsExists) {
      for (auto& scalarFunctionNode : scalarFunctions) {
        const auto& scalar_functions = scalarFunctionNode.as<ScalarExtensionFunctions>();
        const std::string& function_name = scalar_functions.getName();
        extension.op_support_type_map_.insert(
            std::make_pair<std::string, OpSupportExprType>(
                function_name.c_str(), OpSupportExprType::FunctionOper));
        const std::vector<ScalarExtensionFunction>& scalar_extensions =
            scalar_functions.getExtensions();
        for (auto scalar_extension : scalar_extensions) {
          extension.scalar_extension_function_map_.insert(
              std::make_pair<std::string, ScalarExtensionFunction>(
                  function_name.c_str(), std::move(scalar_extension)));
        }
      }
    }

    if (aggregateFunctionsExists) {
      for (auto& aggregateFunctionNode : aggregateFunctions) {
        const auto& aggregate_functions =
            aggregateFunctionNode.as<AggregateExtensionFunctions>();
        const std::string& function_name = aggregate_functions.getName();
        extension.op_support_type_map_.insert(
            std::make_pair<std::string, OpSupportExprType>(
                function_name.c_str(), OpSupportExprType::FunctionOper));
        const std::vector<AggregateExtensionFunction>& agg_extensions =
            aggregate_functions.getExtensions();
        for (auto agg_extension : agg_extensions) {
          extension.agg_extension_function_map_.insert(
              std::make_pair<std::string, AggregateExtensionFunction>(
                  function_name.c_str(), std::move(agg_extension)));
        }
      }
    }

    return true;
  }
};

}  // namespace YAML
