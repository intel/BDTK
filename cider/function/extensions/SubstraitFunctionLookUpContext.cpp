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

#include "SubstraitFunctionLookUpContext.h"

std::shared_ptr<BasicFunctionLookUpContext> SubstraitFunctionLookUpContext::loadExtension(
    SubstraitFunctionMappingsPtr substrait_function_mappings) {
  std::vector<std::string> extensionFiles = {
      "substrait_extension.yaml",
  };
  const auto& extensionRootPath = getFunctionLookUpContextAbsolutePath("substrait");
  return loadExtension(extensionRootPath, extensionFiles, substrait_function_mappings);
}

std::shared_ptr<BasicFunctionLookUpContext> SubstraitFunctionLookUpContext::loadExtension(
    const std::string& basePath,
    const std::vector<std::string>& extensionFiles,
    SubstraitFunctionMappingsPtr substrait_function_mappings) {
  std::vector<std::string> yamlExtensionFiles;
  yamlExtensionFiles.reserve(extensionFiles.size());
  for (auto& extensionFile : extensionFiles) {
    const auto& extensionUri = basePath + "/" + extensionFile;
    yamlExtensionFiles.emplace_back(extensionUri);
  }
  return loadExtension(yamlExtensionFiles, substrait_function_mappings);
}

std::shared_ptr<BasicFunctionLookUpContext> SubstraitFunctionLookUpContext::loadExtension(
    const std::vector<std::string>& yamlExtensionFiles,
    SubstraitFunctionMappingsPtr substrait_function_mappings) {
  SubstraitFunctionLookUpContext* mergedSubstraitFunctionLookUpContext =
      new SubstraitFunctionLookUpContext;
  mergedSubstraitFunctionLookUpContext->scalar_op_map_ =
      substrait_function_mappings->scalarMappings();
  mergedSubstraitFunctionLookUpContext->agg_op_map_ =
      substrait_function_mappings->aggregateMappings();
  mergedSubstraitFunctionLookUpContext->op_support_type_map_ =
      substrait_function_mappings->opsSupportTypeMappings();
  for (const auto& extensionUri : yamlExtensionFiles) {
    const auto& substraitFunctionLookUpContext =
        YAML::LoadFile(extensionUri).as<SubstraitFunctionLookUpContext>();
    const auto& op_support_map = substraitFunctionLookUpContext.op_support_type_map_;
    mergedSubstraitFunctionLookUpContext->op_support_type_map_.insert(
        op_support_map.begin(), op_support_map.end());
  }
  BasicFunctionLookUpContextPtr ptr(mergedSubstraitFunctionLookUpContext);
  return ptr;
}
