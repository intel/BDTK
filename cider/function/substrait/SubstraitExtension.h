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

#ifndef CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITEXTENSION_H
#define CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITEXTENSION_H

#include <optional>
#include "function/substrait/SubstraitFunction.h"
#include "function/substrait/SubstraitFunctionMappings.h"
#include "function/substrait/SubstraitType.h"

namespace cider::function::substrait {

/// class used to deserialize substrait YAML extension files.
class SubstraitExtension {
 public:
  /// deserialize default substrait extension.
  static std::shared_ptr<SubstraitExtension> loadExtension();

  /// deserialize substrait extension by given basePath and extensionFiles.
  static std::shared_ptr<SubstraitExtension> loadExtension(
      const std::string& basePath,
      const std::vector<std::string>& extensionFiles);

  /// deserialize substrait extension by given extensionFiles.
  static std::shared_ptr<SubstraitExtension> loadExtension(
      const std::vector<std::string>& extensionFiles);

  /// lookup scalar function by given scalar function signature.
  std::optional<SubstraitFunctionVariantPtr> lookupScalarFunction(
      const std::string& signature) const;

  /// lookup aggregate function by given aggregate function signature.
  std::optional<SubstraitFunctionVariantPtr> lookupAggregateFunction(
      const std::string& signature) const;

  /// lookup scalar or aggregate function by given function signature.
  std::optional<SubstraitFunctionVariantPtr> lookupFunction(
      const std::string& signature) const;

  /// lookup scalar or aggregate function by given function signature and
  /// function mappings.
  std::optional<SubstraitFunctionVariantPtr> lookupFunction(
      const SubstraitFunctionMappingsPtr& functionMappings,
      const std::string& signature) const;

  /// a collection of scalar function variants loaded from Substrait extension
  /// yaml.
  std::vector<SubstraitFunctionVariantPtr> scalarFunctionVariants;
  /// a collection of aggregate function variants loaded from Substrait
  /// extension yaml.
  std::vector<SubstraitFunctionVariantPtr> aggregateFunctionVariants;

  /// substrait user defined types loaded from Substrait extension yaml.
  std::vector<SubstraitTypeAnchorPtr> types;

 private:
  /// deserialize default substrait extension.
  static std::shared_ptr<SubstraitExtension> loadDefault();
};

using SubstraitExtensionPtr = std::shared_ptr<const SubstraitExtension>;

}  // namespace cider::function::substrait

#endif  // CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITEXTENSION_H
