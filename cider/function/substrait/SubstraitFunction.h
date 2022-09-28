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

#ifndef CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTION_H
#define CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTION_H

#include "function/substrait/SubstraitType.h"

namespace cider::function::substrait {

struct SubstraitFunctionArgument {
  /// whether the argument is required or not.
  virtual const bool isRequired() const = 0;
  /// convert argument type to short type string based on
  /// https://substrait.io/extensions/#function-signature-compound-names
  virtual const std::string toTypeString() const = 0;

  virtual const bool isWildcardType() const { return false; };

  virtual const bool isValueArgument() const { return false; }
};

using SubstraitFunctionArgumentPtr = std::shared_ptr<SubstraitFunctionArgument>;

struct SubstraitEnumArgument : public SubstraitFunctionArgument {
  bool required;
  bool const isRequired() const override { return required; }

  const std::string toTypeString() const override { return required ? "req" : "opt"; }
};

struct SubstraitTypeArgument : public SubstraitFunctionArgument {
  const std::string toTypeString() const override { return "type"; }
  const bool isRequired() const override { return true; }
};

struct SubstraitValueArgument : public SubstraitFunctionArgument {
  SubstraitTypePtr type;

  const std::string toTypeString() const override { return type->signature(); }

  const bool isRequired() const override { return true; }

  const bool isWildcardType() const override { return type->isWildcard(); }

  const bool isValueArgument() const override { return true; }
};

using SubstraitValueArgumentPtr = std::shared_ptr<SubstraitValueArgument>;

struct SubstraitFunctionAnchor {
  /// uri of function anchor corresponding the file
  std::string uri;

  /// function signature which is combination of function name and type of
  /// arguments.
  std::string key;

  bool operator==(const SubstraitFunctionAnchor& other) const {
    return (uri == other.uri && key == other.key);
  }
};

struct SubstraitFunctionVariadic {
  int min;
  std::optional<int> max;
};

struct SubstraitFunctionVariant {
  /// scalar function name.
  std::string name;
  /// scalar function uri.
  std::string uri;
  /// function arguments.
  std::vector<SubstraitFunctionArgumentPtr> arguments;
  /// return type of scalar function.
  SubstraitTypePtr returnType;
  /// function variadic
  std::optional<SubstraitFunctionVariadic> variadic;

  /// create function signature by given function name and arguments.
  static std::string signature(
      const std::string& name,
      const std::vector<SubstraitFunctionArgumentPtr>& arguments);

  /// create function signature by function name and arguments.
  const std::string signature() const { return signature(name, arguments); }

  const SubstraitFunctionAnchor anchor() const { return {uri, signature()}; }

  const bool isWildcard() const {
    for (auto& arg : arguments) {
      if (arg->isWildcardType()) {
        return true;
      }
    }
    return false;
  }

  const bool isVariadic() const { return variadic.has_value(); }

  virtual const bool isAggregateFunction() { return false; }

  virtual const bool isScalarFunction() { return true; }

  /// A collection of required arguments
  std::vector<SubstraitFunctionArgumentPtr> requiredArguments() const;
};

using SubstraitFunctionVariantPtr = std::shared_ptr<SubstraitFunctionVariant>;

struct SubstraitScalarFunctionVariant : public SubstraitFunctionVariant {};

struct SubstraitAggregateFunctionVariant : public SubstraitFunctionVariant {
  SubstraitTypePtr intermediate;
  const bool isAggregateFunction() override { return true; }
  const bool isScalarFunction() override { return false; }

  /// return intermediate function signature by function name and intermediate.
  const std::string intermediateSignature() const {
    if (intermediate) {
      return name + ":" + intermediate->signature();
    }
    return name;
  }
};

using SubstraitAggregateFunctionVariantPtr =
    std::shared_ptr<SubstraitAggregateFunctionVariant>;

struct SubstraitScalarFunction {
  /// scalar function name.
  std::string name;
  /// A collection of scalar function variants.
  std::vector<std::shared_ptr<SubstraitScalarFunctionVariant>> impls;
};

struct SubstraitAggregateFunction {
  /// aggregate function name.
  std::string name;
  /// A collection of aggregate function variants.
  std::vector<std::shared_ptr<SubstraitAggregateFunctionVariant>> impls;
};

}  // namespace cider::function::substrait

namespace std {

/// hash function of cider::function::substrait::SubstraitFunctionAnchor
template <>
struct hash<cider::function::substrait::SubstraitFunctionAnchor> {
  size_t operator()(const cider::function::substrait::SubstraitFunctionAnchor& k) const {
    return hash<std::string>()(k.key) ^ hash<std::string>()(k.uri);
  }
};

};  // namespace std

#endif  // CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTION_H
