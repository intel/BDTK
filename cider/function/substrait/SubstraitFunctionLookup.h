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

#ifndef CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTIONLOOKUP_H
#define CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTIONLOOKUP_H

#include "function/substrait/SubstraitExtension.h"
#include "function/substrait/SubstraitFunctionMappings.h"
#include "function/substrait/SubstraitSignature.h"

namespace cider::function::substrait {

class SubstraitFunctionLookup {
 protected:
  SubstraitFunctionLookup(
      const std::vector<SubstraitFunctionVariantPtr>& functionVariants,
      const SubstraitFunctionMappingsPtr& functionMappings);

 public:
  /// lookup function variant by given substrait function Signature.
  const std::optional<SubstraitFunctionVariantPtr> lookupFunction(
      const SubstraitSignaturePtr& functionSignature) const;

 protected:
  /// get the map which store the function names in difference between velox
  /// and substrait.
  virtual const FunctionMappings getFunctionMappings() const = 0;

  const SubstraitFunctionMappingsPtr functionMappings_;

 private:
  /// An interface for lookup function variant with substrait signature.
  class FunctionVariantMatcher {
   public:
    /// lookup function variant by given substrait function signature.
    ///@return substrait function variant if matched, or null option if not
    /// matched.
    virtual std::optional<SubstraitFunctionVariantPtr> tryMatch(
        const SubstraitSignaturePtr& signature) const = 0;
  };

  using FunctionVariantMatcherPtr = std::shared_ptr<const FunctionVariantMatcher>;

  /// An implementation of FunctionVariantMatcher which match signature with
  /// wildcard type.
  class WildcardFunctionVariantMatcher : public FunctionVariantMatcher {
   public:
    WildcardFunctionVariantMatcher(const SubstraitFunctionVariantPtr& functionVaraint);

    ///  return function varaint if current wildcard function variant match the
    ///  given signature and.
    std::optional<SubstraitFunctionVariantPtr> tryMatch(
        const SubstraitSignaturePtr& signature) const override;

   private:
    /// test current wildcard function variant match the given signature.
    bool isSameTypeTraits(const SubstraitSignaturePtr& signature) const;

    /// A map store type position and its type reference.
    std::unordered_map<int, int> typeTraits;

    /// the underlying function variant;
    const SubstraitFunctionVariantPtr underlying_;
  };

  /// An implementation of FunctionVariantMatcher which match signature with
  /// variadic arguments.
  class VariadicFunctionVariantMatcher : public FunctionVariantMatcher {
   public:
    VariadicFunctionVariantMatcher(const SubstraitFunctionVariantPtr& functionVaraint)
        : underlying_(functionVaraint) {}

    std::optional<SubstraitFunctionVariantPtr> tryMatch(
        const SubstraitSignaturePtr& signature) const override;

   private:
    /// the underlying function variant;
    const SubstraitFunctionVariantPtr underlying_;
  };

  class SubstraitFunctionFinder {
   public:
    /// construct FunctionFinder with function name and it's function variants
    SubstraitFunctionFinder(
        const std::string& name,
        const std::vector<SubstraitFunctionVariantPtr>& functionVariants);

    /// lookup function variant by given substrait function signature.
    const std::optional<SubstraitFunctionVariantPtr> lookupFunction(
        const SubstraitSignaturePtr& signature) const;

   private:
    /// function name
    const std::string name_;
    /// A map store the function signature and corresponding function variant
    std::unordered_map<std::string, SubstraitFunctionVariantPtr> directMap_;
    /// A collection of function variant matcher
    std::vector<FunctionVariantMatcherPtr> functionVariantMatchers_;
  };

  using SubstraitFunctionFinderPtr = std::shared_ptr<const SubstraitFunctionFinder>;

  std::unordered_map<std::string, SubstraitFunctionFinderPtr> functionFinders_;
};

class SubstraitScalarFunctionLookup : public SubstraitFunctionLookup {
 public:
  SubstraitScalarFunctionLookup(const SubstraitExtensionPtr& extension,
                                const SubstraitFunctionMappingsPtr& functionMappings)
      : SubstraitFunctionLookup(extension->scalarFunctionVariants, functionMappings) {}

 protected:
  /// A  map store the difference of scalar function names between velox
  /// and substrait.
  const FunctionMappings getFunctionMappings() const override {
    return functionMappings_->scalarMappings();
  }
};

using SubstraitScalarFunctionLookupPtr =
    std::shared_ptr<const SubstraitScalarFunctionLookup>;

class SubstraitAggregateFunctionLookup : public SubstraitFunctionLookup {
 public:
  SubstraitAggregateFunctionLookup(const SubstraitExtensionPtr& extension,
                                   const SubstraitFunctionMappingsPtr& functionMappings)
      : SubstraitFunctionLookup(extension->aggregateFunctionVariants, functionMappings) {}

 protected:
  /// A  map store the difference of aggregate function names between velox
  /// and substrait.
  const FunctionMappings getFunctionMappings() const override {
    return functionMappings_->aggregateMappings();
  }
};

using SubstraitAggregateFunctionLookupPtr =
    std::shared_ptr<const SubstraitAggregateFunctionLookup>;

}  // namespace cider::function::substrait

#endif  // CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITFUNCTIONLOOKUP_H
