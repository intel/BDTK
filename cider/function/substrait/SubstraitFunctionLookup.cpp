/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "function/substrait/SubstraitFunctionLookup.h"
#include <folly/String.h>
#include "function/substrait/SubstraitSignature.h"

namespace cider::function::substrait {

namespace {

/// create a new function variant with existing function variant and substrait
/// signature.
SubstraitFunctionVariantPtr cloneFunctionVariantWithSignature(
    SubstraitFunctionVariantPtr substraitFunctionVariant,
    SubstraitSignaturePtr substraitSignature) {
  auto functionVariant = *substraitFunctionVariant.get();
  auto& functionArguments = functionVariant.arguments;
  functionArguments.clear();
  functionArguments.reserve(substraitSignature->getArguments().size());
  for (const auto& argument : substraitSignature->getArguments()) {
    const auto& valueArgument = std::make_shared<SubstraitValueArgument>();
    valueArgument->type = argument;
    functionArguments.emplace_back(valueArgument);
  }
  return std::make_shared<SubstraitFunctionVariant>(functionVariant);
}

}  // namespace

SubstraitFunctionLookup::SubstraitFunctionLookup(
    const std::vector<SubstraitFunctionVariantPtr>& functions,
    const SubstraitFunctionMappingsPtr& functionMappings)
    : functionMappings_(functionMappings) {
  std::unordered_map<std::string, std::vector<SubstraitFunctionVariantPtr>> signatures;

  for (const auto& function : functions) {
    const auto& functionSignature = signatures.find(function->name);
    if (functionSignature == signatures.end()) {
      std::vector<SubstraitFunctionVariantPtr> nameFunctions;
      nameFunctions.emplace_back(function);
      signatures.insert({function->name, nameFunctions});
    } else {
      auto& nameFunctions = functionSignature->second;
      nameFunctions.emplace_back(function);
    }
  }

  for (const auto& [name, signature] : signatures) {
    auto functionFinder = std::make_shared<SubstraitFunctionFinder>(name, signature);
    functionFinders_.insert({name, functionFinder});
  }
}

const std::optional<SubstraitFunctionVariantPtr> SubstraitFunctionLookup::lookupFunction(
    const SubstraitSignaturePtr& functionSignature) const {
  const auto& functionMappings = getFunctionMappings();
  const auto& functionName = functionSignature->getName();
  const auto& substraitFunctionName =
      functionMappings.find(functionName) != functionMappings.end()
          ? functionMappings.at(functionName)
          : functionName;

  if (functionFinders_.find(substraitFunctionName) == functionFinders_.end()) {
    return std::nullopt;
  }
  const auto& newFunctionSignature =
      SubstraitFunctionSignature::of(substraitFunctionName,
                                     functionSignature->getArguments(),
                                     functionSignature->getReturnType());
  const auto& functionFinder = functionFinders_.at(substraitFunctionName);
  return functionFinder->lookupFunction(newFunctionSignature);
}

SubstraitFunctionLookup::SubstraitFunctionFinder::SubstraitFunctionFinder(
    const std::string& name,
    const std::vector<SubstraitFunctionVariantPtr>& functions)
    : name_(name) {
  for (const auto& function : functions) {
    if (function->isVariadic()) {
      functionVariantMatchers_.emplace_back(
          std::make_shared<VariadicFunctionVariantMatcher>(function));
    } else if (function->isWildcard()) {
      functionVariantMatchers_.emplace_back(
          std::make_shared<WildcardFunctionVariantMatcher>(function));
    } else {
      directMap_.insert({function->signature(), function});
      if (function->requiredArguments().size() != function->arguments.size()) {
        const std::string& functionKey = SubstraitFunctionVariant::signature(
            function->name, function->requiredArguments());
        directMap_.insert({functionKey, function});
      }
      if (function->isAggregateFunction()) {
        const auto& aggregateFunc =
            std::dynamic_pointer_cast<const SubstraitAggregateFunctionVariant>(function);
        directMap_.insert({aggregateFunc->intermediateSignature(), function});
      }
    }
  }
}

const std::optional<SubstraitFunctionVariantPtr>
SubstraitFunctionLookup::SubstraitFunctionFinder::lookupFunction(
    const SubstraitSignaturePtr& functionSignature) const {
  const auto& types = functionSignature->getArguments();
  const auto& signature = functionSignature->signature();
  /// try to do a direct match
  const auto& directFunctionVariant = directMap_.find(signature);
  if (directFunctionVariant != directMap_.end()) {
    const auto& functionVariant = directFunctionVariant->second;
    const auto& returnType = functionSignature->getReturnType();
    if (returnType && functionSignature->getReturnType() &&
        returnType->isSameAs(functionSignature->getReturnType())) {
      return std::make_optional(functionVariant);
    }
    return std::nullopt;
  }

  // return empty if no arguments
  if (functionSignature->getArguments().empty()) {
    return std::nullopt;
  }

  // try to match with wildcard or variadic function variants.
  for (const auto& functionVariantMatcher : functionVariantMatchers_) {
    const auto& matched = functionVariantMatcher->tryMatch(functionSignature);
    if (matched.has_value()) {
      return matched;
    }
  }
  return std::nullopt;
}

std::optional<SubstraitFunctionVariantPtr>
SubstraitFunctionLookup::VariadicFunctionVariantMatcher ::tryMatch(
    const SubstraitSignaturePtr& signature) const {
  const auto& arguments = signature->getArguments();
  const auto& maxArgumentNum = underlying_->variadic->max;
  if ((arguments.size() < underlying_->variadic->min) ||
      (maxArgumentNum.has_value() && arguments.size() > maxArgumentNum.value())) {
    return std::nullopt;
  }

  const auto& variadicArgument = underlying_->arguments[0];

  for (auto& type : signature->getArguments()) {
    if (variadicArgument->isValueArgument()) {
      const auto& variadicValueArgument =
          std::dynamic_pointer_cast<const SubstraitValueArgument>(variadicArgument);
      if (!variadicValueArgument->type->isSameAs(type)) {
        return std::nullopt;
      }
    }
  }
  return std::make_optional(underlying_);
}

SubstraitFunctionLookup::WildcardFunctionVariantMatcher::WildcardFunctionVariantMatcher(
    const SubstraitFunctionVariantPtr& functionVariant)
    : underlying_(functionVariant) {
  std::unordered_map<std::string, int> typeToRef;
  int typeRef = 0;
  int pos = 0;
  for (auto& arg : underlying_->arguments) {
    if (arg->isValueArgument()) {
      const auto& typeString = arg->toTypeString();
      if (typeToRef.find(typeString) == typeToRef.end()) {
        typeToRef.insert({typeString, typeRef++});
      }
      typeTraits.insert({pos++, typeToRef[typeString]});
    }
  }
}

std::optional<SubstraitFunctionVariantPtr>
SubstraitFunctionLookup::WildcardFunctionVariantMatcher ::tryMatch(
    const SubstraitSignaturePtr& signature) const {
  if (isSameTypeTraits(signature)) {
    return cloneFunctionVariantWithSignature(underlying_, signature);
  }
  return std::nullopt;
}

bool SubstraitFunctionLookup::WildcardFunctionVariantMatcher::isSameTypeTraits(
    const SubstraitSignaturePtr& signature) const {
  std::unordered_map<std::string, int> typeToRef;
  std::unordered_map<int, int> signatureTraits;
  int ref = 0;
  int pos = 0;
  for (auto& arg : signature->getArguments()) {
    const auto& typeString = arg->signature();
    if (typeToRef.find(typeString) == typeToRef.end()) {
      typeToRef.insert({typeString, ref++});
    }
    signatureTraits.insert({pos++, typeToRef[typeString]});
  }

  bool sameSize = typeTraits.size() == signatureTraits.size();
  if (sameSize) {
    for (const auto& [typePos, typeRef] : typeTraits) {
      if (signatureTraits.at(typePos) != typeRef) {
        return false;
      }
    }
    return true;
  } else {
    return false;
  }
}

}  // namespace cider::function::substrait
