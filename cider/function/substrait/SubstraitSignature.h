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

#ifndef CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITSIGNATURE_H
#define CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITSIGNATURE_H

#include "function/substrait/SubstraitFunctionMappings.h"
#include "function/substrait/SubstraitType.h"

namespace cider::function::substrait {

class SubstraitFunctionSignature {
 public:
  /// construct the substrait function signature with function name, return type
  /// and arguments.
  SubstraitFunctionSignature(const std::string& name,
                             const std::vector<SubstraitTypePtr>& arguments,
                             const SubstraitTypePtr& returnType)
      : name_(name), arguments_(arguments), returnType_(returnType) {}

  /// A shortcut method to create shared_ptr of SubstraitFunctionSignature.
  static std::shared_ptr<SubstraitFunctionSignature> of(
      const std::string& name,
      const std::vector<SubstraitTypePtr>& arguments = {},
      const SubstraitTypePtr& returnType = nullptr) {
    return std::make_shared<SubstraitFunctionSignature>(name, arguments, returnType);
  }

  /// Return function signature according to the given function name and
  /// substrait types.
  const std::string signature() const;

  const std::string getName() const { return name_; }

  const std::vector<SubstraitTypePtr> getArguments() const { return arguments_; }

  const SubstraitTypePtr getReturnType() const { return returnType_; }

  /// return an new function signature with function mappings
  static const std::string signature(
      const std::string& functionSignature,
      const SubstraitFunctionMappingsPtr& functionMappings);

 private:
  const std::string name_;
  const std::vector<SubstraitTypePtr> arguments_;
  const SubstraitTypePtr returnType_;
};

using SubstraitSignaturePtr = std::shared_ptr<const SubstraitFunctionSignature>;

}  // namespace cider::function::substrait

#endif  // CIDER_FUNCTION_SUBSTRAIT_SUBSTRAITSIGNATURE_H
