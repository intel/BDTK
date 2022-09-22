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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ExtensionFunction.h"
#include "SubstraitFunctionMappings.h"
#include "function/extensions/BasicFunctionLookUpContext.h"
#include "function/extensions/PrestoFunctionLookUpContext.h"
#include "function/extensions/SubstraitFunctionLookUpContext.h"

class FunctionLookup {
 public:
  FunctionLookup(const SubstraitFunctionMappingsPtr& function_mappings)
      : function_mappings_(function_mappings) {
    registerFunctionLookUpContext(function_mappings);
  }

  /// lookup function variant by given substrait function Signature.
  const FunctionDescriptorPtr lookupFunction(
      const FunctionSignature& function_signature) const;

 private:
  void registerFunctionLookUpContext(SubstraitFunctionMappingsPtr function_mappings) {
    extension_ptr_map_.insert(std::make_pair<std::string, BasicFunctionLookUpContextPtr>(
        "substrait", SubstraitFunctionLookUpContext::loadExtension(function_mappings)));
    extension_ptr_map_.insert(std::make_pair<std::string, BasicFunctionLookUpContextPtr>(
        "presto", PrestoFunctionLookUpContext::loadExtension(function_mappings)));
  }

 private:
  SubstraitFunctionMappingsPtr function_mappings_ = nullptr;
  std::unordered_map<std::string, BasicFunctionLookUpContextPtr> extension_ptr_map_;
};

using FunctionLookupPtr = std::shared_ptr<const FunctionLookup>;
