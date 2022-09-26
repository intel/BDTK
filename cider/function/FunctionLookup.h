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
#ifndef CIDER_FUNCTION_FUNCTIONLOOKUP_H
#define CIDER_FUNCTION_FUNCTIONLOOKUP_H

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../thirdparty/velox/velox/substrait/SubstraitFunctionLookup.h"
#include "../thirdparty/velox/velox/substrait/SubstraitType.h"
#include "../thirdparty/velox/velox/substrait/VeloxToSubstraitMappings.h"
#include "SubstraitFunctionMappings.h"

#define SUBSTRAIT_ENGINE "substrait"
#define PRESTO_ENGINE "presto"

struct FunctionSignature {
  std::string func_name;
  std::vector<facebook::velox::substrait::SubstraitTypePtr> arguments;
  facebook::velox::substrait::SubstraitTypePtr return_type;
  std::string from_platform;
};

struct FunctionDescriptor {
  FunctionSignature func_sig;
  SQLOpsPtr scalar_op_type_ptr = nullptr;
  SQLAggPtr agg_op_type_ptr = nullptr;
  OpSupportExprTypePtr op_support_expr_type_ptr = nullptr;
};

using FunctionDescriptorPtr = std::shared_ptr<FunctionDescriptor>;
using SubstraitFunctionLookupPtr =
    std::shared_ptr<const facebook::velox::substrait::SubstraitFunctionLookup>;

class FunctionLookup {
 public:
  FunctionLookup(const SubstraitFunctionMappingsPtr& function_mappings)
      : function_mappings_(function_mappings) {
    registerFunctionLookUpContext(function_mappings);
  }

  /// lookup function descriptor by given function Signature.
  /// a) If sql_op is not null, means cider runtime function is selected for execution and
  /// corresponding Analyzer::Expr will be created for this scalar function. b) If agg_op
  /// is not null, means cider runtime function is selected for execution and
  /// corresponding Analyzer::AggExpr will be created for this agg function. c) If
  /// op_support_type is not null, means this function is imported from frontend and we
  /// will use that directly, in cider internal, it will create Analyzer::FunctionOper
  /// directly. d) If sql_op/agg_op/op_support_type are all null returned after lookup
  /// done, it indicates that we don't support this function and execution can not be
  /// offloaded.
  const FunctionDescriptorPtr lookupFunction(
      const FunctionSignature& function_signature) const;

 private:
  void registerFunctionLookUpContext(SubstraitFunctionMappingsPtr function_mappings);

  const SQLOpsPtr getFunctionScalarOp(const FunctionSignature& function_signature) const;
  const SQLAggPtr getFunctionAggOp(const FunctionSignature& function_signature) const;
  const OpSupportExprTypePtr getFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprTypePtr getScalarFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprTypePtr getAggFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprTypePtr getExtensionFunctionOpSupportType(
      const FunctionSignature& function_signature) const;

  static std::string getDataPath() {
    const std::string absolute_path = __FILE__;
    auto const pos = absolute_path.find_last_of('/');
    return absolute_path.substr(0, pos) + "/extensions";
  }

 private:
  SubstraitFunctionMappingsPtr function_mappings_ = nullptr;

  facebook::velox::substrait::SubstraitExtensionPtr cider_internal_function_ptr_ =
      facebook::velox::substrait::SubstraitExtension::loadExtension();
  facebook::velox::substrait::SubstraitExtensionPtr substrait_extension_function_ptr_ =
      facebook::velox::substrait::SubstraitExtension::loadExtension(
          {getDataPath() + "/substrait/" + "substrait_extension.yaml"});
  facebook::velox::substrait::SubstraitExtensionPtr presto_extension_function_ptr_ =
      facebook::velox::substrait::SubstraitExtension::loadExtension(
          {getDataPath() + "/presto/" + "presto_extension.yaml"});

  facebook::velox::substrait::SubstraitFunctionMappingsPtr substrait_mappings_ =
      std::make_shared<const facebook::velox::substrait::SubstraitFunctionMappings>();
  facebook::velox::substrait::SubstraitFunctionMappingsPtr presto_mappings_ =
      std::make_shared<
          const facebook::velox::substrait::VeloxToSubstraitFunctionMappings>();

  // internal scalar function map
  std::unordered_map<std::string, SubstraitFunctionLookupPtr>
      scalar_function_look_up_ptr_map_;
  // internal aggregate function map
  std::unordered_map<std::string, SubstraitFunctionLookupPtr>
      aggregate_function_look_up_ptr_map_;
  // extension function map
  std::unordered_map<std::string, SubstraitFunctionLookupPtr>
      extension_function_look_up_ptr_map_;
};

using FunctionLookupPtr = std::shared_ptr<const FunctionLookup>;

#endif  // CIDER_FUNCTION_FUNCTIONLOOKUP_H
