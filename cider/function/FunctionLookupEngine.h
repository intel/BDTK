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

#ifndef CIDER_FUNCTION_FUNCTIONLOOKUP_ENGINE_H
#define CIDER_FUNCTION_FUNCTIONLOOKUP_ENGINE_H

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "function/SubstraitFunctionCiderMappings.h"
#include "function/substrait/SubstraitFunctionLookup.h"
#include "function/substrait/SubstraitType.h"
#include "function/substrait/VeloxToSubstraitMappings.h"

enum PlatformType { SubstraitPlatform, PrestoPlatform, SparkPlatform };

struct FunctionSignature {
  std::string func_name;
  std::vector<cider::function::substrait::SubstraitTypePtr> arguments;
  cider::function::substrait::SubstraitTypePtr return_type;
  PlatformType from_platform;
};

struct FunctionDescriptor {
  FunctionSignature func_sig;
  SQLOps scalar_op_type = SQLOps::kUNDEFINED_OP;
  SQLAgg agg_op_type = SQLAgg::kUNDEFINED_AGG;
  OpSupportExprType op_support_expr_type = OpSupportExprType::kUNDEFINED_EXPR;
};

using FunctionDescriptorPtr = std::shared_ptr<FunctionDescriptor>;
using SubstraitFunctionLookupPtr =
    std::shared_ptr<const cider::function::substrait::SubstraitFunctionLookup>;

class FunctionLookupEngine {
 public:
  FunctionLookupEngine(const PlatformType from_platform) : from_platform_(from_platform) {
    registerFunctionLookUpContext(from_platform);
  }

  /// lookup function descriptor by given function Signature.
  /// a) If sql_op is not kUNDEFINED_OP, means cider runtime function is selected for
  /// execution and corresponding Analyzer::Expr will be created for this scalar function.
  /// b) If agg_op is not kUNDEFINED_AGG, means cider runtime function is selected for
  /// execution and corresponding Analyzer::AggExpr will be created for this agg function.
  /// c) If op_support_type is not kUNDEFINED_EXPR, means this function is imported from
  /// frontend and we will use that directly, in cider internal, it will create
  /// Analyzer::FunctionOper directly. d) If sql_op/agg_op/op_support_type are all
  /// kUNDEFINED returned after lookup done, it indicates that we don't support this
  /// function and execution can not be offloaded.
  const FunctionDescriptor lookupFunction(
      const FunctionSignature& function_signature) const;

 private:
  void registerFunctionLookUpContext(const PlatformType from_platform);

  const SQLOps getFunctionScalarOp(const FunctionSignature& function_signature) const;
  const SQLAgg getFunctionAggOp(const FunctionSignature& function_signature) const;
  const OpSupportExprType getFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getScalarFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getAggFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getExtensionFunctionOpSupportType(
      const FunctionSignature& function_signature) const;

  static std::string getDataPath() {
    const std::string absolute_path = __FILE__;
    auto const pos = absolute_path.find_last_of('/');
    return absolute_path.substr(0, pos) + "/extensions";
  }

  SubstraitFunctionCiderMappingsPtr function_mappings_ =
      std::make_shared<const SubstraitFunctionCiderMappings>();

  // internal scalar function lookup ptr
  SubstraitFunctionLookupPtr scalar_function_look_up_ptr_;
  // internal aggregate function lookup ptr
  SubstraitFunctionLookupPtr aggregate_function_look_up_ptr_;
  // extension function lookup ptr
  SubstraitFunctionLookupPtr extension_function_look_up_ptr_;

  const PlatformType from_platform_;
};

using FunctionLookupEnginePtr = std::shared_ptr<const FunctionLookupEngine>;

#endif  // CIDER_FUNCTION_FUNCTIONLOOKUP_ENGINE_H
