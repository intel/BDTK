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
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <filesystem>

#include "FunctionLookup.h"
#include "FunctionMapping.h"
#include "FunctionSignature.h"
#include "Type.h"
#include "function/SubstraitFunctionCiderMappings.h"
#include "include/cider/CiderSupportPlatType.h"

namespace io::substrait {

class PrestoFunctionMappings : public FunctionMapping {
 public:
  static const std::shared_ptr<PrestoFunctionMappings> make() {
    return std::make_shared<PrestoFunctionMappings>();
  }

  /// scalar function names in difference between presto and Substrait.
  const FunctionMap scalaMapping() const override {
    static const FunctionMap scalarMappings{
        {"plus", "add"},
        {"minus", "subtract"},
        {"mod", "modulus"},
        {"eq", "equal"},
        {"neq", "not_equal"},
        {"substr", "substring"},
    };
    return scalarMappings;
  };
};

}  // namespace io::substrait

using FunctionArgTypeMap = std::unordered_map<std::string, io::substrait::TypeKind>;

struct FunctionSignature {
  std::string func_name;
  std::vector<io::substrait::TypePtr> arguments;
  io::substrait::TypePtr return_type;
  PlatformType from_platform;
};

struct FunctionDescriptor {
  FunctionSignature func_sig;
  SQLOps scalar_op_type = SQLOps::kUNDEFINED_OP;
  SqlStringOpKind string_op_type = SqlStringOpKind::kUNDEFINED_STRING_OP;
  SQLAgg agg_op_type = SQLAgg::kUNDEFINED_AGG;
  OpSupportExprType op_support_expr_type = OpSupportExprType::kUNDEFINED_EXPR;
  bool is_cider_support_function = false;
};

using FunctionDescriptorPtr = std::shared_ptr<FunctionDescriptor>;

class FunctionLookupEngine;
using FunctionLookupEnginePtr = std::shared_ptr<const FunctionLookupEngine>;
using FunctionLookupEnginePtrMap =
    std::unordered_map<const PlatformType, FunctionLookupEnginePtr>;

class FunctionLookupEngine {
 public:
  static FunctionLookupEnginePtr getInstance(const PlatformType from_platform);

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

  // like:vchar<L1>_vchar<L1>, boolean
  const FunctionDescriptor lookupFunction(const std::string& function_signature_str,
                                          const std::string& function_return_type_str,
                                          const PlatformType& from_platform) const;

  static void setDataPath(const std::string& conf_path) {
    data_path_ = conf_path;
  }

 private:
  FunctionLookupEngine(const PlatformType from_platform) : from_platform_(from_platform) {
    registerFunctionLookUpContext(getDataPath(), from_platform);
  }
  void registerFunctionLookUpContext(const std::string& yaml_conf_path, const PlatformType from_platform);
  template <typename T>
  void loadExtensionYamlAndInitializeFunctionLookup(
      const std::string& yaml_conf_path,
      const std::string& platform_name,
      const std::string& yaml_extension_filename,
      const io::substrait::ExtensionPtr& cider_internal_function_ptr);

  const SQLOps getFunctionScalarOp(const FunctionSignature& function_signature) const;
  const SqlStringOpKind getFunctionStringOp(
      const FunctionSignature& function_signature) const;
  const SQLAgg getFunctionAggOp(const FunctionSignature& function_signature) const;
  const OpSupportExprType getFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getScalarFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getAggFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const OpSupportExprType getExtensionFunctionOpSupportType(
      const FunctionSignature& function_signature) const;
  const std::string getRealFunctionName(const std::string& function_name) const;
  const io::substrait::TypePtr getArgueTypePtr(const std::string& argue_type_str) const;

  static std::string getDataPath() {
    if (std::filesystem::is_directory(data_path_)) {
      return data_path_;
    }
    // for ut usage
    // ut does not provide a conf path
    // even ut's yaml file does not copy to a conf path
    // so ut will just use original path and original file
    // that means if we copy the ut to another path
    // ut will core or failed, due to can not find yaml file
    // TODO should provide conf for ut
    std::string file_path = __FILE__;
    size_t pos = file_path.find_last_of('/');
    return file_path.substr(0, pos);
  }

  static std::string data_path_;
  SubstraitFunctionCiderMappingsPtr function_mappings_ =
      std::make_shared<const SubstraitFunctionCiderMappings>();

  // internal scalar function lookup ptr
  io::substrait::FunctionLookupPtr scalar_function_look_up_ptr_;
  // internal aggregate function lookup ptr
  io::substrait::FunctionLookupPtr aggregate_function_look_up_ptr_;
  // extension function lookup ptr
  io::substrait::FunctionLookupPtr extension_function_look_up_ptr_;
  // function mapping
  io::substrait::FunctionMappingPtr function_mapping_ptr_;

  const PlatformType from_platform_;
  static FunctionLookupEnginePtrMap function_lookup_engine_ptr_map_;
  // use for prevent multiple times of new which is caused under multithreading
  // getInstance
  static std::mutex s_mutex_;
};

#endif  // CIDER_FUNCTION_FUNCTIONLOOKUP_ENGINE_H
