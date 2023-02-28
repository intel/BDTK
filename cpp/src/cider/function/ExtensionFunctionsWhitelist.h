/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#ifndef CIDER_FUNCTION_EXTENSIONFUNCTIONSWHITELIST_H
#define CIDER_FUNCTION_EXTENSIONFUNCTIONSWHITELIST_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "type/data/sqltypes.h"
#include "util/toString.h"

enum class ExtArgumentType {
  Int8,
  Int16,
  Int32,
  Int64,
  Float,
  Double,
  Void,
  PInt8,
  PInt16,
  PInt32,
  PInt64,
  PFloat,
  PDouble,
  PBool,
  Bool,
  ArrayInt8,
  ArrayInt16,
  ArrayInt32,
  ArrayInt64,
  ArrayFloat,
  ArrayDouble,
  ArrayBool,
  Cursor,
  ColumnInt8,
  ColumnInt16,
  ColumnInt32,
  ColumnInt64,
  ColumnFloat,
  ColumnDouble,
  ColumnBool,
  TextEncodingNone,
  TextEncodingDict,
  ColumnListInt8,
  ColumnListInt16,
  ColumnListInt32,
  ColumnListInt64,
  ColumnListFloat,
  ColumnListDouble,
  ColumnListBool,
  ColumnTextEncodingDict,
  ColumnListTextEncodingDict,
};

SQLTypeInfo ext_arg_type_to_type_info(const ExtArgumentType ext_arg_type);

class ExtensionFunction {
 public:
  ExtensionFunction(const std::string& name,
                    const std::vector<ExtArgumentType>& args,
                    const ExtArgumentType ret)
      : name_(name), args_(args), ret_(ret) {}

  const std::string getName(bool keep_suffix = true) const;

  const std::vector<ExtArgumentType>& getArgs() const { return args_; }
  const std::vector<ExtArgumentType>& getInputArgs() const { return args_; }

  const ExtArgumentType getRet() const { return ret_; }
  std::string toString() const;
  std::string toStringSQL() const;

 private:
  const std::string name_;
  const std::vector<ExtArgumentType> args_;
  const ExtArgumentType ret_;
};

class ExtensionFunctionsWhitelist {
 public:
  static void add(const std::string& json_func_sigs);

  static void addUdfs(const std::string& json_func_sigs);

  static void clearRTUdfs();
  static void addRTUdfs(const std::string& json_func_sigs);

  static std::vector<ExtensionFunction>* get(const std::string& name);

  static std::vector<ExtensionFunction>* get_udf(const std::string& name);

  static std::vector<ExtensionFunction> get_ext_funcs(const std::string& name);

  static std::vector<ExtensionFunction> get_ext_funcs(const std::string& name,
                                                      size_t arity);

  static std::vector<ExtensionFunction> get_ext_funcs(const std::string& name,
                                                      size_t arity,
                                                      const SQLTypeInfo& rtype);

  static std::string toString(const std::vector<ExtensionFunction>& ext_funcs,
                              std::string tab = "");
  static std::string toString(const std::vector<SQLTypeInfo>& arg_types);
  static std::string toString(const std::vector<ExtArgumentType>& sig_types);
  static std::string toStringSQL(const std::vector<ExtArgumentType>& sig_types);
  static std::string toString(const ExtArgumentType& sig_type);
  static std::string toStringSQL(const ExtArgumentType& sig_type);

  static std::vector<std::string> getLLVMDeclarations(
      const std::unordered_set<std::string>& udf_decls);

 private:
  static void addCommon(
      std::unordered_map<std::string, std::vector<ExtensionFunction>>& sigs,
      const std::string& json_func_sigs);

 private:
  // Compiletime UDFs defined in ExtensionFunctions.hpp
  static std::unordered_map<std::string, std::vector<ExtensionFunction>> functions_;
  // Loadtime UDFs defined via cider server --udf argument
  static std::unordered_map<std::string, std::vector<ExtensionFunction>> udf_functions_;
  // Runtime UDFs defined via thrift interface.
  static std::unordered_map<std::string, std::vector<ExtensionFunction>>
      rt_udf_functions_;
};

std::string toString(const ExtArgumentType& sig_type);

#endif  // CIDER_FUNCTION_EXTENSIONFUNCTIONSWHITELIST_H
