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

#include <string>
#include <vector>
#include "type/data/sqltypes.h"

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

namespace {

// Returns the LLVM name for `type`.
std::string serialize_type(const ExtArgumentType type,
                           bool byval = true,
                           bool declare = false) {
  switch (type) {
    case ExtArgumentType::Bool:
      return "i8";  // clang converts bool to i8
    case ExtArgumentType::Int8:
      return "i8";
    case ExtArgumentType::Int16:
      return "i16";
    case ExtArgumentType::Int32:
      return "i32";
    case ExtArgumentType::Int64:
      return "i64";
    case ExtArgumentType::Float:
      return "float";
    case ExtArgumentType::Double:
      return "double";
    case ExtArgumentType::Void:
      return "void";
    case ExtArgumentType::PInt8:
      return "i8*";
    case ExtArgumentType::PInt16:
      return "i16*";
    case ExtArgumentType::PInt32:
      return "i32*";
    case ExtArgumentType::PInt64:
      return "i64*";
    case ExtArgumentType::PFloat:
      return "float*";
    case ExtArgumentType::PDouble:
      return "double*";
    case ExtArgumentType::PBool:
      return "i1*";
    case ExtArgumentType::ArrayInt8:
      return "{i8*, i64, i8}*";
    case ExtArgumentType::ArrayInt16:
      return "{i16*, i64, i8}*";
    case ExtArgumentType::ArrayInt32:
      return "{i32*, i64, i8}*";
    case ExtArgumentType::ArrayInt64:
      return "{i64*, i64, i8}*";
    case ExtArgumentType::ArrayFloat:
      return "{float*, i64, i8}*";
    case ExtArgumentType::ArrayDouble:
      return "{double*, i64, i8}*";
    case ExtArgumentType::ArrayBool:
      return "{i1*, i64, i8}*";
    case ExtArgumentType::Cursor:
      return "cursor";
    case ExtArgumentType::ColumnInt8:
      return (declare ? (byval ? "{i8*, i64}" : "i8*") : "column_int8");
    case ExtArgumentType::ColumnInt16:
      return (declare ? (byval ? "{i16*, i64}" : "i8*") : "column_int16");
    case ExtArgumentType::ColumnInt32:
      return (declare ? (byval ? "{i32*, i64}" : "i8*") : "column_int32");
    case ExtArgumentType::ColumnInt64:
      return (declare ? (byval ? "{i64*, i64}" : "i8*") : "column_int64");
    case ExtArgumentType::ColumnFloat:
      return (declare ? (byval ? "{float*, i64}" : "i8*") : "column_float");
    case ExtArgumentType::ColumnDouble:
      return (declare ? (byval ? "{double*, i64}" : "i8*") : "column_double");
    case ExtArgumentType::ColumnBool:
      return (declare ? (byval ? "{i8*, i64}" : "i8*") : "column_bool");
    case ExtArgumentType::ColumnTextEncodingDict:
      return (declare ? (byval ? "{i32*, i64}" : "i8*") : "column_text_encoding_dict");
    case ExtArgumentType::TextEncodingNone:
      return (declare ? (byval ? "{i8*, i64}*" : "i8*") : "text_encoding_none");
    case ExtArgumentType::TextEncodingDict:
      return (declare ? "{i8*, i32}*" : "text_encoding_dict");
    case ExtArgumentType::ColumnListInt8:
      return (declare ? "{i8**, i64, i64}*" : "column_list_int8");
    case ExtArgumentType::ColumnListInt16:
      return (declare ? "{i8**, i64, i64}*" : "column_list_int16");
    case ExtArgumentType::ColumnListInt32:
      return (declare ? "{i8**, i64, i64}*" : "column_list_int32");
    case ExtArgumentType::ColumnListInt64:
      return (declare ? "{i8**, i64, i64}*" : "column_list_int64");
    case ExtArgumentType::ColumnListFloat:
      return (declare ? "{i8**, i64, i64}*" : "column_list_float");
    case ExtArgumentType::ColumnListDouble:
      return (declare ? "{i8**, i64, i64}*" : "column_list_double");
    case ExtArgumentType::ColumnListBool:
      return (declare ? "{i8**, i64, i64}*" : "column_list_bool");
    case ExtArgumentType::ColumnListTextEncodingDict:
      return (declare ? "{i8**, i64, i64}*" : "column_list_text_encoding_dict");
    default:
      CHECK(false);
  }
  CHECK(false);
  return "";
}

ExtArgumentType deserialize_type(const std::string& type_name) {
  if (type_name == "bool" || type_name == "i1") {
    return ExtArgumentType::Bool;
  }
  if (type_name == "i8") {
    return ExtArgumentType::Int8;
  }
  if (type_name == "i16") {
    return ExtArgumentType::Int16;
  }
  if (type_name == "i32") {
    return ExtArgumentType::Int32;
  }
  if (type_name == "i64") {
    return ExtArgumentType::Int64;
  }
  if (type_name == "float") {
    return ExtArgumentType::Float;
  }
  if (type_name == "double") {
    return ExtArgumentType::Double;
  }
  if (type_name == "void") {
    return ExtArgumentType::Void;
  }
  if (type_name == "i8*") {
    return ExtArgumentType::PInt8;
  }
  if (type_name == "i16*") {
    return ExtArgumentType::PInt16;
  }
  if (type_name == "i32*") {
    return ExtArgumentType::PInt32;
  }
  if (type_name == "i64*") {
    return ExtArgumentType::PInt64;
  }
  if (type_name == "float*") {
    return ExtArgumentType::PFloat;
  }
  if (type_name == "double*") {
    return ExtArgumentType::PDouble;
  }
  if (type_name == "i1*" || type_name == "bool*") {
    return ExtArgumentType::PBool;
  }
  if (type_name == "{i8*, i64, i8}*") {
    return ExtArgumentType::ArrayInt8;
  }
  if (type_name == "{i16*, i64, i8}*") {
    return ExtArgumentType::ArrayInt16;
  }
  if (type_name == "{i32*, i64, i8}*") {
    return ExtArgumentType::ArrayInt32;
  }
  if (type_name == "{i64*, i64, i8}*") {
    return ExtArgumentType::ArrayInt64;
  }
  if (type_name == "{float*, i64, i8}*") {
    return ExtArgumentType::ArrayFloat;
  }
  if (type_name == "{double*, i64, i8}*") {
    return ExtArgumentType::ArrayDouble;
  }
  if (type_name == "{i1*, i64, i8}*" || type_name == "{bool*, i64, i8}*") {
    return ExtArgumentType::ArrayBool;
  }
  if (type_name == "cursor") {
    return ExtArgumentType::Cursor;
  }
  if (type_name == "column_int8") {
    return ExtArgumentType::ColumnInt8;
  }
  if (type_name == "column_int16") {
    return ExtArgumentType::ColumnInt16;
  }
  if (type_name == "column_int32") {
    return ExtArgumentType::ColumnInt32;
  }
  if (type_name == "column_int64") {
    return ExtArgumentType::ColumnInt64;
  }
  if (type_name == "column_float") {
    return ExtArgumentType::ColumnFloat;
  }
  if (type_name == "column_double") {
    return ExtArgumentType::ColumnDouble;
  }
  if (type_name == "column_bool") {
    return ExtArgumentType::ColumnBool;
  }
  if (type_name == "column_text_encoding_dict") {
    return ExtArgumentType::ColumnTextEncodingDict;
  }
  if (type_name == "text_encoding_none") {
    return ExtArgumentType::TextEncodingNone;
  }
  if (type_name == "text_encoding_dict") {
    return ExtArgumentType::TextEncodingDict;
  }
  if (type_name == "column_list_int8") {
    return ExtArgumentType::ColumnListInt8;
  }
  if (type_name == "column_list_int16") {
    return ExtArgumentType::ColumnListInt16;
  }
  if (type_name == "column_list_int32") {
    return ExtArgumentType::ColumnListInt32;
  }
  if (type_name == "column_list_int64") {
    return ExtArgumentType::ColumnListInt64;
  }
  if (type_name == "column_list_float") {
    return ExtArgumentType::ColumnListFloat;
  }
  if (type_name == "column_list_double") {
    return ExtArgumentType::ColumnListDouble;
  }
  if (type_name == "column_list_bool") {
    return ExtArgumentType::ColumnListBool;
  }
  if (type_name == "column_list_text_encoding_dict") {
    return ExtArgumentType::ColumnListTextEncodingDict;
  }
  CHECK(false);
  return ExtArgumentType::Int16;
}

std::string drop_suffix(const std::string& str) {
  const auto idx = str.find("__");
  if (idx == std::string::npos) {
    return str;
  }
  CHECK_GT(idx, std::string::size_type(0));
  return str.substr(0, idx);
}

}  // namespace

SQLTypeInfo ext_arg_type_to_type_info(const ExtArgumentType ext_arg_type);

class ExtensionFunction {
 public:
  ExtensionFunction() {}
  ExtensionFunction(const std::string& name,
                    const std::vector<ExtArgumentType>& args,
                    const ExtArgumentType ret)
      : name_(name), args_(args), ret_(ret) {}

  void setName(const std::string& name) { name_ = name; }
  void setRet(const ExtArgumentType ret) { ret_ = ret; }
  void setArgs(const std::vector<ExtArgumentType>& args) { args_ = args; }
  const std::string getName(bool keep_suffix = true) const;
  const std::vector<ExtArgumentType>& getArgs() const { return args_; }
  const std::vector<ExtArgumentType>& getInputArgs() const { return args_; }
  const ExtArgumentType getRet() const { return ret_; }
  // get signature such as: Between:DOUBLE_DOUBLE_DOUBLE
  const std::string signature() const;

  std::string toString() const;
  static std::string toString(const ExtArgumentType& sig_type);
  static std::string toString(const std::vector<ExtArgumentType>& sig_types);
  std::string toStringSQL() const;
  static std::string toStringSQL(const ExtArgumentType& sig_type);
  static std::string toStringSQL(const std::vector<ExtArgumentType>& sig_types);

  virtual bool isAggregateFunction() const { return false; }
  virtual bool isScalarFunction() const { return true; }

 private:
  std::string name_;
  std::vector<ExtArgumentType> args_;
  ExtArgumentType ret_;
};

class ScalarExtensionFunction : public ExtensionFunction {};

class AggregateExtensionFunction : public ExtensionFunction {
 public:
  bool isAggregateFunction() const override { return true; }
  bool isScalarFunction() const override { return false; }
};

using ExtensionFunctionPtr = std::shared_ptr<ExtensionFunction>;
using ScalarExtensionFunctionPtr = std::shared_ptr<ScalarExtensionFunction>;
using AggregateExtensionFunctionPtr = std::shared_ptr<AggregateExtensionFunction>;

class ExtensionFunctions {
 public:
  ExtensionFunctions() {}
  ExtensionFunctions(const std::string& name) : name_(name) {}
  ExtensionFunctions(const std::string& name,
                     const std::vector<ExtensionFunction>& extensions)
      : name_(name), extensions_(extensions) {}
  ExtensionFunctions(const std::string& name,
                     bool is_agg_functions,
                     bool is_scalar_functions,
                     const std::vector<ExtensionFunction>& extensions)
      : name_(name)
      , is_agg_functions_(is_agg_functions)
      , is_scalar_functions_(is_scalar_functions)
      , extensions_(extensions) {}
  virtual ~ExtensionFunctions() {}
  void setName(const std::string& name) { name_ = name; }
  void setIsAggFuncions(bool is_agg_functions) { is_agg_functions_ = is_agg_functions; }
  void setIsScalarFuncions(bool is_scalar_functions) {
    is_scalar_functions_ = is_scalar_functions;
  }
  void setExtensions(const std::vector<ExtensionFunction>& extensions) {
    extensions_ = extensions;
  }
  const std::string& getName() const { return name_; }
  const bool getIsAggFunctions() const { return is_agg_functions_; }
  const bool getIsScalarFunctions() const { return is_scalar_functions_; }
  const std::vector<ExtensionFunction>& getExtensions() const { return extensions_; }

 private:
  std::string name_;
  bool is_agg_functions_ = false;
  bool is_scalar_functions_ = false;
  std::vector<ExtensionFunction> extensions_;
};

class ScalarExtensionFunctions : public ExtensionFunctions {
 public:
  ScalarExtensionFunctions() {}
  ScalarExtensionFunctions(const std::string& name,
                           const std::vector<ScalarExtensionFunction>& extensions)
      : extensions_(extensions), ExtensionFunctions(name) {}
  void setExtensions(const std::vector<ScalarExtensionFunction>& extensions) {
    extensions_ = extensions;
  }
  const std::vector<ScalarExtensionFunction>& getExtensions() const {
    return extensions_;
  }

 private:
  bool is_agg_functions_ = false;
  bool is_scalar_functions_ = true;
  std::vector<ScalarExtensionFunction> extensions_;
};

class AggregateExtensionFunctions : public ExtensionFunctions {
 public:
  AggregateExtensionFunctions() {}
  AggregateExtensionFunctions(const std::string& name,
                              const std::vector<AggregateExtensionFunction>& extensions)
      : extensions_(extensions), ExtensionFunctions(name) {}
  void setExtensions(const std::vector<AggregateExtensionFunction>& extensions) {
    extensions_ = extensions;
  }
  const std::vector<AggregateExtensionFunction>& getExtensions() const {
    return extensions_;
  }

 private:
  bool is_agg_functions_ = true;
  bool is_scalar_functions_ = false;
  std::vector<AggregateExtensionFunction> extensions_;
};
