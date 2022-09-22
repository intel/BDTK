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

#include "ExtensionFunction.h"

SQLTypeInfo ext_arg_type_to_type_info(const ExtArgumentType ext_arg_type) {
  /* This function is mostly used for scalar types.
     For non-scalar types, NULL is returned as a placeholder.
   */

  switch (ext_arg_type) {
    case ExtArgumentType::Bool:
      return SQLTypeInfo(kBOOLEAN, false);
    case ExtArgumentType::Int8:
      return SQLTypeInfo(kTINYINT, false);
    case ExtArgumentType::Int16:
      return SQLTypeInfo(kSMALLINT, false);
    case ExtArgumentType::Int32:
      return SQLTypeInfo(kINT, false);
    case ExtArgumentType::Int64:
      return SQLTypeInfo(kBIGINT, false);
    case ExtArgumentType::Float:
      return SQLTypeInfo(kFLOAT, false);
    case ExtArgumentType::Double:
      return SQLTypeInfo(kDOUBLE, false);
    case ExtArgumentType::ArrayInt8:
      return generate_array_type(kTINYINT);
    case ExtArgumentType::ArrayInt16:
      return generate_array_type(kSMALLINT);
    case ExtArgumentType::ArrayInt32:
      return generate_array_type(kINT);
    case ExtArgumentType::ArrayInt64:
      return generate_array_type(kBIGINT);
    case ExtArgumentType::ArrayFloat:
      return generate_array_type(kFLOAT);
    case ExtArgumentType::ArrayDouble:
      return generate_array_type(kDOUBLE);
    case ExtArgumentType::ArrayBool:
      return generate_array_type(kBOOLEAN);
    case ExtArgumentType::ColumnInt8:
      return generate_column_type(kTINYINT);
    case ExtArgumentType::ColumnInt16:
      return generate_column_type(kSMALLINT);
    case ExtArgumentType::ColumnInt32:
      return generate_column_type(kINT);
    case ExtArgumentType::ColumnInt64:
      return generate_column_type(kBIGINT);
    case ExtArgumentType::ColumnFloat:
      return generate_column_type(kFLOAT);
    case ExtArgumentType::ColumnDouble:
      return generate_column_type(kDOUBLE);
    case ExtArgumentType::ColumnBool:
      return generate_column_type(kBOOLEAN);
    case ExtArgumentType::ColumnTextEncodingDict:
      return generate_column_type(kTEXT, kENCODING_DICT, 0 /* comp_param */);
    case ExtArgumentType::TextEncodingNone:
      return SQLTypeInfo(kTEXT, false, kENCODING_NONE);
    case ExtArgumentType::TextEncodingDict:
      return SQLTypeInfo(kTEXT, false, kENCODING_DICT);
    case ExtArgumentType::ColumnListInt8:
      return generate_column_type(kTINYINT);
    case ExtArgumentType::ColumnListInt16:
      return generate_column_type(kSMALLINT);
    case ExtArgumentType::ColumnListInt32:
      return generate_column_type(kINT);
    case ExtArgumentType::ColumnListInt64:
      return generate_column_type(kBIGINT);
    case ExtArgumentType::ColumnListFloat:
      return generate_column_type(kFLOAT);
    case ExtArgumentType::ColumnListDouble:
      return generate_column_type(kDOUBLE);
    case ExtArgumentType::ColumnListBool:
      return generate_column_type(kBOOLEAN);
    case ExtArgumentType::ColumnListTextEncodingDict:
      return generate_column_type(kTEXT, kENCODING_DICT, 0 /* comp_param */);
    default:
      LOG(FATAL) << "ExtArgumentType `" << serialize_type(ext_arg_type)
                 << "` cannot be converted to SQLTypeInfo.";
  }
  return SQLTypeInfo(kNULLT, false);
}

const std::string ExtensionFunction::getName(bool keep_suffix) const {
  return (keep_suffix ? name_ : drop_suffix(name_));
}

const std::string ExtensionFunction::signature() const {
  std::stringstream ss;
  ss << getName();
  if (!args_.empty()) {
    ss << ":";
    for (auto it = args_.begin(); it != args_.end(); ++it) {
      const auto& typeSign = toStringSQL(*it);
      if (it == args_.end() - 1) {
        ss << typeSign;
      } else {
        ss << typeSign << "_";
      }
    }
  }
  return ss.str();
}

std::string ExtensionFunction::toString() const {
  return getName() + "(" + toString(args_) + ") -> " + toString(ret_);
}

std::string ExtensionFunction::toString(const ExtArgumentType& sig_type) {
  return serialize_type(sig_type);
}

std::string ExtensionFunction::toString(const std::vector<ExtArgumentType>& sig_types) {
  std::string r = "";
  for (auto t = sig_types.begin(); t != sig_types.end();) {
    r += serialize_type(*t);
    t++;
    if (t != sig_types.end()) {
      r += ", ";
    }
  }
  return r;
}

std::string ExtensionFunction::toStringSQL() const {
  return getName(/* keep_suffix = */ false) + "(" + toStringSQL(args_) + ") -> " +
         toStringSQL(ret_);
}

std::string ExtensionFunction::toStringSQL(const ExtArgumentType& sig_type) {
  switch (sig_type) {
    case ExtArgumentType::Int8:
      return "TINYINT";
    case ExtArgumentType::Int16:
      return "SMALLINT";
    case ExtArgumentType::Int32:
      return "INTEGER";
    case ExtArgumentType::Int64:
      return "BIGINT";
    case ExtArgumentType::Float:
      return "FLOAT";
    case ExtArgumentType::Double:
      return "DOUBLE";
    case ExtArgumentType::Bool:
      return "BOOLEAN";
    case ExtArgumentType::PInt8:
      return "TINYINT[]";
    case ExtArgumentType::PInt16:
      return "SMALLINT[]";
    case ExtArgumentType::PInt32:
      return "INT[]";
    case ExtArgumentType::PInt64:
      return "BIGINT[]";
    case ExtArgumentType::PFloat:
      return "FLOAT[]";
    case ExtArgumentType::PDouble:
      return "DOUBLE[]";
    case ExtArgumentType::PBool:
      return "BOOLEAN[]";
    case ExtArgumentType::ArrayInt8:
      return "ARRAY<TINYINT>";
    case ExtArgumentType::ArrayInt16:
      return "ARRAY<SMALLINT>";
    case ExtArgumentType::ArrayInt32:
      return "ARRAY<INT>";
    case ExtArgumentType::ArrayInt64:
      return "ARRAY<BIGINT>";
    case ExtArgumentType::ArrayFloat:
      return "ARRAY<FLOAT>";
    case ExtArgumentType::ArrayDouble:
      return "ARRAY<DOUBLE>";
    case ExtArgumentType::ArrayBool:
      return "ARRAY<BOOLEAN>";
    case ExtArgumentType::ColumnInt8:
      return "COLUMN<TINYINT>";
    case ExtArgumentType::ColumnInt16:
      return "COLUMN<SMALLINT>";
    case ExtArgumentType::ColumnInt32:
      return "COLUMN<INT>";
    case ExtArgumentType::ColumnInt64:
      return "COLUMN<BIGINT>";
    case ExtArgumentType::ColumnFloat:
      return "COLUMN<FLOAT>";
    case ExtArgumentType::ColumnDouble:
      return "COLUMN<DOUBLE>";
    case ExtArgumentType::ColumnBool:
      return "COLUMN<BOOLEAN>";
    case ExtArgumentType::ColumnTextEncodingDict:
      return "COLUMN<TEXT ENCODING DICT>";
    case ExtArgumentType::Cursor:
      return "CURSOR";
    case ExtArgumentType::Void:
      return "VOID";
    case ExtArgumentType::TextEncodingNone:
      return "TEXT ENCODING NONE";
    case ExtArgumentType::TextEncodingDict:
      return "TEXT ENCODING DICT";
    case ExtArgumentType::ColumnListInt8:
      return "COLUMNLIST<TINYINT>";
    case ExtArgumentType::ColumnListInt16:
      return "COLUMNLIST<SMALLINT>";
    case ExtArgumentType::ColumnListInt32:
      return "COLUMNLIST<INT>";
    case ExtArgumentType::ColumnListInt64:
      return "COLUMNLIST<BIGINT>";
    case ExtArgumentType::ColumnListFloat:
      return "COLUMNLIST<FLOAT>";
    case ExtArgumentType::ColumnListDouble:
      return "COLUMNLIST<DOUBLE>";
    case ExtArgumentType::ColumnListBool:
      return "COLUMNLIST<BOOLEAN>";
    case ExtArgumentType::ColumnListTextEncodingDict:
      return "COLUMNLIST<TEXT ENCODING DICT>";
    default:
      UNREACHABLE();
  }
  return "";
}

std::string ExtensionFunction::toStringSQL(
    const std::vector<ExtArgumentType>& sig_types) {
  std::string r;
  for (auto t = sig_types.begin(); t != sig_types.end();) {
    r += toStringSQL(*t);
    t++;
    if (t != sig_types.end()) {
      r += ", ";
    }
  }
  return r;
}
