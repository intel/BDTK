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

#ifndef CIDER_TYPEUTILS_H
#define CIDER_TYPEUTILS_H

#include "cider/CiderException.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"

// Public to make substrait type easier
#define CREATE_SUBSTRAIT_TYPE_FULL(name, nullable) \
  TypeUtils::createType(::substrait::Type::KindCase::k##name, nullable)

#define CREATE_SUBSTRAIT_TYPE_FULL_PTR(name, nullable) \
  TypeUtils::createTypePtr(::substrait::Type::KindCase::k##name, nullable)

#define CREATE_SUBSTRAIT_TYPE(name) \
  TypeUtils::createType(::substrait::Type::KindCase::k##name)

#define CREATE_SUBSTRAIT_LIST_TYPE(name) \
  TypeUtils::createListType(substrait::Type::KindCase::k##name)

// Internal use this macro
#define GENERATE_SUBSTRAIT_TYPE(type_name, type_func, nullalbility) \
  {                                                                 \
    auto s = new ::substrait::Type_##type_name();                   \
    s->set_nullability(nullalbility);                               \
    s_type.set_allocated_##type_func(s);                            \
    break;                                                          \
  }

#define GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)            \
  {                                                           \
    auto l = new ::substrait::Type_List();                    \
    l->set_nullability(nullalbility);                         \
    l->set_allocated_type(createTypePtr(typeKind, nullable)); \
    s_type.set_allocated_list(l);                             \
    break;                                                    \
  }

class TypeUtils {
 public:
  static ::substrait::Type* createTypePtr(::substrait::Type::KindCase typeKind,
                                          bool nullable) {
    auto s_type = new ::substrait::Type();
    s_type->CopyFrom(createType(typeKind, nullable));
    return s_type;
  }

  static ::substrait::Type createType(::substrait::Type::KindCase typeKind,
                                      bool nullable = false) {
    ::substrait::Type s_type;
    substrait::Type_Nullability nullalbility =
        nullable ? substrait::Type::NULLABILITY_NULLABLE
                 : substrait::Type::NULLABILITY_REQUIRED;
    switch (typeKind) {
      case ::substrait::Type::KindCase::kBool:
        GENERATE_SUBSTRAIT_TYPE(Boolean, bool_, nullalbility)
      case ::substrait::Type::KindCase::kI8:
        GENERATE_SUBSTRAIT_TYPE(I8, i8, nullalbility)
      case ::substrait::Type::KindCase::kI16:
        GENERATE_SUBSTRAIT_TYPE(I16, i16, nullalbility)
      case ::substrait::Type::KindCase::kI32:
        GENERATE_SUBSTRAIT_TYPE(I32, i32, nullalbility)
      case ::substrait::Type::KindCase::kI64:
        GENERATE_SUBSTRAIT_TYPE(I64, i64, nullalbility)
      case ::substrait::Type::KindCase::kFp32:
        GENERATE_SUBSTRAIT_TYPE(FP32, fp32, nullalbility)
      case ::substrait::Type::KindCase::kFp64:
        GENERATE_SUBSTRAIT_TYPE(FP64, fp64, nullalbility)
      case ::substrait::Type::KindCase::kDate:
        GENERATE_SUBSTRAIT_TYPE(Date, date, nullalbility)
      case ::substrait::Type::KindCase::kTime:
        GENERATE_SUBSTRAIT_TYPE(Time, time, nullalbility)
      case ::substrait::Type::KindCase::kTimestamp:
        GENERATE_SUBSTRAIT_TYPE(Timestamp, timestamp, nullalbility)
      case ::substrait::Type::KindCase::kString:
        GENERATE_SUBSTRAIT_TYPE(String, string, nullalbility)
      case ::substrait::Type::KindCase::kVarchar:
        GENERATE_SUBSTRAIT_TYPE(VarChar, varchar, nullalbility)
      case ::substrait::Type::KindCase::kFixedChar:
        GENERATE_SUBSTRAIT_TYPE(FixedChar, fixed_char, nullalbility)
      case ::substrait::Type::KindCase::kDecimal:
        GENERATE_SUBSTRAIT_TYPE(Decimal, decimal, nullalbility)
      default:
        CIDER_THROW(CiderCompileException,
                    fmt::format("not supported type: {}", typeKind));
    }

    return s_type;
  }

  static ::substrait::Type createListType(::substrait::Type::KindCase typeKind,
                                          bool nullable = false) {
    ::substrait::Type s_type;
    substrait::Type_Nullability nullalbility =
        nullable ? substrait::Type::NULLABILITY_NULLABLE
                 : substrait::Type::NULLABILITY_REQUIRED;
    switch (typeKind) {
      case ::substrait::Type::KindCase::kBool:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kI8:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kI16:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kI32:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kI64:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kFp32:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kFp64:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kDate:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kTime:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kTimestamp:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kString:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kVarchar:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kFixedChar:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      case ::substrait::Type::KindCase::kDecimal:
        GENERATE_SUBSTRAIT_LIST_TYPE(nullalbility)
      default:
        CIDER_THROW(CiderCompileException,
                    fmt::format("not supported type: {}", typeKind));
    }

    return s_type;
  }

  static std::string getStringType(const ::substrait::Type& type) {
    switch (type.kind_case()) {
      case substrait::Type::kBool:
        return "boolean";
      case substrait::Type::kI8:
        return "i8";
      case substrait::Type::kI16:
        return "i16";
      case substrait::Type::kI32:
        return "i32";
      case substrait::Type::kI64:
        return "i64";
      case substrait::Type::kFp32:
        return "fp32";
      case substrait::Type::kFp64:
        return "fp64";
      case substrait::Type::kString:
        return "str";
      case substrait::Type::kBinary:
        return "vbin";
      case substrait::Type::kTimestamp:
        return "ts";
      case substrait::Type::kDate:
        return "date";
      case substrait::Type::kTime:
        return "time";
      case substrait::Type::kIntervalYear:
        return "iyear";
      case substrait::Type::kIntervalDay:
        return "iday";
      case substrait::Type::kTimestampTz:
        return "tstz";
      case substrait::Type::kUuid:
        return "uuid";
      case substrait::Type::kFixedChar:
        return "fchar";
      case substrait::Type::kVarchar:
        return "vchar";
      case substrait::Type::kFixedBinary:
        return "fbin";
      case substrait::Type::kDecimal:
        return "dec";
      case substrait::Type::kStruct:
        return "struct";
      case substrait::Type::kList:
        return "list";
      case substrait::Type::kMap:
        return "map";
      default:
        CIDER_THROW(CiderCompileException,
                    "Failed to get arg type when trying to make func");
    }
  }

  static std::string getStringType(const substrait::Expression_Literal& s_literal_expr) {
    switch (s_literal_expr.literal_type_case()) {
      case substrait::Expression_Literal::LiteralTypeCase::kBoolean:
        return "boolean";
      case substrait::Expression_Literal::LiteralTypeCase::kDecimal:
        return "dec";
      case substrait::Expression_Literal::LiteralTypeCase::kFp32:
        return "fp32";
      case substrait::Expression_Literal::LiteralTypeCase::kFp64:
        return "fp64";
      case substrait::Expression_Literal::LiteralTypeCase::kI8:
        return "i8";
      case substrait::Expression_Literal::LiteralTypeCase::kI16:
        return "i16";
      case substrait::Expression_Literal::LiteralTypeCase::kI32:
        return "i32";
      case substrait::Expression_Literal::LiteralTypeCase::kI64:
        return "i64";
      case substrait::Expression_Literal::LiteralTypeCase::kDate:
        return "date";
      case substrait::Expression_Literal::LiteralTypeCase::kTime:
        return "time";
      case substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
        return "ts";
      case substrait::Expression_Literal::LiteralTypeCase::kFixedChar:
        return "fchar";
      case substrait::Expression_Literal::LiteralTypeCase::kVarChar:
        return "vchar";
      case substrait::Expression_Literal::LiteralTypeCase::kString:
        return "str";
      case substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth:
        return "iyear";
      case substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond:
        return "iday";
      default:
        CIDER_THROW(
            CiderCompileException,
            fmt::format("Unsupported type {}", s_literal_expr.literal_type_case()));
    }
  }

  static substrait::Type_Nullability getSubstraitTypeNullability(bool isNullable) {
    if (isNullable) {
      return substrait::Type::NULLABILITY_NULLABLE;
    }
    return substrait::Type::NULLABILITY_REQUIRED;
  }

  static bool getIsNullable(const substrait::Type_Nullability& type_nullability) {
    switch (type_nullability) {
      case substrait::Type::NULLABILITY_REQUIRED:
        return false;
      case substrait::Type::NULLABILITY_NULLABLE:
        return true;
      case substrait::Type::NULLABILITY_UNSPECIFIED:
        CIDER_THROW(CiderCompileException, "nullability of column is not specified.");
    }
  }

  static bool getIsNullable(const substrait::Type& type) {
    switch (type.kind_case()) {
      case substrait::Type::kBool:
        return getIsNullable(type.bool_().nullability());
      case substrait::Type::kI64:
        return getIsNullable(type.i64().nullability());
      case substrait::Type::kI32:
        return getIsNullable(type.i32().nullability());
      case substrait::Type::kI16:
        return getIsNullable(type.i16().nullability());
      case substrait::Type::kI8:
        return getIsNullable(type.i8().nullability());
      case substrait::Type::kDecimal:
        return getIsNullable(type.decimal().nullability());
      case substrait::Type::kFp64:
        return getIsNullable(type.fp64().nullability());
      case substrait::Type::kFp32:
        return getIsNullable(type.fp32().nullability());
      case substrait::Type::kStruct:
        return getIsNullable(type.struct_().nullability());
      case substrait::Type::kDate:
        return getIsNullable(type.date().nullability());
      case substrait::Type::kTime:
        return getIsNullable(type.time().nullability());
      case substrait::Type::kTimestamp:
        return getIsNullable(type.timestamp().nullability());
      case substrait::Type::kVarchar:
        return getIsNullable(type.varchar().nullability());
      case substrait::Type::kFixedChar:
        return getIsNullable(type.fixed_char().nullability());
      default:
        return true;
    }
  }
};

#endif  // CIDER_TYPEUTILS_H
