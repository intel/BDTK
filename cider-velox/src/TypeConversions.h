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

#pragma once

#include <cstdint>
#include "cider/CiderInterface.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::plugin {

inline const char* getArrowFormat(::substrait::Type& typeName) {
  switch (typeName.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
      return "b";
    case ::substrait::Type::KindCase::kI8:
      return "c";
    case ::substrait::Type::KindCase::kI16:
      return "s";
    case ::substrait::Type::KindCase::kI32:
      return "i";
    case ::substrait::Type::KindCase::kI64:
      return "l";
    case ::substrait::Type::KindCase::kFp32:
      return "f";
    case ::substrait::Type::KindCase::kFp64:
      return "g";
    case ::substrait::Type::KindCase::kDecimal: {
      auto precision = typeName.decimal().precision();
      auto scale = typeName.decimal().scale();
      std::string type_str = std::string("d:") + std::to_string(precision);
      std::string scale_str = std::string(",") + std::to_string(scale);
      type_str += scale_str;
      return type_str.c_str();
    }
    case ::substrait::Type::KindCase::kString:
      return "u";
    case ::substrait::Type::KindCase::kTimestamp:
      return "ttu";  // MICROSECOND
    default:
<<<<<<< HEAD
      VELOX_NYI("Conversion is not supported yet in getArrowFormat for type" +
                typeName.GetTypeName());
=======
      VELOX_UNSUPPORTED("Conversion {} is not supported yet", typeName.kind_case());
>>>>>>> 521b50f ([POAE7-2301]fix some suggestions)
  }
}

inline TypePtr getVeloxType(::substrait::Type& typeName) {
  switch (typeName.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
      return BOOLEAN();
    case ::substrait::Type::KindCase::kI8:
      return TINYINT();
    case ::substrait::Type::KindCase::kI16:
      return SMALLINT();
    case ::substrait::Type::KindCase::kI32:
      return INTEGER();
    case ::substrait::Type::KindCase::kI64:
      return BIGINT();
    case ::substrait::Type::KindCase::kFp32:
      return REAL();
    case ::substrait::Type::KindCase::kFp64:
    case ::substrait::Type::KindCase::kDecimal:
      return DOUBLE();
    case ::substrait::Type::KindCase::kString:
    case ::substrait::Type::KindCase::kVarchar:
      return VARCHAR();
    case ::substrait::Type::KindCase::kTimestamp:
      return TIMESTAMP();
    case ::substrait::Type::KindCase::kDate:
      return DATE();
    case ::substrait::Type::KindCase::kStruct: {
      auto typeSize = typeName.struct_().types_size();
      std::vector<TypePtr> rowTypes;
      std::vector<std::string> names;
      rowTypes.reserve(typeSize);
      names.reserve(typeSize);
      for (int idx = 0; idx < typeSize; idx++) {
        names.emplace_back("col_" + std::to_string(idx));
        rowTypes.emplace_back(std::move(
            getVeloxType(const_cast<::substrait::Type&>(typeName.struct_().types(idx)))));
      }
      return ROW(std::move(names), std::move(rowTypes));
    }
    default:
<<<<<<< HEAD
      VELOX_NYI("Conversion is not supported yet in getVeloxType for type " +
                typeName.GetTypeName());
=======
      VELOX_UNSUPPORTED("Conversion {} is not supported yet", typeName.kind_case());
>>>>>>> 521b50f ([POAE7-2301]fix some suggestions)
  }
}

}  // namespace facebook::velox::plugin
