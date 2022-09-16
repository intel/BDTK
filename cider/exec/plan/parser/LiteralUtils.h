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

#ifndef CIDER_LITERALUTILS_H
#define CIDER_LITERALUTILS_H

#include "cider/CiderException.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"

// Public to make substrait literal easier
#define CREATE_LITERAL(name, value) \
  LiteralUtils::createLiteral(substrait::Type::KindCase::k##name, value)

class LiteralUtils {
 public:
  static ::substrait::Expression* createLiteral(::substrait::Type::KindCase typeKind,
                                                std::string value) {
    auto s_expr = new ::substrait::Expression();
    ::substrait::Expression_Literal* s_literal = s_expr->mutable_literal();
    switch (typeKind) {
      case ::substrait::Type::KindCase::kBool: {
        if (value == "false") {
          s_literal->set_boolean(false);
        } else if (value == "true") {
          s_literal->set_boolean(true);
        } else {
          CIDER_THROW(CiderCompileException, "Substrait bool literal is not correct.");
        }
        break;
      }
      case ::substrait::Type::KindCase::kI8:
        s_literal->set_i8(std::atoi(value.c_str()));
        break;
      case ::substrait::Type::KindCase::kI16:
        s_literal->set_i16(std::atoi(value.c_str()));
        break;
      case ::substrait::Type::KindCase::kI32:
        s_literal->set_i32(std::atoi(value.c_str()));
        break;
      case ::substrait::Type::KindCase::kI64:
        s_literal->set_i64(std::atoi(value.c_str()));
        break;
      case ::substrait::Type::KindCase::kString:
        s_literal->set_string(value);
        break;
      case ::substrait::Type::KindCase::kFp32:
      case ::substrait::Type::KindCase::kFp64:
      case ::substrait::Type::KindCase::kVarchar:
      default:
        CIDER_THROW(CiderCompileException,
                    "not supported literal type: " + std::to_string(typeKind));
    }
    return s_expr;
  }
};

#endif  // CIDER_LITERALUTILS_H
