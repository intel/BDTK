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
#include "type/plan/ConstantExpr.h"

namespace Analyzer {

JITExprValue& Constant::codegen(JITFunction& func) {
  if (auto& expr_var = get_expr_value()) {
    return expr_var;
  }

  const auto& ti = get_type_info();
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();
  switch (type) {
    case kNULLT:
      CIDER_THROW(CiderCompileException,
                  "NULL type literals are not currently supported in this context.");
    case kBOOLEAN:
      return set_expr_value(nullptr,
                            func.createConstant(getJITTag(type), get_constval().boolval));
    case kTINYINT:
    case kSMALLINT:
    case kINT:
    case kBIGINT:
    case kTIME:
    case kTIMESTAMP:
    case kDATE:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
      return set_expr_value(nullptr,
                            func.createConstant(getJITTag(type), get_constval().intval));
    case kFLOAT:
      return set_expr_value(
          nullptr, func.createConstant(getJITTag(type), get_constval().floatval));
    case kDOUBLE:
      return set_expr_value(
          nullptr, func.createConstant(getJITTag(type), get_constval().doubleval));
    case kVARCHAR:
    case kCHAR:
    case kTEXT: {
      UNIMPLEMENTED();
      break;
    }
    default:
      UNIMPLEMENTED();
  }
  UNREACHABLE();
  return expr_var_;
}
}  // namespace Analyzer
