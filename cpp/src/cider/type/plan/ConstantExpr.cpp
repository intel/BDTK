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
#include "cider/CiderOptions.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/jitlib/base/JITValue.h"

namespace Analyzer {

JITExprValue& Constant::codegen(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  if (auto& expr_var = get_expr_value()) {
    return expr_var;
  }

  auto null = func.createLiteral(JITTypeTag::BOOL, get_is_null());
  if (FLAGS_null_separate) {
    null.replace(func.createLiteral(JITTypeTag::BOOL, false));
  }

  const auto& ti = get_type_info();
  const auto type = ti.is_decimal() ? decimal_to_int_type(ti) : ti.get_type();
  switch (type) {
    case kNULLT:
      CIDER_THROW(CiderCompileException,
                  "NULL type literals are not currently supported in this context.");
    case kBOOLEAN:
      return set_expr_value(null,
                            func.createLiteral(getJITTag(type), get_constval().boolval));
    case kTINYINT:
      return set_expr_value(
          null, func.createLiteral(getJITTag(type), get_constval().tinyintval));
    case kSMALLINT:
      return set_expr_value(
          null, func.createLiteral(getJITTag(type), get_constval().smallintval));
    case kINT:
      return set_expr_value(null,
                            func.createLiteral(getJITTag(type), get_constval().intval));
    case kDATE:
      return set_expr_value(null,
                            func.createLiteral(getJITTag(type), get_constval().intval));
    case kBIGINT:
    case kTIME:
    case kTIMESTAMP:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
      return set_expr_value(
          null, func.createLiteral(getJITTag(type), get_constval().bigintval));
    case kFLOAT:
      return set_expr_value(null,
                            func.createLiteral(getJITTag(type), get_constval().floatval));
    case kDOUBLE:
      return set_expr_value(
          null, func.createLiteral(getJITTag(type), get_constval().doubleval));
    case kVARCHAR:
    case kCHAR:
    case kTEXT: {
      return set_expr_value(
          null,
          func.createLiteral(JITTypeTag::INT32, get_constval().stringval->length()),
          func.createStringLiteral(*get_constval().stringval));
    }
    default:
      UNIMPLEMENTED();
  }
  UNREACHABLE();
  return expr_var_;
}

JITExprValue& Constant::codegenNull(CodegenContext& context) {
  JITFunction& func = *context.getJITFunction();
  // if (auto& expr_var = get_expr_value()) {
  //   return expr_var;
  // }

  auto null = func.createLiteral(JITTypeTag::BOOL, get_is_null());
  return set_expr_null(null);
}  // namespace Analyzer
}  // namespace Analyzer
