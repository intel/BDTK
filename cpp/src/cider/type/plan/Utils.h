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
#ifndef PLAN_UTILS_H
#define PLAN_UTILS_H

#include "type/plan/Expr.h"

#include <type/data/sqltypes.h>
#include "type/plan/UnaryExpr.h"
inline const Analyzer::Expr* extract_cast_arg(const Analyzer::Expr* expr) {
  const auto cast_expr = dynamic_cast<const Analyzer::UOper*>(expr);
  if (!cast_expr || cast_expr->get_optype() != kCAST) {
    return expr;
  }
  return cast_expr->get_operand();
}

inline bool is_unnest(const Analyzer::Expr* expr) {
  return dynamic_cast<const Analyzer::UOper*>(expr) &&
         static_cast<const Analyzer::UOper*>(expr)->get_optype() == kUNNEST;
}
inline int64_t get_max_value(SQLTypes type) {
  int64_t max_val;
  switch (type) {
    case kBOOLEAN:
      max_val = std::numeric_limits<bool>::max();
      break;
    case kTINYINT:
      max_val = std::numeric_limits<int8_t>::max();
      break;
    case kSMALLINT:
      max_val = std::numeric_limits<int16_t>::max();
      break;
    case kINT:
      max_val = std::numeric_limits<int32_t>::max();
      break;
    case kTEXT:
    case kVARCHAR:
    case kDECIMAL:
    case kBIGINT:
      max_val = std::numeric_limits<int64_t>::max();
      break;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("type is {}", type));
  }

  return max_val;
}

inline int64_t get_min_value(SQLTypes type) {
  int64_t min_val;
  switch (type) {
    case kBOOLEAN:
      min_val = std::numeric_limits<bool>::min();
      break;
    case kTINYINT:
      min_val = std::numeric_limits<int8_t>::min();
      break;
    case kSMALLINT:
      min_val = std::numeric_limits<int16_t>::min();
      break;
    case kINT:
      min_val = std::numeric_limits<int32_t>::min();
      break;
    case kTEXT:
    case kVARCHAR:
    case kDECIMAL:
    case kBIGINT:
      min_val = std::numeric_limits<int64_t>::min();
      break;
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("type is {}", type));
  }

  return min_val;
}

inline __int128_t exp_to_scale(const unsigned exp) {
  __int128 res = 1;
  for (unsigned i = 0; i < exp; ++i) {
    res *= 10;
  }
  return res;
}
#endif
