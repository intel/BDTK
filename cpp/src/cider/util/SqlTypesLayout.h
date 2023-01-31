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

/**
 * @file    SqlTypesLayout.h
 */

#ifndef QUERYENGINE_SQLTYPESLAYOUT_H
#define QUERYENGINE_SQLTYPESLAYOUT_H

#include "util/TargetInfo.h"

#include "cider/CiderException.h"
#include "type/data/sqltypes.h"
#include "util/Logger.h"

#include <cstdint>
#include <limits>

inline const SQLTypeInfo get_compact_type(const TargetInfo& target) {
  if (!target.is_agg) {
    return target.sql_type;
  }
  const auto agg_type = target.agg_kind;
  const auto& agg_arg = target.agg_arg_type;
  if (agg_arg.get_type() == kNULLT) {
    CHECK_EQ(kCOUNT, agg_type);
    CHECK(!target.is_distinct);
    return target.sql_type;
  }

  if (is_agg_domain_range_equivalent(agg_type)) {
    return agg_arg;
  } else {
    // Nullability of the target needs to match that of the agg for proper initialization
    // of target (aggregate) values
    auto modified_target_type = target.sql_type;
    modified_target_type.set_notnull(agg_arg.get_notnull());
    return modified_target_type;
  }
}

inline void set_compact_type(TargetInfo& target, const SQLTypeInfo& new_type) {
  if (target.is_agg) {
    const auto agg_type = target.agg_kind;
    auto& agg_arg = target.agg_arg_type;
    if (agg_type != kCOUNT || agg_arg.get_type() != kNULLT) {
      agg_arg = new_type;
      return;
    }
  }
  target.sql_type = new_type;
}

inline int64_t inline_int_null_val(const SQLTypeInfo& ti) {
  auto type = ti.get_type();
  if (ti.is_string()) {
    CHECK_EQ(kENCODING_DICT, ti.get_compression());
    CHECK_EQ(4, ti.get_logical_size());
    type = kINT;
  }
  switch (type) {
    case kBOOLEAN:
      return inline_int_null_value<int8_t>();
    case kTINYINT:
      return inline_int_null_value<int8_t>();
    case kSMALLINT:
      return inline_int_null_value<int16_t>();
    case kINT:
      return inline_int_null_value<int32_t>();
    case kBIGINT:
      return inline_int_null_value<int64_t>();
    case kTIMESTAMP:
    case kTIME:
    case kDATE:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
      return inline_int_null_value<int64_t>();
    case kDECIMAL:
    case kNUMERIC:
      return inline_int_null_value<int64_t>();
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("type is {}", type));
  }
}

inline int64_t inline_fixed_encoding_null_val(const SQLTypeInfo& ti) {
  if (ti.get_compression() == kENCODING_NONE) {
    return inline_int_null_val(ti);
  }
  if (ti.get_compression() == kENCODING_DATE_IN_DAYS) {
    switch (ti.get_comp_param()) {
      case 0:
      case 32:
        return inline_int_null_value<int32_t>();
      case 16:
        return inline_int_null_value<int16_t>();
      default:
        CHECK(false) << "Unknown encoding width for date in days: "
                     << ti.get_comp_param();
    }
  }
  if (ti.get_compression() == kENCODING_DICT) {
    CHECK(ti.is_string());
    switch (ti.get_size()) {
      case 1:
        return inline_int_null_value<uint8_t>();
      case 2:
        return inline_int_null_value<uint16_t>();
      case 4:
        return inline_int_null_value<int32_t>();
      default:
        CHECK(false) << "Unknown size for dictionary encoded type: " << ti.get_size();
    }
  }
  CHECK_EQ(kENCODING_FIXED, ti.get_compression());
  CHECK(ti.is_integer() || ti.is_time() || ti.is_decimal());
  CHECK_EQ(0, ti.get_comp_param() % 8);
  return -(1LL << (ti.get_comp_param() - 1));
}

inline double inline_fp_null_val(const SQLTypeInfo& ti) {
  CHECK(ti.is_fp());
  const auto type = ti.get_type();
  switch (type) {
    case kFLOAT:
      return inline_fp_null_value<float>();
    case kDOUBLE:
      return inline_fp_null_value<double>();
    default:
      CIDER_THROW(CiderUnsupportedException, fmt::format("type is {}", type));
  }
}

inline uint64_t exp_to_scale(const unsigned exp) {
  uint64_t res = 1;
  for (unsigned i = 0; i < exp; ++i) {
    res *= 10;
  }
  return res;
}

inline size_t get_bit_width(const SQLTypeInfo& ti, bool is_arrow_format = false) {
  const auto int_type = ti.is_decimal() ? kBIGINT : ti.get_type();
  switch (int_type) {
    case kNULLT:
      LOG(ERROR) << "Untyped NULL values are not supported. Please CAST any NULL "
                    "constants to a type.";
    case kBOOLEAN:
      return 8;
    case kTINYINT:
      return 8;
    case kSMALLINT:
      return 16;
    case kINT:
      return 32;
    case kBIGINT:
      return 64;
    case kFLOAT:
      return 32;
    case kDOUBLE:
      return 64;
    case kDATE:
      if (is_arrow_format) {
        return 32;
      } else {
        return 64;
      }
    case kTIME:
    case kTIMESTAMP:
    case kINTERVAL_DAY_TIME:
    case kINTERVAL_YEAR_MONTH:
      return 64;
    case kTEXT:
    case kVARCHAR:
    case kCHAR:
      return 32;
    case kARRAY:
      if (ti.get_size() == -1) {
        CIDER_THROW(CiderCompileException,
                    "Projecting on unsized array column not supported.");
      }
      return ti.get_size() * 8;
    case kCOLUMN:
    case kCOLUMN_LIST:
      return ti.get_elem_type().get_size() * 8;
    default:
      LOG(ERROR) << "Unhandled int_type: " << int_type;
      return {};
  }
}

inline bool is_unsigned_type(const SQLTypeInfo& ti) {
  return ti.get_compression() == kENCODING_DICT && ti.get_size() < ti.get_logical_size();
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

#endif  // QUERYENGINE_SQLTYPESLAYOUT_H
