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

/**
 * @file    CiderSort.cpp
 * @brief   Cider sort
 **/

#include "CiderSort.h"

namespace generator {

std::vector<Analyzer::OrderEntry> translate_collation(
    const std::vector<SortField>& sort_fields) {
  std::vector<Analyzer::OrderEntry> collation;
  for (size_t i = 0; i < sort_fields.size(); ++i) {
    const auto& sort_field = sort_fields[i];
    collation.emplace_back(sort_field.getField() + 1,
                           sort_field.getSortDir() == SortDirection::Descending,
                           sort_field.getNullsPosition() == NullSortedPosition::First);
  }
  return collation;
}

bool ResultSetComparator::isSubtraitIntegerType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kI8 ||
         type.kind_case() == ::substrait::Type::KindCase::kI16 ||
         type.kind_case() == ::substrait::Type::KindCase::kI32 ||
         type.kind_case() == ::substrait::Type::KindCase::kI64;
}

bool ResultSetComparator::isSubtraitFloatType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kFp32 ||
         type.kind_case() == ::substrait::Type::KindCase::kFp64;
}

bool ResultSetComparator::isSubtraitDateTimeType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kDate ||
         type.kind_case() == ::substrait::Type::KindCase::kTime ||
         type.kind_case() == ::substrait::Type::KindCase::kTimestamp;
}

bool ResultSetComparator::isSubtraitStringType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kString ||
         type.kind_case() == ::substrait::Type::KindCase::kVarchar;
}

bool ResultSetComparator::isSubtraitDecimalType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kDecimal;
}

bool ResultSetComparator::isSubtraitBoolType(const ::substrait::Type& type) const {
  return type.kind_case() == ::substrait::Type::KindCase::kBool;
}

#define GET_TYPE_VALUE_AND_JUDGE_IS_NULL(C_TYPE, TYPE_MIN) \
  {                                                        \
    C_TYPE value = *(C_TYPE*)value_ptr;                    \
    return value == TYPE_MIN;                              \
  }

bool ResultSetComparator::isNull(const int8_t* value_ptr,
                                 const ::substrait::Type& type) const {
  switch (type.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
    case ::substrait::Type::KindCase::kI8: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(int8_t, INT8_MIN)
    }
    case ::substrait::Type::KindCase::kI16: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(int16_t, INT16_MIN)
    }
    case ::substrait::Type::KindCase::kI32: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(int32_t, INT32_MIN)
    }
    case ::substrait::Type::KindCase::kI64: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(int64_t, INT64_MIN)
    }
    case ::substrait::Type::KindCase::kFp32: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(float, INT32_MIN)
    }
    case ::substrait::Type::KindCase::kFp64:
    case ::substrait::Type::KindCase::kDecimal: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(double, INT64_MIN)
    }
    case ::substrait::Type::KindCase::kDate:
    case ::substrait::Type::KindCase::kTime:
    case ::substrait::Type::KindCase::kTimestamp: {
      GET_TYPE_VALUE_AND_JUDGE_IS_NULL(int64_t, INT64_MIN)
    }
    case ::substrait::Type::KindCase::kString: {
      // todo, string value isNull judge
      return false;
    }
    case ::substrait::Type::KindCase::kVarchar: {
      // todo, varchar value isNull judge
      return false;
    }
    default:
      throw std::runtime_error("order by not supported type: " + type.kind_case());
  }
  return false;
}

#define GET_TYPE_VALUE_AND_COMPAIR(C_TYPE)                                               \
  {                                                                                      \
    C_TYPE lhs_value = *(C_TYPE*)lhs_value_ptr;                                          \
    C_TYPE rhs_value = *(C_TYPE*)rhs_value_ptr;                                          \
    if (lhs_value != rhs_value) {                                                        \
      cmp_result = lhs_value < rhs_value ? CompairResult::Less : CompairResult::Greater; \
    }                                                                                    \
    break;                                                                               \
  }

CompairResult ResultSetComparator::compairValue(const int8_t* lhs_value_ptr,
                                                const int8_t* rhs_value_ptr,
                                                const ::substrait::Type& type) const {
  CompairResult cmp_result = CompairResult::Equal;
  switch (type.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
    case ::substrait::Type::KindCase::kI8: {
      GET_TYPE_VALUE_AND_COMPAIR(int8_t)
    }
    case ::substrait::Type::KindCase::kI16: {
      GET_TYPE_VALUE_AND_COMPAIR(int16_t)
    }
    case ::substrait::Type::KindCase::kI32: {
      GET_TYPE_VALUE_AND_COMPAIR(int32_t)
    }
    case ::substrait::Type::KindCase::kI64: {
      GET_TYPE_VALUE_AND_COMPAIR(int64_t)
    }
    case ::substrait::Type::KindCase::kFp32: {
      GET_TYPE_VALUE_AND_COMPAIR(float)
    }
    case ::substrait::Type::KindCase::kFp64:
    case ::substrait::Type::KindCase::kDecimal: {
      GET_TYPE_VALUE_AND_COMPAIR(double)
    }
    case ::substrait::Type::KindCase::kDate:
    case ::substrait::Type::KindCase::kTime:
    case ::substrait::Type::KindCase::kTimestamp: {
      GET_TYPE_VALUE_AND_COMPAIR(int64_t)
    }
    case ::substrait::Type::KindCase::kString: {
      // todo, string value compair
      break;
    }
    case ::substrait::Type::KindCase::kVarchar: {
      // todo, varchar value compair
      break;
    }
    default:
      throw std::runtime_error("order by not supported type: " + type.kind_case());
  }
  return cmp_result;
}

bool ResultSetComparator::operator()(const std::vector<int8_t*>& lhs,
                                     const std::vector<int8_t*>& rhs) const {
  int col_size = types_.size();
  for (const auto& order_entry : sort_info_.order_entries) {
    CHECK_GE(order_entry.tle_no, 1);
    CHECK_LE(order_entry.tle_no, col_size);
    const auto& type = types_[order_entry.tle_no - 1];
    int8_t* lhs_value_ptr = lhs[order_entry.tle_no - 1];
    int8_t* rhs_value_ptr = rhs[order_entry.tle_no - 1];
    if (isNull(lhs_value_ptr, type) && isNull(lhs_value_ptr, type)) {
      continue;
    }
    if (isNull(rhs_value_ptr, type) && !isNull(rhs_value_ptr, type)) {
      return order_entry.nulls_first;
    }
    if (!isNull(rhs_value_ptr, type) && isNull(rhs_value_ptr, type)) {
      return !order_entry.nulls_first;
    }
    CompairResult cmp_result =
        compairValue(reinterpret_cast<const int8_t*>(lhs_value_ptr),
                     reinterpret_cast<const int8_t*>(rhs_value_ptr),
                     type);
    if (cmp_result == CompairResult::Equal) {
      continue;
    } else if (cmp_result == CompairResult::Greater) {
      return false != order_entry.is_desc;
    } else {
      return true != order_entry.is_desc;
    }
  }
  return false;
}

}  // namespace generator
