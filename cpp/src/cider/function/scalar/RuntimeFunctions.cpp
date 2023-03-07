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

#include "function/scalar/RuntimeFunctions.h"
#include "exec/template/TypePunning.h"
#include "function/hash/MurmurHash.h"
#include "type/data/funcannotations.h"
#include "util/CiderBitUtils.h"
// #include "util/quantile.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>
#include <tuple>

// decoder implementations

#include "DecodersImpl.h"

extern "C" ALWAYS_INLINE int32_t external_call_test_sum(int32_t a, int32_t b) {
  return a + b;
}

extern "C" ALWAYS_INLINE int64_t scale_decimal_up(const int64_t operand,
                                                  const uint64_t scale,
                                                  const int64_t operand_null_val,
                                                  const int64_t result_null_val) {
  return operand != operand_null_val ? operand * scale : result_null_val;
}

extern "C" ALWAYS_INLINE int64_t scale_decimal_down_nullable(const int64_t operand,
                                                             const int64_t scale,
                                                             const int64_t null_val) {
  // rounded scale down of a decimal
  if (operand == null_val) {
    return null_val;
  }

  int64_t tmp = scale >> 1;
  tmp = operand >= 0 ? operand + tmp : operand - tmp;
  return tmp / scale;
}

extern "C" ALWAYS_INLINE int64_t scale_decimal_down(const int64_t operand,
                                                    const int64_t scale) {
  int64_t tmp = scale >> 1;
  tmp = operand >= 0 ? operand + tmp : operand - tmp;
  return tmp / scale;
}

// Return floor(dividend / divisor).
// Assumes 0 < divisor.
extern "C" ALWAYS_INLINE int64_t floor_div_lhs(const int64_t dividend,
                                               const int64_t divisor) {
  return (dividend < 0 ? dividend - (divisor - 1) : dividend) / divisor;
}

// Return floor(dividend / divisor) or NULL if dividend IS NULL.
// Assumes 0 < divisor.
extern "C" ALWAYS_INLINE int64_t floor_div_nullable_lhs(const int64_t dividend,
                                                        const int64_t divisor,
                                                        const int64_t null_val) {
  return dividend == null_val ? null_val : floor_div_lhs(dividend, divisor);
}

#define DEF_UMINUS_NULLABLE(type, null_type)                                         \
  extern "C" ALWAYS_INLINE type uminus_##type##_nullable(const type operand,         \
                                                         const null_type null_val) { \
    return operand == null_val ? null_val : -operand;                                \
  }

DEF_UMINUS_NULLABLE(int8_t, int8_t)
DEF_UMINUS_NULLABLE(int16_t, int16_t)
DEF_UMINUS_NULLABLE(int32_t, int32_t)
DEF_UMINUS_NULLABLE(int64_t, int64_t)
DEF_UMINUS_NULLABLE(float, float)
DEF_UMINUS_NULLABLE(double, double)

#undef DEF_UMINUS_NULLABLE

#define DEF_CAST_NULLABLE(from_type, to_type)                                  \
  extern "C" ALWAYS_INLINE to_type cast_##from_type##_to_##to_type##_nullable( \
      const from_type operand,                                                 \
      const from_type from_null_val,                                           \
      const to_type to_null_val) {                                             \
    return operand == from_null_val ? to_null_val : operand;                   \
  }

#define DEF_CAST_SCALED_NULLABLE(from_type, to_type)                                  \
  extern "C" ALWAYS_INLINE to_type cast_##from_type##_to_##to_type##_scaled_nullable( \
      const from_type operand,                                                        \
      const from_type from_null_val,                                                  \
      const to_type to_null_val,                                                      \
      const to_type multiplier) {                                                     \
    return operand == from_null_val ? to_null_val : multiplier * operand;             \
  }

#define DEF_CAST_NULLABLE_BIDIR(type1, type2) \
  DEF_CAST_NULLABLE(type1, type2)             \
  DEF_CAST_NULLABLE(type2, type1)

#define DEF_ROUND_NULLABLE(from_type, to_type)                                     \
  extern "C" ALWAYS_INLINE to_type cast_##from_type##_to_##to_type##_nullable(     \
      const from_type operand,                                                     \
      const from_type from_null_val,                                               \
      const to_type to_null_val) {                                                 \
    return operand == from_null_val ? to_null_val                                  \
                                    : operand < 0 ? operand - 0.5 : operand + 0.5; \
  }

DEF_CAST_NULLABLE_BIDIR(int8_t, int16_t)
DEF_CAST_NULLABLE_BIDIR(int8_t, int32_t)
DEF_CAST_NULLABLE_BIDIR(int8_t, int64_t)
DEF_CAST_NULLABLE_BIDIR(int16_t, int32_t)
DEF_CAST_NULLABLE_BIDIR(int16_t, int64_t)
DEF_CAST_NULLABLE_BIDIR(int32_t, int64_t)
DEF_CAST_NULLABLE_BIDIR(float, double)

DEF_CAST_NULLABLE(int8_t, float)
DEF_CAST_NULLABLE(int16_t, float)
DEF_CAST_NULLABLE(int32_t, float)
DEF_CAST_NULLABLE(int64_t, float)
DEF_CAST_NULLABLE(int8_t, double)
DEF_CAST_NULLABLE(int16_t, double)
DEF_CAST_NULLABLE(int32_t, double)
DEF_CAST_NULLABLE(int64_t, double)

DEF_ROUND_NULLABLE(float, int8_t)
DEF_ROUND_NULLABLE(float, int16_t)
DEF_ROUND_NULLABLE(float, int32_t)
DEF_ROUND_NULLABLE(float, int64_t)
DEF_ROUND_NULLABLE(double, int8_t)
DEF_ROUND_NULLABLE(double, int16_t)
DEF_ROUND_NULLABLE(double, int32_t)
DEF_ROUND_NULLABLE(double, int64_t)

DEF_CAST_NULLABLE(uint8_t, int32_t)
DEF_CAST_NULLABLE(uint16_t, int32_t)
DEF_CAST_SCALED_NULLABLE(int64_t, float)
DEF_CAST_SCALED_NULLABLE(int64_t, double)

#undef DEF_ROUND_NULLABLE
#undef DEF_CAST_NULLABLE_BIDIR
#undef DEF_CAST_SCALED_NULLABLE
#undef DEF_CAST_NULLABLE

extern "C" ALWAYS_INLINE int8_t logical_not(const int8_t operand, const int8_t null_val) {
  return operand == null_val ? operand : (operand ? 0 : 1);
}

extern "C" ALWAYS_INLINE int8_t logical_and(const int8_t lhs,
                                            const int8_t rhs,
                                            const int8_t null_val) {
  if (lhs == null_val) {
    return rhs == 0 ? rhs : null_val;
  }
  if (rhs == null_val) {
    return lhs == 0 ? lhs : null_val;
  }
  return (lhs && rhs) ? 1 : 0;
}

extern "C" ALWAYS_INLINE int8_t logical_or(const int8_t lhs,
                                           const int8_t rhs,
                                           const int8_t null_val) {
  if (lhs == null_val) {
    return rhs == 0 ? null_val : rhs;
  }
  if (rhs == null_val) {
    return lhs == 0 ? null_val : lhs;
  }
  return (lhs || rhs) ? 1 : 0;
}

// aggregator implementations

extern "C" ALWAYS_INLINE uint64_t agg_count(uint64_t* agg, const int64_t) {
  return (*agg)++;
}

extern "C" ALWAYS_INLINE void agg_count_distinct_bitmap(int64_t* agg,
                                                        const int64_t val,
                                                        const int64_t min_val) {
  const uint64_t bitmap_idx = val - min_val;
  reinterpret_cast<int8_t*>(*agg)[bitmap_idx >> 3] |= (1 << (bitmap_idx & 7));
}

extern "C" ALWAYS_INLINE int8_t bit_is_set(const int64_t bitset,
                                           const int64_t val,
                                           const int64_t min_val,
                                           const int64_t max_val,
                                           const int64_t null_val,
                                           const int8_t null_bool_val) {
  if (val == null_val) {
    return null_bool_val;
  }
  if (val < min_val || val > max_val) {
    return 0;
  }
  if (!bitset) {
    return 0;
  }
  const uint64_t bitmap_idx = val - min_val;
  return (reinterpret_cast<const int8_t*>(bitset))[bitmap_idx >> 3] &
                 (1 << (bitmap_idx & 7))
             ? 1
             : 0;
}

extern "C" ALWAYS_INLINE bool bit_is_set_cider(const int64_t bitset,
                                               const int64_t val,
                                               const int64_t min_val,
                                               const int64_t max_val) {
  if (val < min_val || val > max_val) {
    return false;
  }
  if (!bitset) {
    return false;
  }
  const uint64_t bitmap_idx = val - min_val;
  return (reinterpret_cast<const int8_t*>(bitset))[bitmap_idx >> 3] &
                 (1 << (bitmap_idx & 7))
             ? true
             : false;
}

extern "C" ALWAYS_INLINE int64_t agg_sum(int64_t* agg, const int64_t val) {
  const auto old = *agg;
  *agg += val;
  return old;
}

extern "C" ALWAYS_INLINE void agg_max(int64_t* agg, const int64_t val) {
  *agg = std::max(*agg, val);
}

extern "C" ALWAYS_INLINE void agg_min(int64_t* agg, const int64_t val) {
  *agg = std::min(*agg, val);
}

extern "C" ALWAYS_INLINE void agg_id(int64_t* agg, const int64_t val) {
  *agg = val;
}

extern "C" ALWAYS_INLINE int8_t* agg_id_varlen(int8_t* varlen_buffer,
                                               const int64_t offset,
                                               const int8_t* value,
                                               const int64_t size_bytes) {
  for (auto i = 0; i < size_bytes; i++) {
    varlen_buffer[offset + i] = value[i];
  }
  return &varlen_buffer[offset];
}

extern "C" ALWAYS_INLINE int32_t checked_single_agg_id(int64_t* agg,
                                                       const int64_t val,
                                                       const int64_t null_val) {
  if (val == null_val) {
    return 0;
  }

  if (*agg == val) {
    return 0;
  } else if (*agg == null_val) {
    *agg = val;
    return 0;
  } else {
    // see Execute::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES
    return 15;
  }
}

extern "C" ALWAYS_INLINE void agg_count_distinct_bitmap_skip_val(int64_t* agg,
                                                                 const int64_t val,
                                                                 const int64_t min_val,
                                                                 const int64_t skip_val) {
  if (val != skip_val) {
    agg_count_distinct_bitmap(agg, val, min_val);
  }
}

extern "C" ALWAYS_INLINE uint32_t agg_count_int32(uint32_t* agg, const int32_t) {
  return (*agg)++;
}

extern "C" ALWAYS_INLINE int32_t agg_sum_int32(int32_t* agg, const int32_t val) {
  const auto old = *agg;
  *agg += val;
  return old;
}

#define DEF_AGG_MAX_INT(n)                                                              \
  extern "C" ALWAYS_INLINE void agg_max_int##n(int##n##_t* agg, const int##n##_t val) { \
    *agg = std::max(*agg, val);                                                         \
  }

DEF_AGG_MAX_INT(32)
DEF_AGG_MAX_INT(16)
DEF_AGG_MAX_INT(8)
#undef DEF_AGG_MAX_INT

#define DEF_AGG_MIN_INT(n)                                                              \
  extern "C" ALWAYS_INLINE void agg_min_int##n(int##n##_t* agg, const int##n##_t val) { \
    *agg = std::min(*agg, val);                                                         \
  }

DEF_AGG_MIN_INT(32)
DEF_AGG_MIN_INT(16)
DEF_AGG_MIN_INT(8)
#undef DEF_AGG_MIN_INT

#define DEF_AGG_ID_INT(n)                                                              \
  extern "C" ALWAYS_INLINE void agg_id_int##n(int##n##_t* agg, const int##n##_t val) { \
    *agg = val;                                                                        \
  }

#define DEF_CHECKED_SINGLE_AGG_ID_INT(n)                                  \
  extern "C" ALWAYS_INLINE int32_t checked_single_agg_id_int##n(          \
      int##n##_t* agg, const int##n##_t val, const int##n##_t null_val) { \
    if (val == null_val) {                                                \
      return 0;                                                           \
    }                                                                     \
    if (*agg == val) {                                                    \
      return 0;                                                           \
    } else if (*agg == null_val) {                                        \
      *agg = val;                                                         \
      return 0;                                                           \
    } else {                                                              \
      /* see Execute::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES*/            \
      return 15;                                                          \
    }                                                                     \
  }

DEF_AGG_ID_INT(32)
DEF_AGG_ID_INT(16)
DEF_AGG_ID_INT(8)

DEF_CHECKED_SINGLE_AGG_ID_INT(32)
DEF_CHECKED_SINGLE_AGG_ID_INT(16)
DEF_CHECKED_SINGLE_AGG_ID_INT(8)

#undef DEF_AGG_ID_INT
#undef DEF_CHECKED_SINGLE_AGG_ID_INT

#define DEF_WRITE_PROJECTION_INT(n)                                     \
  extern "C" ALWAYS_INLINE void write_projection_int##n(                \
      int8_t* slot_ptr, const int##n##_t val, const int64_t init_val) { \
    if (val != init_val) {                                              \
      *reinterpret_cast<int##n##_t*>(slot_ptr) = val;                   \
    }                                                                   \
  }

DEF_WRITE_PROJECTION_INT(64)
DEF_WRITE_PROJECTION_INT(32)
#undef DEF_WRITE_PROJECTION_INT

extern "C" ALWAYS_INLINE int64_t agg_sum_skip_val(int64_t* agg,
                                                  const int64_t val,
                                                  const int64_t skip_val) {
  const auto old = *agg;
  if (val != skip_val) {
    if (old != skip_val) {
      return agg_sum(agg, val);
    } else {
      *agg = val;
    }
  }
  return old;
}

extern "C" ALWAYS_INLINE int32_t agg_sum_int32_skip_val(int32_t* agg,
                                                        const int32_t val,
                                                        const int32_t skip_val) {
  const auto old = *agg;
  if (val != skip_val) {
    if (old != skip_val) {
      return agg_sum_int32(agg, val);
    } else {
      *agg = val;
    }
  }
  return old;
}

extern "C" ALWAYS_INLINE uint64_t agg_count_skip_val(uint64_t* agg,
                                                     const int64_t val,
                                                     const int64_t skip_val) {
  if (val != skip_val) {
    return agg_count(agg, val);
  }
  return *agg;
}

extern "C" ALWAYS_INLINE uint32_t agg_count_int32_skip_val(uint32_t* agg,
                                                           const int32_t val,
                                                           const int32_t skip_val) {
  if (val != skip_val) {
    return agg_count_int32(agg, val);
  }
  return *agg;
}

#define DEF_SKIP_AGG_ADD(base_agg_func)                       \
  extern "C" ALWAYS_INLINE void base_agg_func##_skip_val(     \
      DATA_T* agg, const DATA_T val, const DATA_T skip_val) { \
    if (val != skip_val) {                                    \
      base_agg_func(agg, val);                                \
    }                                                         \
  }

#define DEF_SKIP_AGG(base_agg_func)                           \
  extern "C" ALWAYS_INLINE void base_agg_func##_skip_val(     \
      DATA_T* agg, const DATA_T val, const DATA_T skip_val) { \
    if (val != skip_val) {                                    \
      const DATA_T old_agg = *agg;                            \
      if (old_agg != skip_val) {                              \
        base_agg_func(agg, val);                              \
      } else {                                                \
        *agg = val;                                           \
      }                                                       \
    }                                                         \
  }

#define DATA_T int64_t
DEF_SKIP_AGG(agg_max)
DEF_SKIP_AGG(agg_min)
#undef DATA_T

#define DATA_T int32_t
DEF_SKIP_AGG(agg_max_int32)
DEF_SKIP_AGG(agg_min_int32)
#undef DATA_T

#define DATA_T int16_t
DEF_SKIP_AGG(agg_max_int16)
DEF_SKIP_AGG(agg_min_int16)
#undef DATA_T

#define DATA_T int8_t
DEF_SKIP_AGG(agg_max_int8)
DEF_SKIP_AGG(agg_min_int8)
#undef DATA_T

#undef DEF_SKIP_AGG_ADD
#undef DEF_SKIP_AGG

// TODO(alex): fix signature

extern "C" ALWAYS_INLINE uint64_t agg_count_double(uint64_t* agg, const double val) {
  return (*agg)++;
}

extern "C" ALWAYS_INLINE void agg_sum_double(int64_t* agg, const double val) {
  const auto r = *reinterpret_cast<const double*>(agg) + val;
  *agg = *reinterpret_cast<const int64_t*>(may_alias_ptr(&r));
}

extern "C" ALWAYS_INLINE void agg_max_double(int64_t* agg, const double val) {
  const auto r = std::max(*reinterpret_cast<const double*>(agg), val);
  *agg = *(reinterpret_cast<const int64_t*>(may_alias_ptr(&r)));
}

extern "C" ALWAYS_INLINE void agg_min_double(int64_t* agg, const double val) {
  const auto r = std::min(*reinterpret_cast<const double*>(agg), val);
  *agg = *(reinterpret_cast<const int64_t*>(may_alias_ptr(&r)));
}

extern "C" ALWAYS_INLINE void agg_id_double(int64_t* agg, const double val) {
  *agg = *(reinterpret_cast<const int64_t*>(may_alias_ptr(&val)));
}

extern "C" ALWAYS_INLINE int32_t checked_single_agg_id_double(int64_t* agg,
                                                              const double val,
                                                              const double null_val) {
  if (val == null_val) {
    return 0;
  }

  if (*agg == *(reinterpret_cast<const int64_t*>(may_alias_ptr(&val)))) {
    return 0;
  } else if (*agg == *(reinterpret_cast<const int64_t*>(may_alias_ptr(&null_val)))) {
    *agg = *(reinterpret_cast<const int64_t*>(may_alias_ptr(&val)));
    return 0;
  } else {
    // see Execute::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES
    return 15;
  }
}

extern "C" ALWAYS_INLINE uint32_t agg_count_float(uint32_t* agg, const float val) {
  return (*agg)++;
}

extern "C" ALWAYS_INLINE void agg_sum_float(int32_t* agg, const float val) {
  const auto r = *reinterpret_cast<const float*>(agg) + val;
  *agg = *reinterpret_cast<const int32_t*>(may_alias_ptr(&r));
}

extern "C" ALWAYS_INLINE void agg_max_float(int32_t* agg, const float val) {
  const auto r = std::max(*reinterpret_cast<const float*>(agg), val);
  *agg = *(reinterpret_cast<const int32_t*>(may_alias_ptr(&r)));
}

extern "C" ALWAYS_INLINE void agg_min_float(int32_t* agg, const float val) {
  const auto r = std::min(*reinterpret_cast<const float*>(agg), val);
  *agg = *(reinterpret_cast<const int32_t*>(may_alias_ptr(&r)));
}

extern "C" ALWAYS_INLINE void agg_id_float(int32_t* agg, const float val) {
  *agg = *(reinterpret_cast<const int32_t*>(may_alias_ptr(&val)));
}

extern "C" ALWAYS_INLINE int32_t checked_single_agg_id_float(int32_t* agg,
                                                             const float val,
                                                             const float null_val) {
  if (val == null_val) {
    return 0;
  }

  if (*agg == *(reinterpret_cast<const int32_t*>(may_alias_ptr(&val)))) {
    return 0;
  } else if (*agg == *(reinterpret_cast<const int32_t*>(may_alias_ptr(&null_val)))) {
    *agg = *(reinterpret_cast<const int32_t*>(may_alias_ptr(&val)));
    return 0;
  } else {
    // see Execute::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES
    return 15;
  }
}

extern "C" ALWAYS_INLINE uint64_t agg_count_double_skip_val(uint64_t* agg,
                                                            const double val,
                                                            const double skip_val) {
  if (val != skip_val) {
    return agg_count_double(agg, val);
  }
  return *agg;
}

extern "C" ALWAYS_INLINE uint32_t agg_count_float_skip_val(uint32_t* agg,
                                                           const float val,
                                                           const float skip_val) {
  if (val != skip_val) {
    return agg_count_float(agg, val);
  }
  return *agg;
}

#define DEF_SKIP_AGG_ADD(base_agg_func)                       \
  extern "C" ALWAYS_INLINE void base_agg_func##_skip_val(     \
      ADDR_T* agg, const DATA_T val, const DATA_T skip_val) { \
    if (val != skip_val) {                                    \
      base_agg_func(agg, val);                                \
    }                                                         \
  }

#define DEF_SKIP_AGG(base_agg_func)                                                \
  extern "C" ALWAYS_INLINE void base_agg_func##_skip_val(                          \
      ADDR_T* agg, const DATA_T val, const DATA_T skip_val) {                      \
    if (val != skip_val) {                                                         \
      const ADDR_T old_agg = *agg;                                                 \
      if (old_agg != *reinterpret_cast<const ADDR_T*>(may_alias_ptr(&skip_val))) { \
        base_agg_func(agg, val);                                                   \
      } else {                                                                     \
        *agg = *reinterpret_cast<const ADDR_T*>(may_alias_ptr(&val));              \
      }                                                                            \
    }                                                                              \
  }

#define DATA_T double
#define ADDR_T int64_t
DEF_SKIP_AGG(agg_sum_double)
DEF_SKIP_AGG(agg_max_double)
DEF_SKIP_AGG(agg_min_double)
#undef ADDR_T
#undef DATA_T

#define DATA_T float
#define ADDR_T int32_t
DEF_SKIP_AGG(agg_sum_float)
DEF_SKIP_AGG(agg_max_float)
DEF_SKIP_AGG(agg_min_float)
#undef ADDR_T
#undef DATA_T

#undef DEF_SKIP_AGG_ADD
#undef DEF_SKIP_AGG

extern "C" ALWAYS_INLINE int64_t decimal_floor(const int64_t x, const int64_t scale) {
  if (x >= 0) {
    return x / scale * scale;
  }
  if (!(x % scale)) {
    return x;
  }
  return x / scale * scale - scale;
}

extern "C" ALWAYS_INLINE int64_t decimal_ceil(const int64_t x, const int64_t scale) {
  return decimal_floor(x, scale) + (x % scale ? scale : 0);
}

// x64 stride functions

extern "C" NEVER_INLINE int32_t pos_start_impl(int32_t* error_code) {
  int32_t row_index_resume{0};
  if (error_code) {
    row_index_resume = error_code[0];
    error_code[0] = 0;
  }
  return row_index_resume;
}

extern "C" NEVER_INLINE int32_t group_buff_idx_impl() {
  return pos_start_impl(nullptr);
}

extern "C" NEVER_INLINE int32_t pos_step_impl() {
  return 1;
}

extern "C" ALWAYS_INLINE void record_error_code(const int32_t err_code,
                                                int32_t* error_codes) {
  // NB: never override persistent error codes (with code greater than zero).
  // If a persistent error
  // (division by zero, for example) occurs before running out of slots, we
  // have to avoid overriding it, because there's a risk that the query would
  // go through if we override with a potentially benign out-of-slots code.
  if (err_code && error_codes[pos_start_impl(nullptr)] <= 0) {
    error_codes[pos_start_impl(nullptr)] = err_code;
  }
}

extern "C" ALWAYS_INLINE int32_t get_error_code(int32_t* error_codes) {
  return error_codes[pos_start_impl(nullptr)];
}

extern "C" ALWAYS_INLINE int8_t* extract_str_ptr(const uint64_t str_and_len) {
  return reinterpret_cast<int8_t*>(str_and_len & 0xffffffffffff);
}

extern "C" ALWAYS_INLINE int32_t extract_str_len(const uint64_t str_and_len) {
  return static_cast<int64_t>(str_and_len) >> 48;
}

extern "C" NEVER_INLINE int8_t* extract_str_ptr_noinline(const uint64_t str_and_len) {
  return extract_str_ptr(str_and_len);
}

extern "C" NEVER_INLINE int32_t extract_str_len_noinline(const uint64_t str_and_len) {
  return extract_str_len(str_and_len);
}

extern "C" ALWAYS_INLINE uint64_t string_pack(const int8_t* ptr, const int32_t len) {
  return (reinterpret_cast<const uint64_t>(ptr) & 0xffffffffffff) |
         (static_cast<const uint64_t>(len) << 48);
}

#ifdef __clang__
#include "function/string/StringLike.cpp"
#endif

extern "C" ALWAYS_INLINE int32_t char_length(const char* str, const int32_t str_len) {
  return str_len;
}

extern "C" ALWAYS_INLINE int32_t char_length_nullable(const char* str,
                                                      const int32_t str_len,
                                                      const int32_t int_null) {
  if (!str) {
    return int_null;
  }
  return str_len;
}

extern "C" ALWAYS_INLINE int32_t key_for_string_encoded(const int32_t str_id) {
  return str_id;
}

extern "C" ALWAYS_INLINE bool sample_ratio(const double proportion,
                                           const int64_t row_offset) {
  const int64_t threshold = 4294967296 * proportion;
  return (row_offset * 2654435761) % 4294967296 < threshold;
}

extern "C" ALWAYS_INLINE double width_bucket(const double target_value,
                                             const double lower_bound,
                                             const double upper_bound,
                                             const double scale_factor,
                                             const int32_t partition_count) {
  if (target_value < lower_bound) {
    return 0;
  } else if (target_value >= upper_bound) {
    return partition_count + 1;
  }
  return ((target_value - lower_bound) * scale_factor) + 1;
}

extern "C" ALWAYS_INLINE double width_bucket_reversed(const double target_value,
                                                      const double lower_bound,
                                                      const double upper_bound,
                                                      const double scale_factor,
                                                      const int32_t partition_count) {
  if (target_value > lower_bound) {
    return 0;
  } else if (target_value <= upper_bound) {
    return partition_count + 1;
  }
  return ((lower_bound - target_value) * scale_factor) + 1;
}

extern "C" ALWAYS_INLINE double width_bucket_nullable(const double target_value,
                                                      const double lower_bound,
                                                      const double upper_bound,
                                                      const double scale_factor,
                                                      const int32_t partition_count,
                                                      const double null_val) {
  if (target_value == null_val) {
    return INT32_MIN;
  }
  return width_bucket(
      target_value, lower_bound, upper_bound, scale_factor, partition_count);
}

extern "C" ALWAYS_INLINE double width_bucket_reversed_nullable(
    const double target_value,
    const double lower_bound,
    const double upper_bound,
    const double scale_factor,
    const int32_t partition_count,
    const double null_val) {
  if (target_value == null_val) {
    return INT32_MIN;
  }
  return width_bucket_reversed(
      target_value, lower_bound, upper_bound, scale_factor, partition_count);
}

// width_bucket with no out-of-bound check version which can be called
// if we can assure the input target_value expr always resides in the valid range
// (so we can also avoid null checking)
extern "C" ALWAYS_INLINE double width_bucket_no_oob_check(const double target_value,
                                                          const double lower_bound,
                                                          const double scale_factor) {
  return ((target_value - lower_bound) * scale_factor) + 1;
}

extern "C" ALWAYS_INLINE double width_bucket_reversed_no_oob_check(
    const double target_value,
    const double lower_bound,
    const double scale_factor) {
  return ((lower_bound - target_value) * scale_factor) + 1;
}

extern "C" ALWAYS_INLINE double width_bucket_expr(const double target_value,
                                                  const bool reversed,
                                                  const double lower_bound,
                                                  const double upper_bound,
                                                  const int32_t partition_count) {
  if (reversed) {
    return width_bucket_reversed(target_value,
                                 lower_bound,
                                 upper_bound,
                                 partition_count / (lower_bound - upper_bound),
                                 partition_count);
  }
  return width_bucket(target_value,
                      lower_bound,
                      upper_bound,
                      partition_count / (upper_bound - lower_bound),
                      partition_count);
}

extern "C" ALWAYS_INLINE double width_bucket_expr_nullable(const double target_value,
                                                           const bool reversed,
                                                           const double lower_bound,
                                                           const double upper_bound,
                                                           const int32_t partition_count,
                                                           const double null_val) {
  if (target_value == null_val) {
    return INT32_MIN;
  }
  return width_bucket_expr(
      target_value, reversed, lower_bound, upper_bound, partition_count);
}

extern "C" ALWAYS_INLINE double width_bucket_expr_no_oob_check(
    const double target_value,
    const bool reversed,
    const double lower_bound,
    const double upper_bound,
    const int32_t partition_count) {
  if (reversed) {
    return width_bucket_reversed_no_oob_check(
        target_value, lower_bound, partition_count / (lower_bound - upper_bound));
  }
  return width_bucket_no_oob_check(
      target_value, lower_bound, partition_count / (upper_bound - lower_bound));
}

extern "C" ALWAYS_INLINE int64_t row_number_window_func(const int64_t output_buff,
                                                        const int64_t pos) {
  return reinterpret_cast<const int64_t*>(output_buff)[pos];
}

extern "C" ALWAYS_INLINE double percent_window_func(const int64_t output_buff,
                                                    const int64_t pos) {
  return reinterpret_cast<const double*>(output_buff)[pos];
}

extern "C" ALWAYS_INLINE double load_double(const int64_t* agg) {
  return *reinterpret_cast<const double*>(may_alias_ptr(agg));
}

extern "C" ALWAYS_INLINE float load_float(const int32_t* agg) {
  return *reinterpret_cast<const float*>(may_alias_ptr(agg));
}

extern "C" ALWAYS_INLINE double load_avg_int(const int64_t* sum,
                                             const int64_t* count,
                                             const double null_val) {
  return *count != 0 ? static_cast<double>(*sum) / *count : null_val;
}

extern "C" ALWAYS_INLINE double load_avg_decimal(const int64_t* sum,
                                                 const int64_t* count,
                                                 const double null_val,
                                                 const uint32_t scale) {
  return *count != 0 ? (static_cast<double>(*sum) / pow(10, scale)) / *count : null_val;
}

extern "C" ALWAYS_INLINE double load_avg_double(const int64_t* agg,
                                                const int64_t* count,
                                                const double null_val) {
  return *count != 0 ? *reinterpret_cast<const double*>(may_alias_ptr(agg)) / *count
                     : null_val;
}

extern "C" ALWAYS_INLINE double load_avg_float(const int32_t* agg,
                                               const int32_t* count,
                                               const double null_val) {
  return *count != 0 ? *reinterpret_cast<const float*>(may_alias_ptr(agg)) / *count
                     : null_val;
}

extern "C" NEVER_INLINE void linear_probabilistic_count(uint8_t* bitmap,
                                                        const uint32_t bitmap_bytes,
                                                        const uint8_t* key_bytes,
                                                        const uint32_t key_len) {
  const uint32_t bit_pos = MurmurHash3(key_bytes, key_len, 0) % (bitmap_bytes * 8);
  const uint32_t word_idx = bit_pos / 32;
  const uint32_t bit_idx = bit_pos % 32;
  reinterpret_cast<uint32_t*>(bitmap)[word_idx] |= 1 << bit_idx;
}

extern "C" NEVER_INLINE void query_stub_hoisted_literals_with_row_skip_mask(
    const int8_t** col_buffers,
    const int8_t* literals,
    const int64_t* num_rows,
    const uint64_t* frag_row_offsets,
    const int32_t* max_matched,
    const int64_t* init_agg_value,
    int64_t** out,
    uint32_t frag_idx,
    const int64_t* join_hash_tables,
    int32_t* total_matched,
    int32_t* error_code,
    uint8_t* row_skip_mask) {
#ifndef _WIN32
  assert(col_buffers || row_skip_mask || literals || num_rows || frag_row_offsets ||
         max_matched || init_agg_value || out || frag_idx || error_code ||
         join_hash_tables || total_matched);
#endif
}

extern "C" void query_hoisted_literals_with_row_skip_mask(
    const int8_t*** input_buffers,
    const uint64_t* __restrict__ num_fragments,
    const int8_t* literals,
    const int64_t* num_rows,
    const uint64_t* frag_row_offsets,
    const int32_t* max_matched,
    int32_t* total_matched,
    const int64_t* init_agg_value,
    int64_t** out,
    int32_t* error_code,
    const uint32_t* __restrict__ num_tables_ptr,
    const int64_t* join_hash_tables) {
  const int8_t** col_buffers = input_buffers ? input_buffers[0] : nullptr;
  const uint8_t* row_skip_mask =
      input_buffers ? reinterpret_cast<const uint8_t*>(input_buffers[1]) : nullptr;
  query_stub_hoisted_literals_with_row_skip_mask(col_buffers,
                                                 literals,
                                                 num_rows,
                                                 frag_row_offsets,
                                                 max_matched,
                                                 init_agg_value,
                                                 out,
                                                 0,
                                                 join_hash_tables,
                                                 total_matched,
                                                 error_code,
                                                 const_cast<uint8_t*>(row_skip_mask));
}

extern "C" NEVER_INLINE void query_stub_hoisted_literals(const int8_t** col_buffers,
                                                         const int8_t* literals,
                                                         const int64_t* num_rows,
                                                         const uint64_t* frag_row_offsets,
                                                         const int32_t* max_matched,
                                                         const int64_t* init_agg_value,
                                                         int64_t** out,
                                                         uint32_t frag_idx,
                                                         const int64_t* join_hash_tables,
                                                         int32_t* total_matched,
                                                         int32_t* error_code) {
#ifndef _WIN32
  assert(col_buffers || literals || num_rows || frag_row_offsets || max_matched ||
         init_agg_value || out || frag_idx || error_code || join_hash_tables ||
         total_matched);
#endif
}

extern "C" void multifrag_query_hoisted_literals(
    const int8_t*** col_buffers,
    const uint64_t* __restrict__ num_fragments,
    const int8_t* literals,
    const int64_t* num_rows,
    const uint64_t* frag_row_offsets,
    const int32_t* max_matched,
    int32_t* total_matched,
    const int64_t* init_agg_value,
    int64_t** out,
    int32_t* error_code,
    const uint32_t* __restrict__ num_tables_ptr,
    const int64_t* join_hash_tables) {
  for (uint32_t i = 0; i < *num_fragments; ++i) {
    query_stub_hoisted_literals(col_buffers ? col_buffers[i] : nullptr,
                                literals,
                                &num_rows[i * (*num_tables_ptr)],
                                &frag_row_offsets[i * (*num_tables_ptr)],
                                max_matched,
                                init_agg_value,
                                out,
                                i,
                                join_hash_tables,
                                total_matched,
                                error_code);
  }
}

extern "C" NEVER_INLINE void query_stub(const int8_t** col_buffers,
                                        const int64_t* num_rows,
                                        const uint64_t* frag_row_offsets,
                                        const int32_t* max_matched,
                                        const int64_t* init_agg_value,
                                        int64_t** out,
                                        uint32_t frag_idx,
                                        const int64_t* join_hash_tables,
                                        int32_t* total_matched,
                                        int32_t* error_code) {
#ifndef _WIN32
  assert(col_buffers || num_rows || frag_row_offsets || max_matched || init_agg_value ||
         out || frag_idx || error_code || join_hash_tables || total_matched);
#endif
}

extern "C" void multifrag_query(const int8_t*** col_buffers,
                                const uint64_t* __restrict__ num_fragments,
                                const int64_t* num_rows,
                                const uint64_t* frag_row_offsets,
                                const int32_t* max_matched,
                                int32_t* total_matched,
                                const int64_t* init_agg_value,
                                int64_t** out,
                                int32_t* error_code,
                                const uint32_t* __restrict__ num_tables_ptr,
                                const int64_t* join_hash_tables) {
  for (uint32_t i = 0; i < *num_fragments; ++i) {
    query_stub(col_buffers ? col_buffers[i] : nullptr,
               &num_rows[i * (*num_tables_ptr)],
               &frag_row_offsets[i * (*num_tables_ptr)],
               max_matched,
               init_agg_value,
               out,
               i,
               join_hash_tables,
               total_matched,
               error_code);
  }
}

extern "C" ALWAYS_INLINE bool check_interrupt() {
  if (check_interrupt_init(static_cast<unsigned>(INT_CHECK))) {
    return true;
  }
  return false;
}

extern "C" bool check_interrupt_init(unsigned command) {
  static std::atomic_bool runtime_interrupt_flag{false};

  if (command == static_cast<unsigned>(INT_CHECK)) {
    if (runtime_interrupt_flag.load()) {
      return true;
    }
    return false;
  }
  if (command == static_cast<unsigned>(INT_ABORT)) {
    runtime_interrupt_flag.store(true);
    return false;
  }
  if (command == static_cast<unsigned>(INT_RESET)) {
    runtime_interrupt_flag.store(false);
    return false;
  }
  return false;
}

extern "C" ALWAYS_INLINE bool check_dictionary_is_null(int8_t* dictionary) {
  bool ret = dictionary == nullptr;
  return ret;
}

extern "C" ALWAYS_INLINE int get_str_length_from_dictionary(int8_t* dictionary,
                                                            uint64_t index) {
  const int32_t* offset_buffer = reinterpret_cast<const int32_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
  return offset_buffer[index + 1] - offset_buffer[index];
}

extern "C" ALWAYS_INLINE const int8_t* get_str_ptr_from_dictionary(int8_t* dictionary,
                                                                   uint64_t index) {
  const int32_t* offset_buffer = reinterpret_cast<const int32_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
  const int8_t* data_buffer = reinterpret_cast<const int8_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[2]);

  return data_buffer + offset_buffer[index];
}

extern "C" ALWAYS_INLINE const int32_t
get_str_length_from_dictionary_or_buffer(int8_t* dictionary,
                                         uint64_t index,
                                         int32_t* offset_buffer) {
  if (dictionary) {
    const int32_t* actual_offset_buffer = reinterpret_cast<const int32_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
    int32_t index_in_dict = offset_buffer[index];
    int len =
        actual_offset_buffer[index_in_dict + 1] - actual_offset_buffer[index_in_dict];
    return len;
  } else {
    return offset_buffer[index + 1] - offset_buffer[index];
  }
}

extern "C" ALWAYS_INLINE const int8_t* get_str_ptr_from_dictionary_or_buffer(
    int8_t* dictionary,
    uint64_t index,
    int32_t* offset_buffer,
    int8_t* data_buffer) {
  if (dictionary) {
    int32_t index_in_dict = offset_buffer[index];
    const int32_t* actual_offset_buffer = reinterpret_cast<const int32_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
    const int8_t* actual_data_buffer = reinterpret_cast<const int8_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[2]);
    return actual_data_buffer + actual_offset_buffer[index_in_dict];
  } else {
    return data_buffer + offset_buffer[index];
  }
}

extern "C" ALWAYS_INLINE bool check_bit_vector_set(uint8_t* bit_vector, uint64_t index) {
  return CiderBitUtils::isBitSetAt(bit_vector, index);
}

extern "C" ALWAYS_INLINE bool check_bit_vector_clear(uint8_t* bit_vector,
                                                     uint64_t index) {
  bool ans = !CiderBitUtils::isBitSetAt(bit_vector, index);
  return ans;
}
extern "C" ALWAYS_INLINE bool check_bit_vector_clear_opt(uint8_t* bit_vector,
                                                         uint64_t index) {
  return CiderBitUtils::isBitClearAt(bit_vector, index);
}

extern "C" ALWAYS_INLINE void set_bit_vector(uint8_t* bit_vector, uint64_t index) {
  CiderBitUtils::setBitAt(bit_vector, index);
}

extern "C" ALWAYS_INLINE void clear_bit_vector(uint8_t* bit_vector, uint64_t index) {
  CiderBitUtils::clearBitAt(bit_vector, index);
}

// For temporary use
extern "C" ALWAYS_INLINE void set_null_vector_bit(uint8_t* bit_vector,
                                                  uint64_t index,
                                                  bool is_null) {
  is_null ? CiderBitUtils::clearBitAt(bit_vector, index)
          : CiderBitUtils::setBitAt(bit_vector, index);
}
extern "C" ALWAYS_INLINE void set_null_vector_bit_opt(uint8_t* bit_vector,
                                                      uint64_t index,
                                                      bool is_null) {
  CiderBitUtils::setBitAtUnified(bit_vector, index, is_null);
}

extern "C" ALWAYS_INLINE void do_memcpy(int8_t* dst, int8_t* src, int32_t len) {
  memcpy(dst, src, len);
}

extern "C" ALWAYS_INLINE void bitwise_and_2(uint8_t* output,
                                            const uint8_t* a,
                                            const uint8_t* b,
                                            uint64_t bit_num) {
  CiderBitUtils::bitwiseAnd(output, a, b, bit_num);
}

extern "C" ALWAYS_INLINE void null_buffer_memcpy(int8_t* dst,
                                                 int8_t* src,
                                                 int64_t bit_num) {
  memcpy(dst, src, (bit_num + 7) >> 3);
}

extern "C" ALWAYS_INLINE int8_t* extract_str_ptr_arrow(int8_t* data_buffer,
                                                       int8_t* offset_buffer,
                                                       uint64_t pos) {
  return (data_buffer + reinterpret_cast<int32_t*>(offset_buffer)[pos]);
}

extern "C" ALWAYS_INLINE int32_t extract_str_len_arrow(int8_t* offset_buffer,
                                                       uint64_t pos) {
  int32_t* offset = reinterpret_cast<int32_t*>(offset_buffer);
  return offset[pos + 1] - offset[pos];
}

#include "ExtensionFunctions.hpp"
// Clang compiler will automatically optimize away unused functions.
extern "C" RUNTIME_EXPORT NEVER_INLINE bool cider_Between(const double x,
                                                          const double low,
                                                          const double high) {
  return Between(x, low, high);
}

extern "C" RUNTIME_EXPORT NEVER_INLINE bool cider_Between__3(const int64_t x,
                                                             const int64_t low,
                                                             const int64_t high) {
  return Between__3(x, low, high);
}

#include "exec/nextgen/context/ContextRuntimeFunctions.h"
#include "exec/nextgen/function/CiderDateFunctions.cpp"
#include "exec/nextgen/function/CiderSetFunctions.cpp"
#include "exec/nextgen/function/CiderStringFunctions.cpp"
#include "exec/nextgen/operators/OperatorRuntimeFunctions.h"
#include "function/aggregate/CiderRuntimeFunctions.h"
