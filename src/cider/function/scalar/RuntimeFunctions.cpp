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

#include "function/scalar/RuntimeFunctions.h"
#include "exec/template/BufferCompaction.h"
#include "exec/template/HyperLogLogRank.h"
#include "exec/template/TypePunning.h"
#include "function/hash/MurmurHash.h"
#include "type/data/funcannotations.h"
#include "util/CiderBitUtils.h"
#include "util/quantile.h"

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

// arithmetic operator implementations

#define DEF_ARITH_NULLABLE(type, null_type, opname, opsym)        \
  extern "C" ALWAYS_INLINE type opname##_##type##_nullable(       \
      const type lhs, const type rhs, const null_type null_val) { \
    if (lhs != null_val && rhs != null_val) {                     \
      return lhs opsym rhs;                                       \
    }                                                             \
    return null_val;                                              \
  }

#define DEF_ARITH_NULLABLE_LHS(type, null_type, opname, opsym)    \
  extern "C" ALWAYS_INLINE type opname##_##type##_nullable_lhs(   \
      const type lhs, const type rhs, const null_type null_val) { \
    if (lhs != null_val) {                                        \
      return lhs opsym rhs;                                       \
    }                                                             \
    return null_val;                                              \
  }

#define DEF_ARITH_NULLABLE_RHS(type, null_type, opname, opsym)    \
  extern "C" ALWAYS_INLINE type opname##_##type##_nullable_rhs(   \
      const type lhs, const type rhs, const null_type null_val) { \
    if (rhs != null_val) {                                        \
      return lhs opsym rhs;                                       \
    }                                                             \
    return null_val;                                              \
  }

#define DEF_CMP_NULLABLE(type, null_type, opname, opsym)      \
  extern "C" ALWAYS_INLINE int8_t opname##_##type##_nullable( \
      const type lhs,                                         \
      const type rhs,                                         \
      const null_type null_val,                               \
      const int8_t null_bool_val) {                           \
    if (lhs != null_val && rhs != null_val) {                 \
      return lhs opsym rhs;                                   \
    }                                                         \
    return null_bool_val;                                     \
  }

#define DEF_CMP_NULLABLE_LHS(type, null_type, opname, opsym)      \
  extern "C" ALWAYS_INLINE int8_t opname##_##type##_nullable_lhs( \
      const type lhs,                                             \
      const type rhs,                                             \
      const null_type null_val,                                   \
      const int8_t null_bool_val) {                               \
    if (lhs != null_val) {                                        \
      return lhs opsym rhs;                                       \
    }                                                             \
    return null_bool_val;                                         \
  }

#define DEF_CMP_NULLABLE_RHS(type, null_type, opname, opsym)      \
  extern "C" ALWAYS_INLINE int8_t opname##_##type##_nullable_rhs( \
      const type lhs,                                             \
      const type rhs,                                             \
      const null_type null_val,                                   \
      const int8_t null_bool_val) {                               \
    if (rhs != null_val) {                                        \
      return lhs opsym rhs;                                       \
    }                                                             \
    return null_bool_val;                                         \
  }

#define DEF_SAFE_DIV_NULLABLE(type, null_type, opname)            \
  extern "C" ALWAYS_INLINE type safe_div_##type(                  \
      const type lhs, const type rhs, const null_type null_val) { \
    if (lhs != null_val && rhs != null_val && rhs != 0) {         \
      return lhs / rhs;                                           \
    }                                                             \
    return null_val;                                              \
  }

#define DEF_SAFE_INF_DIV_NULLABLE(type, null_type, opname)                      \
  extern "C" ALWAYS_INLINE type safe_inf_div_##type(const type lhs,             \
                                                    const type rhs,             \
                                                    const null_type inf_val,    \
                                                    const null_type null_val) { \
    if (rhs != 0) {                                                             \
      return lhs / rhs;                                                         \
    }                                                                           \
    if (lhs > 0) {                                                              \
      return inf_val;                                                           \
    } else if (lhs == 0) {                                                      \
      return null_val;                                                          \
    } else {                                                                    \
      return -inf_val;                                                          \
    }                                                                           \
  }

#define DEF_BINARY_NULLABLE_ALL_OPS(type, null_type) \
  DEF_ARITH_NULLABLE(type, null_type, add, +)        \
  DEF_ARITH_NULLABLE(type, null_type, sub, -)        \
  DEF_ARITH_NULLABLE(type, null_type, mul, *)        \
  DEF_ARITH_NULLABLE(type, null_type, div, /)        \
  DEF_SAFE_DIV_NULLABLE(type, null_type, safe_div)   \
  DEF_ARITH_NULLABLE_LHS(type, null_type, add, +)    \
  DEF_ARITH_NULLABLE_LHS(type, null_type, sub, -)    \
  DEF_ARITH_NULLABLE_LHS(type, null_type, mul, *)    \
  DEF_ARITH_NULLABLE_LHS(type, null_type, div, /)    \
  DEF_ARITH_NULLABLE_RHS(type, null_type, add, +)    \
  DEF_ARITH_NULLABLE_RHS(type, null_type, sub, -)    \
  DEF_ARITH_NULLABLE_RHS(type, null_type, mul, *)    \
  DEF_ARITH_NULLABLE_RHS(type, null_type, div, /)    \
  DEF_CMP_NULLABLE(type, null_type, eq, ==)          \
  DEF_CMP_NULLABLE(type, null_type, ne, !=)          \
  DEF_CMP_NULLABLE(type, null_type, lt, <)           \
  DEF_CMP_NULLABLE(type, null_type, gt, >)           \
  DEF_CMP_NULLABLE(type, null_type, le, <=)          \
  DEF_CMP_NULLABLE(type, null_type, ge, >=)          \
  DEF_CMP_NULLABLE_LHS(type, null_type, eq, ==)      \
  DEF_CMP_NULLABLE_LHS(type, null_type, ne, !=)      \
  DEF_CMP_NULLABLE_LHS(type, null_type, lt, <)       \
  DEF_CMP_NULLABLE_LHS(type, null_type, gt, >)       \
  DEF_CMP_NULLABLE_LHS(type, null_type, le, <=)      \
  DEF_CMP_NULLABLE_LHS(type, null_type, ge, >=)      \
  DEF_CMP_NULLABLE_RHS(type, null_type, eq, ==)      \
  DEF_CMP_NULLABLE_RHS(type, null_type, ne, !=)      \
  DEF_CMP_NULLABLE_RHS(type, null_type, lt, <)       \
  DEF_CMP_NULLABLE_RHS(type, null_type, gt, >)       \
  DEF_CMP_NULLABLE_RHS(type, null_type, le, <=)      \
  DEF_CMP_NULLABLE_RHS(type, null_type, ge, >=)

DEF_BINARY_NULLABLE_ALL_OPS(int8_t, int64_t)
DEF_BINARY_NULLABLE_ALL_OPS(int16_t, int64_t)
DEF_BINARY_NULLABLE_ALL_OPS(int32_t, int64_t)
DEF_BINARY_NULLABLE_ALL_OPS(int64_t, int64_t)
DEF_BINARY_NULLABLE_ALL_OPS(float, float)
DEF_BINARY_NULLABLE_ALL_OPS(double, double)
DEF_ARITH_NULLABLE(int8_t, int64_t, mod, %)
DEF_ARITH_NULLABLE(int16_t, int64_t, mod, %)
DEF_ARITH_NULLABLE(int32_t, int64_t, mod, %)
DEF_ARITH_NULLABLE(int64_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_LHS(int8_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_LHS(int16_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_LHS(int32_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_LHS(int64_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_RHS(int8_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_RHS(int16_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_RHS(int32_t, int64_t, mod, %)
DEF_ARITH_NULLABLE_RHS(int64_t, int64_t, mod, %)
DEF_SAFE_INF_DIV_NULLABLE(float, float, safe_inf_div)
DEF_SAFE_INF_DIV_NULLABLE(double, double, safe_inf_div)

#undef DEF_BINARY_NULLABLE_ALL_OPS
#undef DEF_SAFE_DIV_NULLABLE
#undef DEF_CMP_NULLABLE_RHS
#undef DEF_CMP_NULLABLE_LHS
#undef DEF_CMP_NULLABLE
#undef DEF_ARITH_NULLABLE_RHS
#undef DEF_ARITH_NULLABLE_LHS
#undef DEF_ARITH_NULLABLE
#undef DEF_SAFE_INF_DIV_NULLABLE

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

extern "C" NEVER_INLINE void agg_approximate_count_distinct(int64_t* agg,
                                                            const int64_t key,
                                                            const uint32_t b) {
  const uint64_t hash = MurmurHash64A(&key, sizeof(key), 0);
  const uint32_t index = hash >> (64 - b);
  const uint8_t rank = get_rank(hash << b, 64 - b);
  uint8_t* M = reinterpret_cast<uint8_t*>(*agg);
  M[index] = std::max(M[index], rank);
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

// group by helpers

extern "C" NEVER_INLINE const int64_t* init_shared_mem_nop(
    const int64_t* groups_buffer,
    const int32_t groups_buffer_size) {
  return groups_buffer;
}

extern "C" NEVER_INLINE void write_back_nop(int64_t* dest,
                                            int64_t* src,
                                            const int32_t sz) {
#ifndef _WIN32
  // the body is not really needed, just make sure the call is not optimized away
  assert(dest);
#endif
}

extern "C" int64_t* init_shared_mem(const int64_t* global_groups_buffer,
                                    const int32_t groups_buffer_size) {
  return nullptr;
}

extern "C" NEVER_INLINE void init_group_by_buffer_impl(
    int64_t* groups_buffer,
    const int64_t* init_vals,
    const uint32_t groups_buffer_entry_count,
    const uint32_t key_qw_count,
    const uint32_t agg_col_count,
    const bool keyless,
    const int8_t warp_size) {
#ifndef _WIN32
  // the body is not really needed, just make sure the call is not optimized away
  assert(groups_buffer);
#endif
}

template <typename T>
ALWAYS_INLINE int64_t* get_matching_group_value(int64_t* groups_buffer,
                                                const uint32_t h,
                                                const T* key,
                                                const uint32_t key_count,
                                                const uint32_t row_size_quad) {
  auto off = h * row_size_quad;
  auto row_ptr = reinterpret_cast<T*>(groups_buffer + off);
  if (*row_ptr == get_empty_key<T>()) {
    memcpy(row_ptr, key, key_count * sizeof(T));
    auto row_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr + key_count);
    return reinterpret_cast<int64_t*>(align_to_int64(row_ptr_i8));
  }
  if (memcmp(row_ptr, key, key_count * sizeof(T)) == 0) {
    auto row_ptr_i8 = reinterpret_cast<int8_t*>(row_ptr + key_count);
    return reinterpret_cast<int64_t*>(align_to_int64(row_ptr_i8));
  }
  return nullptr;
}

extern "C" ALWAYS_INLINE int64_t* get_matching_group_value(int64_t* groups_buffer,
                                                           const uint32_t h,
                                                           const int64_t* key,
                                                           const uint32_t key_count,
                                                           const uint32_t key_width,
                                                           const uint32_t row_size_quad) {
  switch (key_width) {
    case 4:
      return get_matching_group_value(groups_buffer,
                                      h,
                                      reinterpret_cast<const int32_t*>(key),
                                      key_count,
                                      row_size_quad);
    case 8:
      return get_matching_group_value(groups_buffer, h, key, key_count, row_size_quad);
    default:;
  }
  return nullptr;
}

template <typename T>
ALWAYS_INLINE int32_t get_matching_group_value_columnar_slot(int64_t* groups_buffer,
                                                             const uint32_t entry_count,
                                                             const uint32_t h,
                                                             const T* key,
                                                             const uint32_t key_count) {
  auto off = h;
  auto key_buffer = reinterpret_cast<T*>(groups_buffer);
  if (key_buffer[off] == get_empty_key<T>()) {
    for (size_t i = 0; i < key_count; ++i) {
      key_buffer[off] = key[i];
      off += entry_count;
    }
    return h;
  }
  off = h;
  for (size_t i = 0; i < key_count; ++i) {
    if (key_buffer[off] != key[i]) {
      return -1;
    }
    off += entry_count;
  }
  return h;
}

extern "C" ALWAYS_INLINE int32_t
get_matching_group_value_columnar_slot(int64_t* groups_buffer,
                                       const uint32_t entry_count,
                                       const uint32_t h,
                                       const int64_t* key,
                                       const uint32_t key_count,
                                       const uint32_t key_width) {
  switch (key_width) {
    case 4:
      return get_matching_group_value_columnar_slot(groups_buffer,
                                                    entry_count,
                                                    h,
                                                    reinterpret_cast<const int32_t*>(key),
                                                    key_count);
    case 8:
      return get_matching_group_value_columnar_slot(
          groups_buffer, entry_count, h, key, key_count);
    default:
      return -1;
  }
  return -1;
}

extern "C" ALWAYS_INLINE int64_t* get_matching_group_value_columnar(
    int64_t* groups_buffer,
    const uint32_t h,
    const int64_t* key,
    const uint32_t key_qw_count,
    const size_t entry_count) {
  auto off = h;
  if (groups_buffer[off] == empty_key_64) {
    for (size_t i = 0; i < key_qw_count; ++i) {
      groups_buffer[off] = key[i];
      off += entry_count;
    }
    return &groups_buffer[off];
  }
  off = h;
  for (size_t i = 0; i < key_qw_count; ++i) {
    if (groups_buffer[off] != key[i]) {
      return nullptr;
    }
    off += entry_count;
  }
  return &groups_buffer[off];
}

/*
 * For a particular hashed_index, returns the row-wise offset
 * to the first matching agg column in memory.
 * It also checks the corresponding group column, and initialize all
 * available keys if they are not empty (it is assumed all group columns are
 * 64-bit wide).
 *
 * Memory layout:
 *
 * | prepended group columns (64-bit each) | agg columns |
 */
extern "C" ALWAYS_INLINE int64_t* get_matching_group_value_perfect_hash(
    int64_t* groups_buffer,
    const uint32_t hashed_index,
    const int64_t* key,
    const uint32_t key_count,
    const uint32_t row_size_quad) {
  uint32_t off = hashed_index * row_size_quad;
  if (groups_buffer[off] == empty_key_64) {
    for (uint32_t i = 0; i < key_count; ++i) {
      groups_buffer[off + i] = key[i];
    }
  }
  return groups_buffer + off + key_count;
}

/**
 * For a particular hashed index (only used with multi-column perfect hash group by)
 * it returns the row-wise offset of the group in the output buffer.
 * Since it is intended for keyless hash use, it assumes there is no group columns
 * prepending the output buffer.
 */
extern "C" ALWAYS_INLINE int64_t* get_matching_group_value_perfect_hash_keyless(
    int64_t* groups_buffer,
    const uint32_t hashed_index,
    const uint32_t row_size_quad) {
  return groups_buffer + row_size_quad * hashed_index;
}

/*
 * For a particular hashed_index, find and initialize (if necessary) all the group
 * columns corresponding to a key. It is assumed that all group columns are 64-bit wide.
 */
extern "C" ALWAYS_INLINE void set_matching_group_value_perfect_hash_columnar(
    int64_t* groups_buffer,
    const uint32_t hashed_index,
    const int64_t* key,
    const uint32_t key_count,
    const uint32_t entry_count) {
  if (groups_buffer[hashed_index] == empty_key_64) {
    for (uint32_t i = 0; i < key_count; i++) {
      groups_buffer[i * entry_count + hashed_index] = key[i];
    }
  }
}

#include "exec/template/operator/join/hashtable/runtime/JoinHashTableQueryRuntime.cpp"
#include "function/aggregate/GroupByRuntime.cpp"

extern "C" ALWAYS_INLINE int64_t* get_group_value_fast_keyless(
    int64_t* groups_buffer,
    const int64_t key,
    const int64_t min_key,
    const int64_t /* bucket */,
    const uint32_t row_size_quad) {
  return groups_buffer + row_size_quad * (key - min_key);
}

extern "C" ALWAYS_INLINE int64_t* get_group_value_fast_keyless_semiprivate(
    int64_t* groups_buffer,
    const int64_t key,
    const int64_t min_key,
    const int64_t /* bucket */,
    const uint32_t row_size_quad,
    const uint8_t thread_warp_idx,
    const uint8_t warp_size) {
  return groups_buffer + row_size_quad * (warp_size * (key - min_key) + thread_warp_idx);
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
#include "function/string/CiderStringFunctions.cpp"
#include "function/string/StringLike.cpp"
#endif

#include "function/scalar/TopKRuntime.cpp"

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

extern "C" ALWAYS_INLINE bool check_bit_vector_set(uint8_t* bit_vector, uint64_t index) {
  return CiderBitUtils::isBitSetAt(bit_vector, index);
}

extern "C" ALWAYS_INLINE bool check_bit_vector_clear(uint8_t* bit_vector,
                                                     uint64_t index) {
  bool ans = !CiderBitUtils::isBitSetAt(bit_vector, index);
  return ans;
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

extern "C" ALWAYS_INLINE void do_memcpy(int8_t* dst, int8_t* src, int32_t len) {
  memcpy(dst, src, len);
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

#include "exec/nextgen/context/ContextRuntimeFunctions.h"
#include "exec/nextgen/operators/OperatorRuntimeFunctions.h"
#include "function/aggregate/CiderRuntimeFunctions.h"
#include "function/datetime/CiderDateFunctions.cpp"
