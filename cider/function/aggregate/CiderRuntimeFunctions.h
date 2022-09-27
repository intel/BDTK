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

#ifndef CIDER_FUNCTION_RUNTIME_FUNCTIONS_H
#define CIDER_FUNCTION_RUNTIME_FUNCTIONS_H

#include <cmath>
#include <cstdint>

#include "exec/template/TypePunning.h"
#include "type/data/funcannotations.h"
#include "util/CiderBitUtils.h"

/********************* Simple Aggregation Functions *****************************/
#define DEF_CIDER_SIMPLE_AGG_INT(width, aggname, aggfunc)                               \
  extern "C" ALWAYS_INLINE void cider_agg_##aggname##_int##width(                       \
      int##width##_t* agg_val_buffer, const uint64_t index, const int##width##_t val) { \
    aggfunc(*(agg_val_buffer + index), val);                                            \
  }

#define DEF_CIDER_SIMPLE_AGG_INT_NULLABLE(width, aggname, aggfunc)           \
  extern "C" ALWAYS_INLINE void cider_agg_##aggname##_int##width##_nullable( \
      int##width##_t* agg_val_buffer,                                        \
      const uint64_t index,                                                  \
      const int##width##_t val,                                              \
      uint8_t* agg_null_buffer,                                              \
      bool is_null) {                                                        \
    if (!is_null) {                                                          \
      aggfunc(*(agg_val_buffer + index), val);                               \
      CiderBitUtils::setBitAt(agg_null_buffer, index);                       \
    }                                                                        \
  }

#define DEF_CIDER_SIMPLE_AGG_FP(fpType, fpName, width, aggname, aggfunc)        \
  extern "C" ALWAYS_INLINE void cider_agg_##aggname##_##fpName(                 \
      int##width##_t* agg_val_buffer, const uint64_t index, const fpType val) { \
    fpType* agg_val_alias =                                                     \
        reinterpret_cast<fpType*>(may_alias_ptr(agg_val_buffer + index));       \
    aggfunc(*agg_val_alias, val);                                               \
  }

#define DEF_CIDER_SIMPLE_AGG_FP_NULLABLE(fpType, fpName, width, aggname, aggfunc) \
  extern "C" ALWAYS_INLINE void cider_agg_##aggname##_##fpName##_nullable(        \
      int##width##_t* agg_val_buffer,                                             \
      const uint64_t index,                                                       \
      const fpType val,                                                           \
      uint8_t* agg_null_buffer,                                                   \
      bool is_null) {                                                             \
    if (!is_null) {                                                               \
      fpType* agg_val_alias =                                                     \
          reinterpret_cast<fpType*>(may_alias_ptr(agg_val_buffer + index));       \
      aggfunc(*agg_val_alias, val);                                               \
      CiderBitUtils::setBitAt(agg_null_buffer, index);                            \
    }                                                                             \
  }

#define DEF_CIDER_SIMPLE_AGG_FUNCS(aggName, aggFunc)                       \
  DEF_CIDER_SIMPLE_AGG_INT(32, aggName, aggFunc)                           \
  DEF_CIDER_SIMPLE_AGG_INT(64, aggName, aggFunc)                           \
  DEF_CIDER_SIMPLE_AGG_INT_NULLABLE(32, aggName, aggFunc)                  \
  DEF_CIDER_SIMPLE_AGG_INT_NULLABLE(64, aggName, aggFunc)                  \
  DEF_CIDER_SIMPLE_AGG_FP(float, float, 32, aggName, aggFunc)              \
  DEF_CIDER_SIMPLE_AGG_FP(float, floatSpec, 64, aggName, aggFunc)          \
  DEF_CIDER_SIMPLE_AGG_FP(double, double, 64, aggName, aggFunc)            \
  DEF_CIDER_SIMPLE_AGG_FP_NULLABLE(float, float, 32, aggName, aggFunc)     \
  DEF_CIDER_SIMPLE_AGG_FP_NULLABLE(float, floatSpec, 64, aggName, aggFunc) \
  DEF_CIDER_SIMPLE_AGG_FP_NULLABLE(double, double, 64, aggName, aggFunc)

template <typename T>
ALWAYS_INLINE void cider_agg_min(T& agg_val, const T& val) {
  agg_val = std::min(agg_val, val);
}
DEF_CIDER_SIMPLE_AGG_FUNCS(min, cider_agg_min)

template <typename T>
ALWAYS_INLINE void cider_agg_max(T& agg_val, const T& val) {
  agg_val = std::max(agg_val, val);
}
DEF_CIDER_SIMPLE_AGG_FUNCS(max, cider_agg_max)

template <typename T>
ALWAYS_INLINE void cider_agg_sum(T& agg_val, const T& val) {
  agg_val += val;
}
DEF_CIDER_SIMPLE_AGG_FUNCS(sum, cider_agg_sum)

template <typename T>
ALWAYS_INLINE void cider_agg_id(T& agg_val, const T& val) {
  agg_val = val;
}
DEF_CIDER_SIMPLE_AGG_FUNCS(id, cider_agg_id)
/********************************************************************************/

/********************** Project Id Functions *****************************/

#define DEF_CIDER_ID_PROJ_INT(width)                                                    \
  extern "C" ALWAYS_INLINE void cider_agg_id_proj_int##width(                           \
      int##width##_t* agg_val_buffer, const uint64_t index, const int##width##_t val) { \
    cider_agg_id(*(agg_val_buffer + index), val);                                       \
  }

#define DEF_CIDER_ID_PROJ_INT_NULLABLE(width)                            \
  extern "C" ALWAYS_INLINE void cider_agg_id_proj_int##width##_nullable( \
      int##width##_t* agg_val_buffer,                                    \
      const uint64_t index,                                              \
      const int##width##_t val,                                          \
      uint8_t* agg_null_buffer,                                          \
      bool is_null) {                                                    \
    if (is_null) {                                                       \
      CiderBitUtils::clearBitAt(agg_null_buffer, index);                 \
    } else {                                                             \
      cider_agg_id(*(agg_val_buffer + index), val);                      \
    }                                                                    \
  }

#define DEF_CIDER_ID_PROJ_FP(fpType, fpName, width)                             \
  extern "C" ALWAYS_INLINE void cider_agg_id_proj_##fpName(                     \
      int##width##_t* agg_val_buffer, const uint64_t index, const fpType val) { \
    fpType* agg_val_alias =                                                     \
        reinterpret_cast<fpType*>(may_alias_ptr(agg_val_buffer + index));       \
    cider_agg_id(*agg_val_alias, val);                                          \
  }

#define DEF_CIDER_ID_PROJ_FP_NULLABLE(fpType, fpName, width)                \
  extern "C" ALWAYS_INLINE void cider_agg_id_proj_##fpName##_nullable(      \
      int##width##_t* agg_val_buffer,                                       \
      const uint64_t index,                                                 \
      const fpType val,                                                     \
      uint8_t* agg_null_buffer,                                             \
      bool is_null) {                                                       \
    if (is_null) {                                                          \
      CiderBitUtils::clearBitAt(agg_null_buffer, index);                    \
    } else {                                                                \
      fpType* agg_val_alias =                                               \
          reinterpret_cast<fpType*>(may_alias_ptr(agg_val_buffer + index)); \
      cider_agg_id(*agg_val_alias, val);                                    \
    }                                                                       \
  }

DEF_CIDER_ID_PROJ_INT(8)
DEF_CIDER_ID_PROJ_INT(16)
DEF_CIDER_ID_PROJ_INT(32)
DEF_CIDER_ID_PROJ_INT(64)
DEF_CIDER_ID_PROJ_INT_NULLABLE(8)
DEF_CIDER_ID_PROJ_INT_NULLABLE(16)
DEF_CIDER_ID_PROJ_INT_NULLABLE(32)
DEF_CIDER_ID_PROJ_INT_NULLABLE(64)
DEF_CIDER_ID_PROJ_FP(float, float, 32)
DEF_CIDER_ID_PROJ_FP(double, double, 64)
DEF_CIDER_ID_PROJ_FP_NULLABLE(float, float, 32)
DEF_CIDER_ID_PROJ_FP_NULLABLE(double, double, 64)

/********************************************************************************/

/********************** Count Aggregation Functions *****************************/
#define DEF_CIDER_COUNT_AGG(width)                            \
  extern "C" ALWAYS_INLINE void cider_agg_count_int##width(   \
      int##width##_t* agg_val_buffer, const uint64_t index) { \
    ++(*(agg_val_buffer + index));                            \
  }

#define DEF_CIDER_COUNT_AGG_NULLABLE(width)                                 \
  extern "C" ALWAYS_INLINE void cider_agg_count_int##width##_nullable(      \
      int##width##_t* agg_val_buffer, const uint64_t index, bool is_null) { \
    if (!is_null) {                                                         \
      ++(*(agg_val_buffer + index));                                        \
    }                                                                       \
  }

DEF_CIDER_COUNT_AGG(32)
DEF_CIDER_COUNT_AGG(64)
DEF_CIDER_COUNT_AGG_NULLABLE(32)
DEF_CIDER_COUNT_AGG_NULLABLE(64)
/********************************************************************************/

#endif  // CIDER_FUNCTION_RUNTIME_FUNCTIONS_H
