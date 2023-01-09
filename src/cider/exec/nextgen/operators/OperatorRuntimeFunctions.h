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

#ifndef NEXTEGN_CIDER_FUNCTION_RUNTIME_FUNCTIONS_H
#define NEXTEGN_CIDER_FUNCTION_RUNTIME_FUNCTIONS_H

#include "exec/nextgen/context/RuntimeContext.h"
#include "type/data/funcannotations.h"

/******************* Simple Aggregation Functions For Nextgen ************************/
#define DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT(width, aggname, aggfunc)         \
  extern "C" ALWAYS_INLINE void nextgen_cider_agg_##aggname##_int##width( \
      int##width##_t* agg_val_addr, const int##width##_t val) {           \
    aggfunc(*agg_val_addr, val);                                          \
  }

#define DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_WITH_OFFSET(width, aggname, aggfunc)           \
  extern "C" ALWAYS_INLINE void nextgen_cider_agg_with_offset_##aggname##_int##width(   \
      int##width##_t* agg_val_buffer, const uint64_t index, const int##width##_t val) { \
    aggfunc(*(agg_val_buffer + index), val);                                            \
  }

#define DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_NULLABLE(width, aggname, aggfunc)           \
  extern "C" ALWAYS_INLINE void nextgen_cider_agg_##aggname##_int##width##_nullable( \
      int##width##_t* agg_val_addr,                                                  \
      const int##width##_t val,                                                      \
      uint8_t* agg_null_addr,                                                        \
      bool is_null) {                                                                \
    if (!is_null) {                                                                  \
      aggfunc(*agg_val_addr, val);                                                   \
      *agg_null_addr = 0;                                                            \
    }                                                                                \
  }

#define DEF_NEXTEGN_CIDER_SIMPLE_AGG_FUNCS(aggName, aggFunc)         \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT(8, aggName, aggFunc)              \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT(16, aggName, aggFunc)             \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT(32, aggName, aggFunc)             \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT(64, aggName, aggFunc)             \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_WITH_OFFSET(8, aggName, aggFunc)  \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_WITH_OFFSET(16, aggName, aggFunc) \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_WITH_OFFSET(32, aggName, aggFunc) \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_WITH_OFFSET(64, aggName, aggFunc) \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_NULLABLE(8, aggName, aggFunc)     \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_NULLABLE(16, aggName, aggFunc)    \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_NULLABLE(32, aggName, aggFunc)    \
  DEF_NEXTEGN_CIDER_SIMPLE_AGG_INT_NULLABLE(64, aggName, aggFunc)

template <typename T>
ALWAYS_INLINE void nextgen_cider_agg_sum(T& agg_val, const T& val) {
  agg_val += val;
}
DEF_NEXTEGN_CIDER_SIMPLE_AGG_FUNCS(sum, nextgen_cider_agg_sum)

template <typename T>
ALWAYS_INLINE void nextgen_cider_agg_min(T& agg_val, const T& val) {
  agg_val = std::min(agg_val, val);
}
DEF_NEXTEGN_CIDER_SIMPLE_AGG_FUNCS(min, nextgen_cider_agg_min)

template <typename T>
ALWAYS_INLINE void nextgen_cider_agg_max(T& agg_val, const T& val) {
  agg_val = std::max(agg_val, val);
}
DEF_NEXTEGN_CIDER_SIMPLE_AGG_FUNCS(max, nextgen_cider_agg_max)

/******************* Simple Aggregation COUNT For Nextgen ************************/
extern "C" ALWAYS_INLINE void nextgen_cider_agg_count(int64_t* agg_val_addr) {
  ++(*agg_val_addr);
}

extern "C" ALWAYS_INLINE void nextgen_cider_agg_count_nullable(int64_t* agg_val_addr,
                                                               uint8_t* agg_null_addr,
                                                               bool is_null) {
  if (!is_null) {
    ++(*agg_val_addr);
    *agg_null_addr = 0;
  }
}

#endif  // NEXTEGN_CIDER_FUNCTION_RUNTIME_FUNCTIONS_H
