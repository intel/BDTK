/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "exec/nextgen/context/CiderSet.h"

#define DEF_CIDER_INT64_SET_CONTAINS(type)                                        \
  extern "C" RUNTIME_FUNC ALLOW_INLINE bool cider_set_contains_##type##_val(      \
      int8_t* set_ptr, const type val) {                                          \
    auto cider_set =                                                              \
        reinterpret_cast<cider::exec::nextgen::context::CiderInt64Set*>(set_ptr); \
    return cider_set->contains(val);                                              \
  }

DEF_CIDER_INT64_SET_CONTAINS(int8_t)
DEF_CIDER_INT64_SET_CONTAINS(int16_t)
DEF_CIDER_INT64_SET_CONTAINS(int32_t)
DEF_CIDER_INT64_SET_CONTAINS(int64_t)

#define DEF_CIDER_DOUBLE_SET_CONTAINS(type)                                        \
  extern "C" RUNTIME_FUNC ALLOW_INLINE bool cider_set_contains_##type##_val(       \
      int8_t* set_ptr, const type val) {                                           \
    auto cider_set =                                                               \
        reinterpret_cast<cider::exec::nextgen::context::CiderDoubleSet*>(set_ptr); \
    return cider_set->contains(val);                                               \
  }

DEF_CIDER_DOUBLE_SET_CONTAINS(float)
DEF_CIDER_DOUBLE_SET_CONTAINS(double)

extern "C" RUNTIME_FUNC ALLOW_INLINE bool cider_set_contains_string_val(int8_t* set_ptr,
                                                                        const char* str,
                                                                        int len) {
  auto cider_set =
      reinterpret_cast<cider::exec::nextgen::context::CiderStringSet*>(set_ptr);
  std::string val_str(str, len);
  return cider_set->contains(val_str);
}
