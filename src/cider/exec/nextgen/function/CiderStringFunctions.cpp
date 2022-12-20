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

#include "exec/nextgen/context/StringHeap.h"

ALWAYS_INLINE uint64_t pack_string(const int8_t* ptr, const int32_t len) {
  return (reinterpret_cast<const uint64_t>(ptr) & 0xffffffffffff) |
         (static_cast<const uint64_t>(len) << 48);
}

// not in use.
extern "C" RUNTIME_EXPORT int64_t cider_substring(const char* str, int pos, int len) {
  const char* ret_ptr = str + pos - 1;
  return pack_string((const int8_t*)ret_ptr, (const int32_t)len);
}

// pos parameter starts from 1 rather than 0
extern "C" RUNTIME_EXPORT int64_t cider_substring_extra(char* string_heap_ptr,
                                                        const char* str,
                                                        int pos,
                                                        int len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str + pos - 1, len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

// pos starts with 1. A negative starting position is interpreted as being relative
// to the end of the string
extern "C" RUNTIME_EXPORT int32_t format_substring_pos(int pos, int str_len) {
  int32_t ret = 1;
  if (pos > 0) {
    if (pos > str_len) {
      ret = str_len + 1;
    } else {
      ret = pos;
    }
  } else if (pos < 0) {
    if (pos + str_len >= 0) {
      ret = str_len + pos + 1;
    }
  }
  return ret;
}

// pos should be [1, str_len+1]
extern "C" RUNTIME_EXPORT int32_t format_substring_len(int pos,
                                                       int str_len,
                                                       int target_len) {
  // already out of range, return empty string.
  if (pos == str_len + 1) {
    return 0;
  }
  // not reach to max str length, return target length
  if (pos + target_len <= str_len + 1) {
    return target_len;
  } else {
    // reach to max str length
    return str_len - pos + 1;
  }
}
