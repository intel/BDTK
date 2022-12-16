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

#include "CiderStringFunctions.h"
#include "exec/nextgen/context/StringHeap.h"

ALWAYS_INLINE uint64_t pack_string(const int8_t* ptr, const int32_t len) {
  return (reinterpret_cast<const uint64_t>(ptr) & 0xffffffffffff) |
         (static_cast<const uint64_t>(len) << 48);
}

extern "C" RUNTIME_EXPORT int64_t cider_substring(const char* str,
                                                  int str_len,
                                                  int start,
                                                  int len) {
  const char* ret_ptr = str + start;
  int64_t ret = (reinterpret_cast<const uint64_t>(ret_ptr) & 0xffffffffffff) |
                (static_cast<const uint64_t>(len) << 48);
  return ret;
}

extern "C" RUNTIME_EXPORT int64_t cider_substring_extra(char* string_heap_ptr,
                                                        const char* str,
                                                        int str_len,
                                                        int start,
                                                        int len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str + start, len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

