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

#ifndef CIDER_STRING_FUNCTIONS_H
#define CIDER_STRING_FUNCTIONS_H

#include "type/data/funcannotations.h"

#include <cstdint>

ALWAYS_INLINE uint64_t pack_string(const int8_t* ptr, const int32_t len);

extern "C" RUNTIME_EXPORT int64_t cider_substring(const char* str, int pos, int len);
extern "C" RUNTIME_EXPORT int64_t cider_substring_extra(char* string_heap_ptr,
                                                        const char* str,
                                                        int pos,
                                                        int len);

extern "C" RUNTIME_EXPORT int32_t format_substring_pos(int pos, int str_len);
extern "C" RUNTIME_EXPORT int32_t format_substring_len(int pos,
                                                       int str_len,
                                                       int target_len);
#endif