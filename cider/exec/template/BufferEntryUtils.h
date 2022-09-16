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
#ifndef QUERYENGINE_BUFFERENTRYUTILS_H
#define QUERYENGINE_BUFFERENTRYUTILS_H

#include "type/data/funcannotations.h"

#ifdef FORCE_CPU_VERSION
#undef INLINE
#define INLINE inline
#else
#define INLINE
#endif

namespace {

template <class K>
INLINE bool is_empty_entry(const size_t entry_idx,
                           const int8_t* groupby_buffer,
                           const size_t key_stride);

template <>
INLINE bool is_empty_entry<int32_t>(const size_t entry_idx,
                                    const int8_t* groupby_buffer,
                                    const size_t key_stride) {
  const auto key_ptr = groupby_buffer + entry_idx * key_stride;
  return (*reinterpret_cast<const int32_t*>(key_ptr) == EMPTY_KEY_32);
}

template <>
INLINE bool is_empty_entry<int64_t>(const size_t entry_idx,
                                    const int8_t* groupby_buffer,
                                    const size_t key_stride) {
  const auto key_ptr = groupby_buffer + entry_idx * key_stride;
  return (*reinterpret_cast<const int64_t*>(key_ptr) == EMPTY_KEY_64);
}

}  // namespace

#undef INLINE
#endif  // QUERYENGINE_BUFFERENTRYUTILS_H
