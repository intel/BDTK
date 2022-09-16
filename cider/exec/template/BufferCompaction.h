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
#ifndef BUFFER_COMPACTION_H
#define BUFFER_COMPACTION_H

#include <cstdint>
#include "type/data/funcannotations.h"

#include <algorithm>

constexpr int8_t MAX_BYTE_WIDTH_SUPPORTED = 8;

inline unsigned compact_byte_width(unsigned qw, unsigned low_bound) {
  return std::max(qw, low_bound);
}

template <typename T>
FORCE_INLINE T align_to_int64(T addr) {
  addr += sizeof(int64_t) - 1;
  return (T)(((uint64_t)addr >> 3) << 3);
}

#endif /* BUFFER_COMPACTION_H */
