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

#ifndef CHECKED_ALLOC_H
#define CHECKED_ALLOC_H

#define BOOST_STACKTRACE_GNU_SOURCE_NOT_REQUIRED 1

#include "util/boost_stacktrace.hpp"

#include <cstdlib>
#include <ostream>
#include <stdexcept>
#include <string>
#include "cider/CiderException.h"
#include "util/Logger.h"

inline void* checked_malloc(const size_t size) {
  auto ptr = malloc(size);
  if (!ptr) {
    CIDER_THROW(
        CiderOutOfMemoryException,
        "Not enough CPU memory available to allocate " + std::to_string(size) + " bytes");
  }
  return ptr;
}

inline void* checked_calloc(const size_t nmemb, const size_t size) {
  auto ptr = calloc(nmemb, size);
  if (!ptr) {
    CIDER_THROW(CiderOutOfMemoryException,
                "Not enough CPU memory available to allocate " +
                    std::to_string(nmemb * size) + " bytes");
  }
  return ptr;
}

struct CheckedAllocDeleter {
  void operator()(void* p) { free(p); }
};

#endif  // CHECKED_ALLOC_H
