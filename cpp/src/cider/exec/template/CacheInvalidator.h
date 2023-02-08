/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#ifndef CACHEINVALIDATOR_H
#define CACHEINVALIDATOR_H

template <typename... CACHE_HOLDING_TYPES>
class CacheInvalidator {
 public:
  static void invalidateCaches() { internalInvalidateCache<CACHE_HOLDING_TYPES...>(); }

 private:
  CacheInvalidator() = delete;
  ~CacheInvalidator() = delete;

  template <typename CACHE_HOLDING_TYPE>
  static void internalInvalidateCache() {
    CACHE_HOLDING_TYPE::getCacheInvalidator()();
  }

  template <typename FIRST_CACHE_HOLDING_TYPE,
            typename SECOND_CACHE_HOLDING_TYPE,
            typename... REMAINING_CACHE_HOLDING_TYPES>
  static void internalInvalidateCache() {
    FIRST_CACHE_HOLDING_TYPE::getCacheInvalidator()();
    internalInvalidateCache<SECOND_CACHE_HOLDING_TYPE,
                            REMAINING_CACHE_HOLDING_TYPES...>();
  }
};

#endif
