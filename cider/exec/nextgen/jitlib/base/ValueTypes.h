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
#ifndef JITLIB_VALUE_TYPES_H
#define JITLIB_VALUE_TYPES_H

#include <cstdint>

namespace jitlib {
enum TypeTag { INVALID, VOID, INT8, INT16, INT32, INT64 };

template <TypeTag>
struct TypeTraits {
  using NativeType = int8_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr TypeTag tag = INVALID;
  static constexpr const char* name = "INVALID";
};

template <>
struct TypeTraits<VOID> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr TypeTag tag = VOID;
  static constexpr const char* name = "VOID";
};

template <>
struct TypeTraits<INT8> {
  using NativeType = int8_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr TypeTag tag = INT8;
  static constexpr const char* name = "INT8";
};

template <>
struct TypeTraits<INT16> {
  using NativeType = int16_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr TypeTag tag = INT16;
  static constexpr const char* name = "INT16";
};

template <>
struct TypeTraits<INT32> {
  using NativeType = int32_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr TypeTag tag = INT32;
  static constexpr const char* name = "INT32";
};

template <>
struct TypeTraits<INT64> {
  using NativeType = int64_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr TypeTag tag = INT64;
  static constexpr const char* name = "INT64";
};

};  // namespace jitlib

#endif
