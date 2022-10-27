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
#ifndef JITLIB_BASE_VALUETYPES_H
#define JITLIB_BASE_VALUETYPES_H

#include <cstdint>

#include "util/Logger.h"

namespace jitlib {
enum class JITBackendTag { LLVMJIT };
enum JITTypeTag {
  INVALID,
  VOID,
  BOOL,
  INT8,
  INT16,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,
  POINTER,
  TUPLE,  // Logical struct
  STRUCT  // Physical struct
};

template <JITTypeTag>
struct JITTypeTraits {
  using NativeType = int8_t;
  static constexpr bool is_fixed_width = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = INVALID;
  static constexpr const char* name = "INVALID";
};

template <>
struct JITTypeTraits<VOID> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = VOID;
  static constexpr const char* name = "VOID";
};

template <>
struct JITTypeTraits<BOOL> {
  using NativeType = bool;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = BOOL;
  static constexpr const char* name = "BOOL";
};

template <>
struct JITTypeTraits<INT8> {
  using NativeType = int8_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = INT8;
  static constexpr const char* name = "INT8";
};

template <>
struct JITTypeTraits<INT16> {
  using NativeType = int16_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = INT16;
  static constexpr const char* name = "INT16";
};

template <>
struct JITTypeTraits<INT32> {
  using NativeType = int32_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = INT32;
  static constexpr const char* name = "INT32";
};

template <>
struct JITTypeTraits<INT64> {
  using NativeType = int64_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = INT64;
  static constexpr const char* name = "INT64";
};

template <>
struct JITTypeTraits<FLOAT> {
  using NativeType = float;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = FLOAT;
  static constexpr const char* name = "FLOAT";
};

template <>
struct JITTypeTraits<DOUBLE> {
  using NativeType = double;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = DOUBLE;
  static constexpr const char* name = "DOUBLE";
};

template <>
struct JITTypeTraits<POINTER> {
  using NativeType = void*;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = POINTER;
  static constexpr const char* name = "POINTER";
};

template <>
struct JITTypeTraits<TUPLE> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = TUPLE;
  static constexpr const char* name = "TUPLE";
};

template <>
struct JITTypeTraits<STRUCT> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = STRUCT;
  static constexpr const char* name = "STRUCT";
};

inline uint64_t getJITTypeSize(JITTypeTag type_tag) {
  switch (type_tag) {
    case BOOL:
      return JITTypeTraits<BOOL>::width;
    case INT8:
      return JITTypeTraits<INT8>::width;
    case INT16:
      return JITTypeTraits<INT16>::width;
    case INT32:
      return JITTypeTraits<INT32>::width;
    case INT64:
      return JITTypeTraits<INT64>::width;
    case FLOAT:
      return JITTypeTraits<FLOAT>::width;
    case DOUBLE:
      return JITTypeTraits<DOUBLE>::width;
    case POINTER:
      return JITTypeTraits<POINTER>::width;
    default:
      LOG(ERROR) << "Invalid JITType in getJITTypeSize: " << type_tag;
  }
  return 0;
}

inline const char* getJITTypeName(JITTypeTag type_tag) {
  switch (type_tag) {
    case BOOL:
      return JITTypeTraits<BOOL>::name;
    case INT8:
      return JITTypeTraits<INT8>::name;
    case INT16:
      return JITTypeTraits<INT16>::name;
    case INT32:
      return JITTypeTraits<INT32>::name;
    case INT64:
      return JITTypeTraits<INT64>::name;
    case FLOAT:
      return JITTypeTraits<FLOAT>::name;
    case DOUBLE:
      return JITTypeTraits<DOUBLE>::name;
    case POINTER:
      return JITTypeTraits<POINTER>::name;
    case TUPLE:
      return JITTypeTraits<TUPLE>::name;
    case STRUCT:
      return JITTypeTraits<STRUCT>::name;
    default:
      LOG(ERROR) << "Invalid JITType in getJITTypeName: " << type_tag;
  }
  return 0;
}

};  // namespace jitlib

#endif  // JITLIB_BASE_VALUETYPES_H
