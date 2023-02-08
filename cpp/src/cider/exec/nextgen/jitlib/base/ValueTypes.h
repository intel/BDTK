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
#ifndef JITLIB_BASE_VALUETYPES_H
#define JITLIB_BASE_VALUETYPES_H

#include <any>

#include "util/Logger.h"

namespace cider::jitlib {
enum class JITTypeTag {
  INVALID,
  VOID,
  BOOL,
  INT8,
  INT16,
  INT32,
  INT64,
  INT128,
  FLOAT,
  DOUBLE,
  POINTER,
  VARCHAR,
  TUPLE,  // Logical struct
  STRUCT  // Physical struct
};

template <JITTypeTag>
struct JITTypeTraits {
  using NativeType = int8_t;
  static constexpr bool is_fixed_width = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INVALID;
  static constexpr const char* name = "INVALID";
};

template <>
struct JITTypeTraits<JITTypeTag::VOID> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = JITTypeTag::VOID;
  static constexpr const char* name = "VOID";
};

template <>
struct JITTypeTraits<JITTypeTag::BOOL> {
  using NativeType = bool;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::BOOL;
  static constexpr const char* name = "BOOL";
};

template <>
struct JITTypeTraits<JITTypeTag::INT8> {
  using NativeType = int8_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INT8;
  static constexpr const char* name = "INT8";
};

template <>
struct JITTypeTraits<JITTypeTag::INT16> {
  using NativeType = int16_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INT16;
  static constexpr const char* name = "INT16";
};

template <>
struct JITTypeTraits<JITTypeTag::INT32> {
  using NativeType = int32_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INT32;
  static constexpr const char* name = "INT32";
};

template <>
struct JITTypeTraits<JITTypeTag::INT64> {
  using NativeType = int64_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INT64;
  static constexpr const char* name = "INT64";
};

template <>
struct JITTypeTraits<JITTypeTag::INT128> {
  using NativeType = __int128_t;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::INT128;
  static constexpr const char* name = "INT128";
};

template <>
struct JITTypeTraits<JITTypeTag::FLOAT> {
  using NativeType = float;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::FLOAT;
  static constexpr const char* name = "FLOAT";
};

template <>
struct JITTypeTraits<JITTypeTag::DOUBLE> {
  using NativeType = double;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::DOUBLE;
  static constexpr const char* name = "DOUBLE";
};

template <>
struct JITTypeTraits<JITTypeTag::POINTER> {
  using NativeType = void*;
  static constexpr bool isFixedWidth = true;
  static constexpr uint64_t width = sizeof(NativeType);
  static constexpr uint64_t bits = sizeof(NativeType) * 8;
  static constexpr JITTypeTag tag = JITTypeTag::POINTER;
  static constexpr const char* name = "POINTER";
};

template <>
struct JITTypeTraits<JITTypeTag::TUPLE> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = JITTypeTag::TUPLE;
  static constexpr const char* name = "TUPLE";
};

template <>
struct JITTypeTraits<JITTypeTag::STRUCT> {
  using NativeType = void;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = JITTypeTag::STRUCT;
  static constexpr const char* name = "STRUCT";
};

template <>
struct JITTypeTraits<JITTypeTag::VARCHAR> {
  using NativeType = std::string;
  static constexpr bool isFixedWidth = false;
  static constexpr JITTypeTag tag = JITTypeTag::VARCHAR;
  static constexpr const char* name = "VARCHAR";
};

inline const char* getJITTypeName(JITTypeTag type_tag) {
  switch (type_tag) {
    case JITTypeTag::BOOL:
      return JITTypeTraits<JITTypeTag::BOOL>::name;
    case JITTypeTag::INT8:
      return JITTypeTraits<JITTypeTag::INT8>::name;
    case JITTypeTag::INT16:
      return JITTypeTraits<JITTypeTag::INT16>::name;
    case JITTypeTag::INT32:
      return JITTypeTraits<JITTypeTag::INT32>::name;
    case JITTypeTag::INT64:
      return JITTypeTraits<JITTypeTag::INT64>::name;
    case JITTypeTag::FLOAT:
      return JITTypeTraits<JITTypeTag::FLOAT>::name;
    case JITTypeTag::DOUBLE:
      return JITTypeTraits<JITTypeTag::DOUBLE>::name;
    case JITTypeTag::POINTER:
      return JITTypeTraits<JITTypeTag::POINTER>::name;
    case JITTypeTag::VARCHAR:
      return JITTypeTraits<JITTypeTag::VARCHAR>::name;
    case JITTypeTag::TUPLE:
      return JITTypeTraits<JITTypeTag::TUPLE>::name;
    case JITTypeTag::STRUCT:
      return JITTypeTraits<JITTypeTag::STRUCT>::name;
    case JITTypeTag::INT128:
      return JITTypeTraits<JITTypeTag::INT128>::name;
    default:
      LOG(ERROR) << "Invalid JITType in getJITTypeName";
  }
  return 0;
}

inline uint64_t getJITTypeSize(JITTypeTag type_tag) {
  switch (type_tag) {
    case JITTypeTag::BOOL:
      return JITTypeTraits<JITTypeTag::BOOL>::width;
    case JITTypeTag::INT8:
      return JITTypeTraits<JITTypeTag::INT8>::width;
    case JITTypeTag::INT16:
      return JITTypeTraits<JITTypeTag::INT16>::width;
    case JITTypeTag::INT32:
      return JITTypeTraits<JITTypeTag::INT32>::width;
    case JITTypeTag::INT64:
      return JITTypeTraits<JITTypeTag::INT64>::width;
    case JITTypeTag::INT128:
      return JITTypeTraits<JITTypeTag::INT128>::width;
    case JITTypeTag::FLOAT:
      return JITTypeTraits<JITTypeTag::FLOAT>::width;
    case JITTypeTag::DOUBLE:
      return JITTypeTraits<JITTypeTag::DOUBLE>::width;
    case JITTypeTag::POINTER:
      return JITTypeTraits<JITTypeTag::POINTER>::width;
    default:
      LOG(ERROR) << "Invalid JITType in getJITTypeSize: " << getJITTypeName(type_tag);
  }
  return 0;
}

template <typename T>
inline std::any castLiteral(JITTypeTag target_type, T value) {
  switch (target_type) {
    case JITTypeTag::BOOL:
      return static_cast<JITTypeTraits<JITTypeTag::BOOL>::NativeType>(value);
    case JITTypeTag::INT8:
      return static_cast<JITTypeTraits<JITTypeTag::INT8>::NativeType>(value);
    case JITTypeTag::INT16:
      return static_cast<JITTypeTraits<JITTypeTag::INT16>::NativeType>(value);
    case JITTypeTag::INT32:
      return static_cast<JITTypeTraits<JITTypeTag::INT32>::NativeType>(value);
    case JITTypeTag::INT64:
    case JITTypeTag::POINTER:
      return static_cast<JITTypeTraits<JITTypeTag::INT64>::NativeType>(value);
    case JITTypeTag::INT128:
      return static_cast<JITTypeTraits<JITTypeTag::INT128>::NativeType>(value);
    case JITTypeTag::FLOAT:
      return static_cast<JITTypeTraits<JITTypeTag::FLOAT>::NativeType>(value);
    case JITTypeTag::DOUBLE:
      return static_cast<JITTypeTraits<JITTypeTag::DOUBLE>::NativeType>(value);
    default:
      LOG(ERROR) << "Invalid JITType in castLiteral: " << getJITTypeName(target_type);
  }
  return -1;
}
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_VALUETYPES_H
