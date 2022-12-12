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
#ifndef JITLIB_BASE_JITVALUEOPERATIONS_H
#define JITLIB_BASE_JITVALUEOPERATIONS_H

#include <type_traits>

#include "exec/nextgen/jitlib/base/JITFunction.h"

namespace cider::jitlib {
template <typename T>
struct is_jitvalue_convertable {
  using NativeType = typename std::remove_reference<T>::type;
  static constexpr bool v =
      std::is_arithmetic_v<NativeType> || std::is_same_v<NativeType, bool>;
};
template <typename T>
inline constexpr bool is_jitvalue_convertable_v = is_jitvalue_convertable<T>::v;

template <typename T>
using IsJITValueConvertable =
    typename std::enable_if_t<is_jitvalue_convertable_v<T>, bool>;

inline JITValuePointer operator&&(JITValue& lh, JITValue& rh) {
  return lh.andOp(rh);
}

// disable pointer to bool implicit cast
template <class T, std::enable_if_t<std::is_same_v<T, bool>, bool> = true>
inline JITValuePointer operator&&(JITValue& lh, T rh) {
  if (rh) {
    return &lh;
  }
  auto& func = lh.getParentJITFunction();
  return func.createLiteral(JITTypeTag::BOOL, false);
}

// disable pointer to bool implicit cast
template <class T, std::enable_if_t<std::is_same_v<T, bool>, bool> = true>
inline JITValuePointer operator&&(T lh, JITValue& rh) {
  return rh && lh;
}

inline JITValuePointer operator||(JITValue& lh, JITValue& rh) {
  return lh.orOp(rh);
}

// disable pointer to bool implicit cast
template <class T, std::enable_if_t<std::is_same_v<T, bool>, bool> = true>
inline JITValuePointer operator||(JITValue& lh, T rh) {
  if (!rh) {
    return &lh;
  }
  auto& func = lh.getParentJITFunction();
  return func.createLiteral(JITTypeTag::BOOL, true);
}

// disable pointer to bool implicit cast
template <class T, std::enable_if_t<std::is_same_v<T, bool>, bool> = true>
inline JITValuePointer operator||(T lh, JITValue& rh) {
  return rh || lh;
}

inline JITValuePointer operator!(JITValue& value) {
  return value.notOp();
}

inline JITValuePointer operator+(JITValue& lh, JITValue& rh) {
  return lh.add(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator+(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh + *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator+(T lh, JITValue& rh) {
  return rh + lh;
}

inline JITValuePointer operator-(JITValue& value) {
  return value.uminus();
}

inline JITValuePointer operator-(JITValue& lh, JITValue& rh) {
  return lh.sub(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator-(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh - *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator-(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer - rh;
}

inline JITValuePointer operator*(JITValue& lh, JITValue& rh) {
  return lh.mul(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator*(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh * *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator*(T lh, JITValue& rh) {
  return rh * lh;
}

inline JITValuePointer operator/(JITValue& lh, JITValue& rh) {
  return lh.div(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator/(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh / *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator/(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer / rh;
}

inline JITValuePointer operator%(JITValue& lh, JITValue& rh) {
  return lh.mod(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator%(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh % *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator%(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer % rh;
}

inline JITValuePointer operator==(JITValue& lh, JITValue& rh) {
  return lh.eq(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator==(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh == *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator==(T lh, JITValue& rh) {
  return rh == lh;
}

inline JITValuePointer operator!=(JITValue& lh, JITValue& rh) {
  return lh.ne(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator!=(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh != *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator!=(T lh, JITValue& rh) {
  return rh != lh;
}

inline JITValuePointer operator<(JITValue& lh, JITValue& rh) {
  return lh.lt(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator<(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh < *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator<(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer < rh;
}

inline JITValuePointer operator<=(JITValue& lh, JITValue& rh) {
  return lh.le(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator<=(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh <= *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator<=(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer <= rh;
}

inline JITValuePointer operator>(JITValue& lh, JITValue& rh) {
  return lh.gt(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator>(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh > *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator>(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer > rh;
}

inline JITValuePointer operator>=(JITValue& lh, JITValue& rh) {
  return lh.ge(rh);
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator>=(JITValue& lh, T rh) {
  auto& parent_func = lh.getParentJITFunction();
  auto type = lh.getValueTypeTag();
  JITValuePointer rh_pointer = parent_func.createLiteral(type, rh);
  return lh >= *rh_pointer;
}

template <typename T, IsJITValueConvertable<T> = true>
inline JITValuePointer operator>=(T lh, JITValue& rh) {
  auto& parent_func = rh.getParentJITFunction();
  auto type = rh.getValueTypeTag();
  JITValuePointer lh_pointer = parent_func.createLiteral(type, lh);
  return *lh_pointer >= rh;
}

inline JITValuePointer operator*(JITValue& value) {
  return value.dereference();
}
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITVALUEOPERATIONS_H
