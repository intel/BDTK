/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) 2016-2022 ClickHouse, Inc.
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

#pragma once

#include <common/base/wide_integer.h>
#include <type_traits>

namespace cider::hashtable {

using namespace cider::wide;

static_assert(sizeof(Int256) == 32);
static_assert(sizeof(UInt256) == 32);

/// The standard library type traits, such as std::is_arithmetic, with one exception
/// (std::common_type), are "set in stone". Attempting to specialize them causes undefined
/// behavior. So instead of using the std type_traits, we use our own version which allows
/// extension.
template <typename T>
struct is_signed {
  static constexpr bool value = std::is_signed_v<T>;
};

template <>
struct is_signed<Int128> {
  static constexpr bool value = true;
};
template <>
struct is_signed<Int256> {
  static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_signed_v = is_signed<T>::value;

template <typename T>
struct is_unsigned {
  static constexpr bool value = std::is_unsigned_v<T>;
};

template <>
struct is_unsigned<UInt128> {
  static constexpr bool value = true;
};
template <>
struct is_unsigned<UInt256> {
  static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_unsigned_v = is_unsigned<T>::value;

// TODO(Deegue): Enable after C++20 is supportted.
// template <class T>
// concept is_integer = std::is_integral_v<T> || std::is_same_v<T, Int128> ||
//     std::is_same_v<T, UInt128> || std::is_same_v<T, Int256> || std::is_same_v<T,
//     UInt256>;

// template <class T>
// concept is_floating_point = std::is_floating_point_v<T>;

template <typename T>
struct is_arithmetic {
  static constexpr bool value = std::is_arithmetic_v<T>;
};

template <>
struct is_arithmetic<Int128> {
  static constexpr bool value = true;
};
template <>
struct is_arithmetic<UInt128> {
  static constexpr bool value = true;
};
template <>
struct is_arithmetic<Int256> {
  static constexpr bool value = true;
};
template <>
struct is_arithmetic<UInt256> {
  static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_arithmetic_v = is_arithmetic<T>::value;

template <typename T>
struct make_unsigned {
  using type = std::make_unsigned_t<T>;
};

template <>
struct make_unsigned<Int128> {
  using type = UInt128;
};
template <>
struct make_unsigned<UInt128> {
  using type = UInt128;
};
template <>
struct make_unsigned<Int256> {
  using type = UInt256;
};
template <>
struct make_unsigned<UInt256> {
  using type = UInt256;
};

template <typename T>
using make_unsigned_t = typename make_unsigned<T>::type;

template <typename T>
struct make_signed {
  using type = std::make_signed_t<T>;
};

template <>
struct make_signed<Int128> {
  using type = Int128;
};
template <>
struct make_signed<UInt128> {
  using type = Int128;
};
template <>
struct make_signed<Int256> {
  using type = Int256;
};
template <>
struct make_signed<UInt256> {
  using type = Int256;
};

template <typename T>
using make_signed_t = typename make_signed<T>::type;

template <typename T>
struct is_big_int {
  static constexpr bool value = false;
};

template <>
struct is_big_int<Int128> {
  static constexpr bool value = true;
};
template <>
struct is_big_int<UInt128> {
  static constexpr bool value = true;
};
template <>
struct is_big_int<Int256> {
  static constexpr bool value = true;
};
template <>
struct is_big_int<UInt256> {
  static constexpr bool value = true;
};

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;
}  // namespace cider::hashtable
