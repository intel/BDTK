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

// code adapted from DuckDb Hugeint types
// https://github.com/duckdb/duckdb/blob/master/src/common/types/hugeint.cpp

#ifndef CIDER_CIDERDECIMAL128_H
#define CIDER_CIDERDECIMAL128_H

#include <limits>
#include <string>
#include <cstdint>

#include "cider/CiderBatch.h"

// 128-bit integer consisting of a int64 (high) and a uint64 (low)
// value = 2^64 * high + low
// can also be used for representing fix-point decimals
struct CiderInt128 {
  int64_t high;
  uint64_t low;

  CiderInt128() = default;
  CiderInt128(const CiderInt128& rhs) = default;
  CiderInt128(CiderInt128&& rhs) = default;
  CiderInt128& operator=(const CiderInt128& rhs) = default;
  CiderInt128& operator=(CiderInt128&& rhs) = default;

  explicit CiderInt128(int64_t hi, uint64_t lo);
  explicit CiderInt128(int64_t value);
};

class CiderInt128Utils {
 private:
  // Finds the highest bit that is set to 1 in a positive CiderInt128 instance
  // returns 1-indexed bit position, and returns 0 if all bits are 0
  static uint8_t HighestSetBitPositive(const CiderInt128& value);

  // Checks if a bit is set at position pos
  static bool IsBitSetAt(const CiderInt128& value, uint8_t pos);

 public:
  static CiderInt128 Int64ToCiderInt128(int64_t value);
  static std::string ToString(const CiderInt128& value,
                              uint8_t width = 38,
                              uint8_t scale = 0);
  // computes int128-uint64 division
  // the numerator int128 must be positive
  // returns quotient, outputs remainder via remainder out reference parameter
  static CiderInt128 DivModPositive(const CiderInt128& numerator,
                                    uint64_t denominator,
                                    uint64_t& remainder);
  // left shift
  static CiderInt128 LeftShift(const CiderInt128& input, uint8_t n = 1);
  // negation
  static CiderInt128 Negate(const CiderInt128& input);

  static std::string Int128ToString(CiderInt128 input);
  static std::string Decimal128ToString(CiderInt128 input,
                                        uint8_t width,
                                        uint8_t scale);
};

#endif
