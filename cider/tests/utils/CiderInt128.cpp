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

#include "CiderInt128.h"

CiderInt128::CiderInt128(int64_t hi, uint64_t lo) {
  this->high = hi;
  this->low = lo;
}

CiderInt128::CiderInt128(int64_t value) {
  auto temp = CiderInt128Utils::Int64ToCiderInt128(value);
  this->high = temp.high;
  this->low = temp.low;
}

bool CiderInt128::operator==(const CiderInt128& rhs) const {
  return CiderInt128Utils::Equal(*this, rhs);
}

uint8_t CiderInt128Utils::HighestSetBitPositive(const CiderInt128& value) {
  uint8_t result = 0;

  if (value.high) {
    result = 64;
    uint64_t hi = (uint64_t)value.high;
    while (hi) {
      hi = hi >> 1;
      result++;
    }
  } else if (value.low) {
    result = 0;
    uint64_t lo = value.low;
    while (lo) {
      lo = lo >> 1;
      result++;
    }
  }

  return result;
}

bool CiderInt128Utils::IsBitSetAt(const CiderInt128& value, uint8_t pos) {
  if (pos < 64) {
    return value.low & (uint64_t(1) << pos);
  } else {
    return value.high & (uint64_t(1) << (uint64_t(pos - 64)));
  }
}

CiderInt128 CiderInt128Utils::Int64ToCiderInt128(int64_t value) {
  auto result = CiderInt128(0, 0);
  result.low = (uint64_t)value;
  result.high = value < 0 ? -1 : 0;

  return result;
}

CiderInt128 CiderInt128Utils::LeftShift(const CiderInt128& input, uint8_t n) {
  // only supports shifting (0, 64) bits
  CHECK_GT(n, 0);
  CHECK_LT(n, 64);

  auto result = CiderInt128(0, 0);
  result.low = input.low << n;
  result.high = (input.high << n) + (input.low >> (64 - n));

  return result;
}

CiderInt128 CiderInt128Utils::DivModPositive(const CiderInt128& numerator,
                                             uint64_t denominator,
                                             uint64_t& remainder) {
  CHECK_GE(numerator.high, 0);
  auto result = CiderInt128(0);
  remainder = 0;

  auto highest_set_bit = HighestSetBitPositive(numerator);
  for (auto x = highest_set_bit; x > 0; --x) {
    // left-shift result and remainder
    result = LeftShift(result);
    remainder = remainder << 1;

    if (IsBitSetAt(numerator, x - 1)) {
      // if current bit is set, add current bit to remainder
      remainder += 1;
    }

    if (remainder >= denominator) {
      remainder -= denominator;
      result.low++;
      if (result.low == 0) {
        // low overflowed, add carriage to high
        result.high++;
      }
    }
  }

  return result;
}

CiderInt128 CiderInt128Utils::Negate(const CiderInt128& input) {
  auto result = CiderInt128(0, 0);
  if (input.high == std::numeric_limits<int64_t>::min() && input.low == 0) {
    CIDER_THROW(CiderRuntimeException, "Int128 overflowed!");
  }

  result.low = std::numeric_limits<uint64_t>::max() - input.low + 1;
  result.high = -1 - input.high + (input.low == 0);

  return result;
}

bool CiderInt128Utils::Equal(const CiderInt128& lhs, const CiderInt128& rhs) {
  bool high_eq = lhs.high == rhs.high;
  bool low_eq = lhs.low == rhs.low;

  return high_eq && low_eq;
}

std::string CiderInt128Utils::Int128ToString(CiderInt128 input) {
  uint64_t remainder = 0;
  std::string result;
  bool is_negative = input.high < 0;

  if (is_negative) {
    input = Negate(input);
  }

  // build string with % 10 remainder
  while (true) {
    if (!input.low && !input.high) {
      break;
    }
    input = DivModPositive(input, 10, remainder);
    result = std::string(1, '0' + remainder) + result;
  }

  if (!result.size()) {
    return std::string("0");
  }

  return is_negative ? "-" + result : result;
}

std::string CiderInt128Utils::Decimal128ToString(CiderInt128 input,
                                                 uint8_t width,
                                                 uint8_t scale) {
  if (!scale) {
    // treat as an integer
    return Int128ToString(input);
  }

  uint8_t scale_counter = 0;
  uint64_t remainder = 0;
  std::string result;
  bool is_negative = input.high < 0;
  if (is_negative) {
    input = Negate(input);
  }

  while (true) {
    if (!input.low && !input.high) {
      break;
    }
    scale_counter++;
    input = DivModPositive(input, 10, remainder);
    result = std::string(1, '0' + remainder) + result;
    if (scale_counter == scale) {
      result = "." + result;
    }
  }

  return is_negative ? "-" + result : result;
}
