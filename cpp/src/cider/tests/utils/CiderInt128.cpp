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

#include "CiderInt128.h"
#include "cider/CiderException.h"

std::string CiderInt128Utils::Int128ToString(__int128_t input) {
  uint8_t precision_counter = 0;
  uint64_t remainder = 0;
  std::string result;
  bool is_negative = input < 0;

  if (is_negative) {
    input = -input;
  }

  // build string with % 10 remainder
  while (input) {
    remainder = input % 10;
    input /= 10;
    result = std::string(1, '0' + remainder) + result;
    precision_counter++;
    if (precision_counter > 38) {
      // int128 has a maximum precision of 38 digits
      CIDER_THROW(CiderRuntimeException, "decimal value overflow");
    }
  }

  if (!result.size()) {
    return std::string("0");
  }

  return is_negative ? "-" + result : result;
}

std::string CiderInt128Utils::Decimal128ToString(__int128_t input,
                                                 uint8_t precision,
                                                 uint8_t scale) {
  if (!scale) {
    // treat as an integer
    return Int128ToString(input);
  }

  uint8_t precision_counter = 0;
  uint8_t scale_counter = 0;
  uint64_t remainder = 0;
  std::string result;
  bool is_negative = input < 0;
  if (is_negative) {
    input = -input;
  }

  while (input) {
    remainder = input % 10;
    input /= 10;
    result = std::string(1, '0' + remainder) + result;

    scale_counter++;
    if (scale_counter == scale) {
      result = "." + result;
    }

    precision_counter++;
    if (precision_counter > precision) {
      CIDER_THROW(CiderRuntimeException, "decimal value overflow");
    }
  }

  if (scale_counter < scale) {
    // in cases where scale > length, append 0's
    while (scale_counter < scale) {
      // pad zeros
      result = "0" + result;
      scale_counter++;
    }
    // append 0. to the very beginning
    result = "0." + result;
  } else if (scale_counter == scale) {
    // scale == length, append 0
    result = "0" + result;
  }

  return is_negative ? "-" + result : result;
}

double CiderInt128Utils::Decimal128ToDouble(__int128_t input,
                                            uint8_t precision,
                                            uint8_t scale) {
  auto val_str = Decimal128ToString(input, precision, scale);
  return std::stod(val_str);
}
