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

#pragma once

#include <climits>
#include <cstdint>
namespace facebook::velox::plugin {

inline bool isBitSet(const uint64_t* bits, int32_t idx) {
  // calculate bit size per array slot
  uint64_t bitSize = sizeof(bits[0]) * CHAR_BIT;
  // n & (1 << (idx - 1)), here follows a bitwise impl for (idx - 1)
  return bits[idx / (bitSize)] & (1 << (idx & ((sizeof(bits[0]) * CHAR_BIT) - 1)));
}

// BitMasks for 0b11111110, 0b11111101, ..., 0b01111111
static constexpr uint8_t kZeroBitmasks[] = {
    static_cast<uint8_t>(~(1 << 0)),
    static_cast<uint8_t>(~(1 << 1)),
    static_cast<uint8_t>(~(1 << 2)),
    static_cast<uint8_t>(~(1 << 3)),
    static_cast<uint8_t>(~(1 << 4)),
    static_cast<uint8_t>(~(1 << 5)),
    static_cast<uint8_t>(~(1 << 6)),
    static_cast<uint8_t>(~(1 << 7)),
};

// BitMasks for 0b00000001, 0b00000010, ..., 0b10000000
static constexpr uint8_t kOneBitmasks[] =
    {1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7};

inline void clearBit(uint64_t* bits, int32_t idx) {
  auto castBits = reinterpret_cast<uint8_t*>(bits);
  // bitmask with only one 0 at kth
  castBits[idx / 8] &= kZeroBitmasks[idx % 8];
}

inline void setBit(uint64_t* bits, int32_t idx) {
  auto castBits = reinterpret_cast<uint8_t*>(bits);
  // bitmask with only one 1 at kth
  castBits[idx / 8] |= kOneBitmasks[idx % 8];
}
}  // namespace facebook::velox::plugin
