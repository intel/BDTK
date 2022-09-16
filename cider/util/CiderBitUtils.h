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

#ifndef CIDER_CIDERBITUTILS_H
#define CIDER_CIDERBITUTILS_H

#include <cstdint>
#include <cstdlib>
#include <memory>

#include "type/data/funcannotations.h"

namespace CiderBitUtils {

template <size_t AlignmentFactor = 16>
class CiderBitVector {
 public:
  constexpr static size_t kSizeAlignmentFactor = AlignmentFactor;
  constexpr static size_t kAlignmentOffset = []() {
    static_assert((kSizeAlignmentFactor > 0 &&
                   0 == (kSizeAlignmentFactor & (kSizeAlignmentFactor - 1))),
                  "kSizeAlignmentFactor should be power of two.");
    size_t ans = 0, num = kSizeAlignmentFactor;
    while (num >>= 1) {
      ++ans;
    }
    return ans;
  }();

  explicit CiderBitVector(size_t bits_num, uint8_t init_val = 0)
      : bits_num_(alignBitsNum(bits_num))
      , data_(bits_num_ ? std::aligned_alloc(kSizeAlignmentFactor, bits_num_ >> 3)
                        : nullptr) { /* Mem alignment, default is 16 bytes*/
    resetBits(init_val);
  }

  CiderBitVector() : CiderBitVector(0) {}

  ~CiderBitVector() { std::free(data_); }

  CiderBitVector(const CiderBitVector& rh)
      : bits_num_(rh.bits_num_)
      , data_(bits_num_ ? std::aligned_alloc(kSizeAlignmentFactor, bits_num_ >> 3)
                        : nullptr) {
    uint8_t* ptr = as<uint8_t>();
    const uint8_t* rh_ptr = rh.as<uint8_t>();
    for (size_t i = 0; i < (bits_num_ >> 3); ++i) {
      ptr[i] = rh_ptr[i];
    }
  }

  CiderBitVector& operator=(const CiderBitVector& rh) {
    if (this == &rh) {
      return *this;
    }

    std::free(data_);

    bits_num_ = rh.bits_num_;
    uint8_t* ptr = as<uint8_t>();
    const uint8_t* rh_ptr = rh.as<uint8_t>();
    for (size_t i = 0; i < (bits_num_ >> 3); ++i) {
      ptr[i] = rh_ptr[i];
    }
  }

  CiderBitVector& operator=(CiderBitVector&& rh) noexcept {
    if (this == &rh) {
      return *this;
    }

    std::free(data_);

    bits_num_ = rh.bits_num_;
    data_ = rh.data_;

    rh.bits_num_ = 0;
    rh.data_ = nullptr;

    return *this;
  }

  CiderBitVector(CiderBitVector&& rh) noexcept
      : bits_num_(rh.bits_num_), data_(rh.data_) {
    rh.bits_num_ = 0;
    rh.data_ = nullptr;
  }

  template <typename T>
  T* as() {
    return reinterpret_cast<T*>(data_);
  }

  template <typename T>
  const T* as() const {
    return reinterpret_cast<const T*>(data_);
  }

  size_t getBitsNum() const { return bits_num_; }

  void resetBits(uint8_t val) {
    uint8_t* ptr = as<uint8_t>();
    for (size_t i = 0; i < (bits_num_ >> 3); ++i) {
      ptr[i] = val;
    }
  }

 private:
  // Round up bit num based on alingment factor.
  static size_t alignBitsNum(size_t expect_bits) {
    expect_bits += 7;
    expect_bits >>= 3;
    expect_bits += kSizeAlignmentFactor - 1;
    expect_bits >>= kAlignmentOffset;
    expect_bits <<= kAlignmentOffset;
    return expect_bits << 3;
  }

  size_t bits_num_;
  void* data_;
};

constexpr static uint8_t kCiderBitMask[] =
    {1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7};

constexpr static uint8_t kCiderBitReverseMask[] = {static_cast<uint8_t>(~(1)),
                                                   static_cast<uint8_t>(~(1 << 1)),
                                                   static_cast<uint8_t>(~(1 << 2)),
                                                   static_cast<uint8_t>(~(1 << 3)),
                                                   static_cast<uint8_t>(~(1 << 4)),
                                                   static_cast<uint8_t>(~(1 << 5)),
                                                   static_cast<uint8_t>(~(1 << 6)),
                                                   static_cast<uint8_t>(~(1 << 7))};

FORCE_INLINE bool isBitSetAt(const uint8_t* bit_vector, size_t index) {
  return bit_vector[index >> 3] & kCiderBitMask[index & 0x7];
}

FORCE_INLINE void setBitAt(uint8_t* bit_vector, size_t index) {
  bit_vector[index >> 3] |= kCiderBitMask[index & 0x7];
}

FORCE_INLINE void clearBitAt(uint8_t* bit_vector, size_t index) {
  bit_vector[index >> 3] &= kCiderBitReverseMask[index & 0x7];
}

inline size_t countSetBits(uint8_t* bit_vector, size_t end) {
  size_t i = 0, ans = 0;
  for (; i + 64 <= end; i += 64) {
    ans += __builtin_popcountl(reinterpret_cast<uint64_t*>(bit_vector)[i >> 6]);
  }

  for (; i < end; ++i) {
    ans += isBitSetAt(bit_vector, i);
  }

  return ans;
}
};  // namespace CiderBitUtils

#endif
