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

#include <cassert>
#include <functional>
#include <iosfwd>
#include <stdexcept>
#include <string>
#include <vector>

#include <common/base/unaligned.h>
#include <common/contrib/cityhash102/include/city.h>

#include <emmintrin.h>
#include <nmmintrin.h>
#include <smmintrin.h>

namespace cider::hashtable {

/**
 * The std::string_view-like container to avoid creating strings to find substrings in the
 * hash table.
 */
struct StringRef {
  const char* data = nullptr;
  size_t size = 0;

  // Non-constexpr due to reinterpret_cast.
  template <typename CharT>
  StringRef(const CharT* data_, size_t size_)
      : data(reinterpret_cast<const char*>(data_)), size(size_) {
    // Sanity check for overflowed values.
    assert(size < 0x8000000000000000ULL);
  }

  constexpr StringRef(const char* data_, size_t size_) : data(data_), size(size_) {}

  StringRef(const std::string& s) : data(s.data()), size(s.size()) {}  /// NOLINT
  constexpr explicit StringRef(std::string_view s) : data(s.data()), size(s.size()) {}
  constexpr StringRef(const char* data_)
      : StringRef(std::string_view{data_}) {}  /// NOLINT
  constexpr StringRef() = default;

  bool empty() const { return size == 0; }

  std::string toString() const { return std::string(data, size); }
  explicit operator std::string() const { return toString(); }

  std::string_view toView() const { return std::string_view(data, size); }
  constexpr explicit operator std::string_view() const {
    return std::string_view(data, size);
  }
};

using StringRefs = std::vector<StringRef>;

/** Compare strings for equality.
 * The approach is controversial and does not win in all cases.
 * For more information, see hash_map_string_2.cpp
 */
inline bool compareSSE2(const char* p1, const char* p2) {
  return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))));
}

inline bool compareSSE2x4(const char* p1, const char* p2) {
  return 0xFFFF ==
         _mm_movemask_epi8(_mm_and_si128(
             _mm_and_si128(
                 _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))),
                 _mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 1),
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 1))),
             _mm_and_si128(
                 _mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 2),
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 2)),
                 _mm_cmpeq_epi8(
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1) + 3),
                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2) + 3)))));
}

inline bool memequalSSE2Wide(const char* p1, const char* p2, size_t size) {
  /** The order of branches and the trick with overlapping comparisons
   * are the same as in memcpy implementation.
   */
  if (size <= 16) {
    // TODO(Deegue): Add 17..24 bytes support
    if (size >= 8) {
      // Chunks of 8..16 bytes.
      return unalignedLoad<uint64_t>(p1) == unalignedLoad<uint64_t>(p2) &&
             unalignedLoad<uint64_t>(p1 + size - 8) ==
                 unalignedLoad<uint64_t>(p2 + size - 8);
    } else if (size >= 4) {
      // Chunks of 4..7 bytes.
      return unalignedLoad<uint32_t>(p1) == unalignedLoad<uint32_t>(p2) &&
             unalignedLoad<uint32_t>(p1 + size - 4) ==
                 unalignedLoad<uint32_t>(p2 + size - 4);
    } else if (size >= 2) {
      // Chunks of 2..3 bytes.
      return unalignedLoad<uint16_t>(p1) == unalignedLoad<uint16_t>(p2) &&
             unalignedLoad<uint16_t>(p1 + size - 2) ==
                 unalignedLoad<uint16_t>(p2 + size - 2);
    } else if (size >= 1) {
      // A single byte.
      return *p1 == *p2;
    }
    return true;
  }

  while (size >= 64) {
    if (compareSSE2x4(p1, p2)) {
      p1 += 64;
      p2 += 64;
      size -= 64;
    } else {
      return false;
    }
  }

  switch (size / 16) {
    case 3:
      if (!compareSSE2(p1 + 32, p2 + 32))
        return false;
      [[fallthrough]];
    case 2:
      if (!compareSSE2(p1 + 16, p2 + 16))
        return false;
      [[fallthrough]];
    case 1:
      if (!compareSSE2(p1, p2))
        return false;
  }

  return compareSSE2(p1 + size - 16, p2 + size - 16);
}

inline bool operator==(StringRef lhs, StringRef rhs) {
  if (lhs.size != rhs.size)
    return false;

  if (lhs.size == 0)
    return true;

  return memequalSSE2Wide(lhs.data, rhs.data, lhs.size);
}

inline bool operator!=(StringRef lhs, StringRef rhs) {
  return !(lhs == rhs);
}

inline bool operator<(StringRef lhs, StringRef rhs) {
  int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
  return cmp < 0 || (cmp == 0 && lhs.size < rhs.size);
}

inline bool operator>(StringRef lhs, StringRef rhs) {
  int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
  return cmp > 0 || (cmp == 0 && lhs.size > rhs.size);
}

struct StringRefHash64 {
  size_t operator()(StringRef x) const {
    return CityHash_v1_0_2::CityHash64(x.data, x.size);
  }
};

// Parts are taken from CityHash.

inline uint64_t hashLen16(uint64_t u, uint64_t v) {
  return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(u, v));
}

inline uint64_t shiftMix(uint64_t val) {
  return val ^ (val >> 47);
}

inline uint64_t rotateByAtLeast1(uint64_t val, uint8_t shift) {
  return (val >> shift) | (val << (64 - shift));
}

inline size_t hashLessThan8(const char* data, size_t size) {
  static constexpr uint64_t k2 = 0x9ae16a3b2f90404fULL;
  static constexpr uint64_t k3 = 0xc949d7c7509e6557ULL;

  if (size >= 4) {
    uint64_t a = unalignedLoad<uint32_t>(data);
    return hashLen16(size + (a << 3), unalignedLoad<uint32_t>(data + size - 4));
  }

  if (size > 0) {
    uint8_t a = data[0];
    uint8_t b = data[size >> 1];
    uint8_t c = data[size - 1];
    uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
    uint32_t z = static_cast<uint32_t>(size) + (static_cast<uint32_t>(c) << 2);
    return shiftMix(y * k2 ^ z * k3) * k2;
  }

  return k2;
}

inline size_t hashLessThan16(const char* data, size_t size) {
  if (size > 8) {
    uint64_t a = unalignedLoad<uint64_t>(data);
    uint64_t b = unalignedLoad<uint64_t>(data + size - 8);
    return hashLen16(a, rotateByAtLeast1(b + size, static_cast<uint8_t>(size))) ^ b;
  }

  return hashLessThan8(data, size);
}

struct CRC32Hash {
  unsigned operator()(StringRef x) const {
    const char* pos = x.data;
    size_t size = x.size;

    if (size == 0) {
      return 0;
    }

    if (size < 8) {
      return static_cast<unsigned>(hashLessThan8(x.data, x.size));
    }

    const char* end = pos + size;
    unsigned res = -1U;

    do {
      uint64_t word = unalignedLoad<uint64_t>(pos);
      res = static_cast<unsigned>(_mm_crc32_u64(res, word));

      pos += 8;
    } while (pos + 8 < end);

    uint64_t word = unalignedLoad<uint64_t>(end - 8);  // I'm not sure if this is normal.
    res = static_cast<unsigned>(_mm_crc32_u64(res, word));

    return res;
  }
};

struct StringRefHash : CRC32Hash {};

namespace ZeroTraits {
inline bool check(const StringRef& x) {
  return 0 == x.size;
}
inline void set(StringRef& x) {
  x.size = 0;
}
}  // namespace ZeroTraits

std::ostream& operator<<(std::ostream& os, const StringRef& str);

}  // namespace cider::hashtable
