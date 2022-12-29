/*
 * Copyright (c) 2022 Intel Corporation.
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

// #include <common/Types.h>
// #include <common/base/StringRef.h>
// #include <common/base/types.h>
#include <common/base/unaligned.h>
#include <common/contrib/cityhash102/include/city.h>

#include <type_traits>

/** Hash functions that are better than the trivial function std::hash.
 *
 * Example: when we do aggregation by the visitor ID, the performance increase is more
 * than 5 times. This is because of following reasons:
 * - in Metrica web analytics system, visitor identifier is an integer that has timestamp
 * with seconds resolution in lower bits;
 * - in typical implementation of standard library, hash function for integers is trivial
 * and just use lower bits;
 * - traffic is non-uniformly distributed across a day;
 * - we are using open-addressing linear probing hash tables that are most critical to
 * hash function quality, and trivial hash function gives disastrous results.
 */

/** Taken from MurmurHash. This is Murmur finalizer.
 * Faster than intHash32 when inserting into the hash table uint64_t -> uint64_t, where
 * the key is the visitor ID.
 */
inline uint64_t intHash64(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;

  return x;
}

/** CRC32C is not very high-quality as a hash function,
 *  according to avalanche and bit independence tests (see SMHasher software), as well as
 * a small number of bits, but can behave well when used in hash tables, due to high speed
 * (latency 3 + 1 clock cycle, throughput 1 clock cycle). Works only with SSE 4.2 support.
 */
#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

template <typename T>
struct is_big_int {
  static constexpr bool value = false;
};

template <typename T>
inline constexpr bool is_big_int_v = is_big_int<T>::value;

/// NOTE: Intel intrinsic can be confusing.
/// - https://code.google.com/archive/p/sse-intrinsics/wikis/PmovIntrinsicBug.wiki
/// - https://stackoverflow.com/questions/15752770/mm-crc32-u64-poorly-defined
inline uint64_t intHashCRC32(uint64_t x) {
#ifdef __SSE4_2__
  return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
  return __crc32cd(-1U, x);
#else
  /// On other platforms we do not have CRC32. NOTE This can be confusing.
  /// NOTE: consider using intHash32()
  return intHash64(x);
#endif
}
inline uint64_t intHashCRC32(uint64_t x, uint64_t updated_value) {
#ifdef __SSE4_2__
  return _mm_crc32_u64(updated_value, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
  return __crc32cd(static_cast<uint32_t>(updated_value), x);
#else
  /// On other platforms we do not have CRC32. NOTE This can be confusing.
  return intHash64(x) ^ updated_value;
#endif
}

// template <typename T>
// requires std::has_unique_object_representations_v<T> && (sizeof(T) % sizeof(uint64_t)
// == 0) inline uint64_t intHashCRC32(const T & x, uint64_t updated_value)
// {
//     const auto * begin = reinterpret_cast<const char *>(&x);
//     for (size_t i = 0; i < sizeof(T); i += sizeof(uint64_t))
//     {
//         updated_value = intHashCRC32(unalignedLoad<uint64_t>(begin), updated_value);
//         begin += sizeof(uint64_t);
//     }

//     return updated_value;
// }

template <typename T>
// template <std::floating_point T>
// requires(sizeof(T) <= sizeof(uint64_t))
inline uint64_t intHashCRC32(T x, uint64_t updated_value) {
  static_assert(std::numeric_limits<T>::is_iec559);

  // In IEEE 754, the only two floating point numbers that compare equal are 0.0 and -0.0.
  // See std::hash<float>.
  if (x == static_cast<T>(0.0))
    return intHashCRC32(0, updated_value);

  uint64_t repr;

  if constexpr (sizeof(T) == sizeof(uint32_t))
    std::memcpy(&repr, &x, 4);
  else
    std::memcpy(&repr, &x, 8);

  return intHashCRC32(repr, updated_value);
}

inline uint32_t updateWeakHash32(const uint8_t* pos,
                                 size_t size,
                                 uint32_t updated_value) {
  if (size < 8) {
    uint64_t value = 0;

    switch (size) {
      case 0:
        break;
      case 1:
        __builtin_memcpy(&value, pos, 1);
        break;
      case 2:
        __builtin_memcpy(&value, pos, 2);
        break;
      case 3:
        __builtin_memcpy(&value, pos, 3);
        break;
      case 4:
        __builtin_memcpy(&value, pos, 4);
        break;
      case 5:
        __builtin_memcpy(&value, pos, 5);
        break;
      case 6:
        __builtin_memcpy(&value, pos, 6);
        break;
      case 7:
        __builtin_memcpy(&value, pos, 7);
        break;
      default:
        UNREACHABLE();
    }

    reinterpret_cast<unsigned char*>(&value)[7] = size;
    return static_cast<uint32_t>(intHashCRC32(value, updated_value));
  }

  const auto* end = pos + size;
  while (pos + 8 <= end) {
    auto word = unalignedLoad<uint64_t>(pos);
    updated_value = static_cast<uint32_t>(intHashCRC32(word, updated_value));

    pos += 8;
  }

  if (pos < end) {
    /// If string size is not divisible by 8.
    /// Lets' assume the string was 'abcdefghXYZ', so it's tail is 'XYZ'.
    uint8_t tail_size = end - pos;
    /// Load tailing 8 bytes. Word is 'defghXYZ'.
    auto word = unalignedLoad<uint64_t>(end - 8);
    /// Prepare mask which will set other 5 bytes to 0. It is 0xFFFFFFFFFFFFFFFF << 5 =
    /// 0xFFFFFF0000000000. word & mask = '\0\0\0\0\0XYZ' (bytes are reversed because of
    /// little ending)
    word &= (~uint64_t(0)) << uint8_t(8 * (8 - tail_size));
    /// Use least byte to store tail length.
    word |= tail_size;
    /// Now word is '\3\0\0\0\0XYZ'
    updated_value = static_cast<uint32_t>(intHashCRC32(word, updated_value));
  }

  return updated_value;
}

template <typename T>
// requires (sizeof(T) <= sizeof(uint64_t))
inline size_t DefaultHash64(T key) {
  uint64_t out{0};
  std::memcpy(&out, &key, sizeof(T));
  return intHash64(out);
}

// TODO: Implement and enable later
// template <typename T>
// requires (sizeof(T) > sizeof(uint64_t))
// inline size_t DefaultHash64(T key)
// {
//     if constexpr (is_big_int_v<T> && sizeof(T) == 16)
//     {
//         /// TODO This is classical antipattern.
//         return intHash64(
//             static_cast<uint64_t>(key) ^
//             static_cast<uint64_t>(key >> 64));
//     }
//     else if constexpr (std::is_same_v<T, DB::UUID>)
//     {
//         return intHash64(
//             static_cast<uint64_t>(key.toUnderType()) ^
//             static_cast<uint64_t>(key.toUnderType() >> 64));
//     }
//     else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
//     {
//         return intHash64(
//             static_cast<uint64_t>(key) ^
//             static_cast<uint64_t>(key >> 64) ^
//             static_cast<uint64_t>(key >> 128) ^
//             static_cast<uint64_t>(key >> 256));
//     }
//     UNREACHABLE();
// }

template <typename T>
struct DefaultHash {
  size_t operator()(T key) const { return DefaultHash64<T>(key); }
};

// template <typename T>
// template <DB::is_decimal T>
// struct DefaultHash<T>
// {
//     size_t operator() (T key) const
//     {
//         return DefaultHash64<typename T::NativeType>(key.value);
//     }
// };

template <typename T>
struct HashCRC32;

template <typename T>
// requires (sizeof(T) <= sizeof(uint64_t))
inline size_t hashCRC32(T key, uint64_t updated_value = -1) {
  uint64_t out{0};
  std::memcpy(&out, &key, sizeof(T));
  return intHashCRC32(out, updated_value);
}

// template <typename T>
// requires (sizeof(T) > sizeof(uint64_t))
// inline size_t hashCRC32(T key, uint64_t updated_value = -1)
// {
//     return intHashCRC32(key, updated_value);
// }

#define DEFINE_HASH(T)                                           \
  template <>                                                    \
  struct HashCRC32<T> {                                          \
    size_t operator()(T key) const { return hashCRC32<T>(key); } \
  };

DEFINE_HASH(uint8_t)
DEFINE_HASH(uint16_t)
DEFINE_HASH(uint32_t)
DEFINE_HASH(uint64_t)
DEFINE_HASH(int8_t)
DEFINE_HASH(int16_t)
DEFINE_HASH(int32_t)
DEFINE_HASH(int64_t)
DEFINE_HASH(float)
DEFINE_HASH(double)

#undef DEFINE_HASH

/// It is reasonable to use for uint8_t, uint16_t with sufficient hash table size.
struct TrivialHash {
  template <typename T>
  size_t operator()(T key) const {
    return key;
  }
};

/** A relatively good non-cryptographic hash function from uint64_t to uint32_t.
 * But worse (both in quality and speed) than just cutting intHash64.
 * Taken from here: http://www.concentric.net/~ttwang/tech/inthash.htm
 *
 * Slightly changed compared to the function by link: shifts to the right are accidentally
 * replaced by a cyclic shift to the right. This change did not affect the smhasher test
 * results.
 *
 * It is recommended to use different salt for different tasks.
 * That was the case that in the database values were sorted by hash (for low-quality
 * pseudo-random spread), and in another place, in the aggregate function, the same hash
 * was used in the hash table, as a result, this aggregate function was monstrously slowed
 * due to collisions.
 *
 * NOTE Salting is far from perfect, because it commutes with first steps of calculation.
 *
 * NOTE As mentioned, this function is slower than intHash64.
 * But occasionally, it is faster, when written in a loop and loop is vectorized.
 */
template <uint64_t salt>
inline uint32_t intHash32(uint64_t key) {
  key ^= salt;

  key = (~key) + (key << 18);
  key = key ^ ((key >> 31) | (key << 33));
  key = key * 21;
  key = key ^ ((key >> 11) | (key << 53));
  key = key + (key << 6);
  key = key ^ ((key >> 22) | (key << 42));

  return static_cast<uint32_t>(key);
}

/// For containers.
template <typename T, uint64_t salt = 0>
struct IntHash32 {
  size_t operator()(const T& key) const {
    if constexpr (is_big_int_v<T> && sizeof(T) == 16) {
      return intHash32<salt>(key.items[0] ^ key.items[1]);
    } else if constexpr (is_big_int_v<T> && sizeof(T) == 32) {
      return intHash32<salt>(key.items[0] ^ key.items[1] ^ key.items[2] ^ key.items[3]);
    } else {
      if constexpr (sizeof(T) <= sizeof(uint64_t)) {
        uint64_t out{0};
        std::memcpy(&out, &key, sizeof(T));
        return intHash32<salt>(out);
      }
    }

    UNREACHABLE();
  }
};

// TODO: Implement and enable later
// template <>
// struct DefaultHash<StringRef> : public StringRefHash {};
