/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef QUERYENGINE_COUNTDISTINCT_H
#define QUERYENGINE_COUNTDISTINCT_H

#include "HyperLogLog.h"
#include "cider/CiderBatch.h"
#include "exec/template/common/descriptors/CountDistinctDescriptor.h"
#include "robin_hood.h"

#include <bitset>
#include <vector>

using CountDistinctDescriptors = std::vector<CountDistinctDescriptor>;

inline size_t bitmap_set_size(const int8_t* bitmap, const size_t bitmap_byte_sz) {
  const auto bitmap_word_count = bitmap_byte_sz >> 3;
  const auto bitmap_rem_bytes = bitmap_byte_sz & 7;
  const auto bitmap64 = reinterpret_cast<const int64_t*>(bitmap);
  size_t set_size = 0;
  for (size_t i = 0; i < bitmap_word_count; ++i) {
    std::bitset<64> word_bitset(bitmap64[i]);
    set_size += word_bitset.count();
  }
  const auto rem_bitmap = reinterpret_cast<const int8_t*>(&bitmap64[bitmap_word_count]);
  for (size_t i = 0; i < bitmap_rem_bytes; ++i) {
    std::bitset<8> byte_bitset(rem_bitmap[i]);
    set_size += byte_bitset.count();
  }
  return set_size;
}

inline void bitmap_set_union(int8_t* lhs, int8_t* rhs, const size_t bitmap_sz) {
  for (size_t i = 0; i < bitmap_sz; ++i) {
    lhs[i] = lhs[i] | rhs[i];
  }
}

// Bring all the set bits in the multiple sub-bitmaps into the first sub-bitmap.
inline void partial_bitmap_union(int8_t* set_vals,
                                 const CountDistinctDescriptor& count_distinct_desc) {
  auto partial_set_vals = set_vals;
  CHECK_EQ(
      size_t(0),
      count_distinct_desc.bitmapPaddedSizeBytes() % count_distinct_desc.sub_bitmap_count);
  const auto partial_padded_size =
      count_distinct_desc.bitmapPaddedSizeBytes() / count_distinct_desc.sub_bitmap_count;
  for (size_t i = 1; i < count_distinct_desc.sub_bitmap_count; ++i) {
    partial_set_vals += partial_padded_size;
    bitmap_set_union(set_vals, partial_set_vals, count_distinct_desc.bitmapSizeBytes());
  }
}

template <typename T>
inline std::vector<T> count_distinct_set(
    const int64_t set_handle,
    const CountDistinctDescriptor& count_distinct_desc) {
  if (!set_handle) {
    return {};
  }
  std::vector<T> values;
  if (count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
    auto set_vals = reinterpret_cast<unsigned int8_t*>(set_handle);
    for (int i = 0; i < count_distinct_desc.bitmapSizeBytes(); i++) {
      auto set_val = set_vals[i];
      int j = 0;
      // Example: 0010 0111
      while (set_val >> 1 != 0) {
        if (set_val % 2 == 1) {
          auto value = i * 8 + j + count_distinct_desc.min_val;
          auto value_1 = (T)value;
          values.push_back(value_1);
        }
        set_val = set_val >> 1;
        j++;
      }
      if (set_val % 2 == 1) {
        auto value = i * 8 + j + count_distinct_desc.min_val;
        auto value_1 = (T)value;
        values.push_back(value_1);
      }
    }
    return values;
  }

  CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
  auto uset = reinterpret_cast<robin_hood::unordered_set<int64_t>*>(set_handle);
  for (auto iter = uset->begin(); iter != uset->end(); ++iter) {
    values.push_back((T)*iter);
  }
  return values;
}

template <typename T>
inline int64_t wrap_to_batch(std::vector<T> values,
                             std::shared_ptr<CiderAllocator> allocator) {
  T* column = reinterpret_cast<T*>(allocator->allocate(sizeof(T) * values.size()));
  memcpy(column, values.data(), sizeof(T) * values.size());

  std::vector<const int8_t*> table_ptr;
  table_ptr.push_back(reinterpret_cast<int8_t*>(column));
  CiderBatch* batch = new CiderBatch(values.size(), table_ptr);
  // TODO: may need add schema
  return reinterpret_cast<int64_t>(batch);
}

inline int64_t count_distinct_set_address(
    const int64_t set_handle,
    const CountDistinctDescriptor& count_distinct_desc,
    std::shared_ptr<CiderAllocator> allocator) {
  int64_t distinct_vals_addr;
  switch (count_distinct_desc.arg_type) {
    case SQLTypes::kTINYINT: {
      auto values = count_distinct_set<int8_t>(set_handle, count_distinct_desc);
      distinct_vals_addr =
          reinterpret_cast<int64_t>(wrap_to_batch<int8_t>(values, allocator));
      break;
    }
    case SQLTypes::kSMALLINT: {
      auto values = count_distinct_set<int16_t>(set_handle, count_distinct_desc);
      distinct_vals_addr =
          reinterpret_cast<int64_t>(wrap_to_batch<int16_t>(values, allocator));
      break;
    }
    case SQLTypes::kINT: {
      CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
      auto values = count_distinct_set<int32_t>(set_handle, count_distinct_desc);
      distinct_vals_addr =
          reinterpret_cast<int64_t>(wrap_to_batch<int32_t>(values, allocator));
      break;
    }
    case SQLTypes::kBIGINT: {
      CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
      auto values = count_distinct_set<int64_t>(set_handle, count_distinct_desc);
      distinct_vals_addr =
          reinterpret_cast<int64_t>(wrap_to_batch<int64_t>(values, allocator));
      break;
    }
  }
  return distinct_vals_addr;
}

inline int64_t count_distinct_set_size(
    const int64_t set_handle,
    const CountDistinctDescriptor& count_distinct_desc) {
  if (!set_handle) {
    return 0;
  }
  if (count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
    auto set_vals = reinterpret_cast<int8_t*>(set_handle);
    if (count_distinct_desc.approximate) {
      CHECK_GT(count_distinct_desc.bitmap_sz_bits, 0);
      return hll_size(reinterpret_cast<const int8_t*>(set_vals),
                      count_distinct_desc.bitmap_sz_bits);
    }
    if (count_distinct_desc.sub_bitmap_count > 1) {
      partial_bitmap_union(set_vals, count_distinct_desc);
    }
    return bitmap_set_size(set_vals, count_distinct_desc.bitmapSizeBytes());
  }
  CHECK(count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
  return reinterpret_cast<robin_hood::unordered_set<int64_t>*>(set_handle)->size();
}

inline void count_distinct_set_union(
    const int64_t new_set_handle,
    const int64_t old_set_handle,
    const CountDistinctDescriptor& new_count_distinct_desc,
    const CountDistinctDescriptor& old_count_distinct_desc) {
  if (new_count_distinct_desc.impl_type_ == CountDistinctImplType::Bitmap) {
    auto new_set = reinterpret_cast<int8_t*>(new_set_handle);
    auto old_set = reinterpret_cast<int8_t*>(old_set_handle);
    if (new_count_distinct_desc.approximate) {
      CHECK(old_count_distinct_desc.approximate);
      hll_unify(reinterpret_cast<int8_t*>(new_set),
                reinterpret_cast<int8_t*>(old_set),
                1 << old_count_distinct_desc.bitmap_sz_bits);
    } else {
      CHECK_EQ(new_count_distinct_desc.sub_bitmap_count,
               old_count_distinct_desc.sub_bitmap_count);
      CHECK_GE(old_count_distinct_desc.sub_bitmap_count, size_t(1));
      // NB: For low cardinality input. Treat them as if they are regular
      // bitmaps and let count_distinct_set_size take care of additional reduction.
      const auto bitmap_byte_sz = old_count_distinct_desc.sub_bitmap_count == 1
                                      ? old_count_distinct_desc.bitmapSizeBytes()
                                      : old_count_distinct_desc.bitmapPaddedSizeBytes();
      bitmap_set_union(new_set, old_set, bitmap_byte_sz);
    }
  } else {
    CHECK(old_count_distinct_desc.impl_type_ == CountDistinctImplType::HashSet);
    auto old_set = reinterpret_cast<robin_hood::unordered_set<int64_t>*>(old_set_handle);
    auto new_set = reinterpret_cast<robin_hood::unordered_set<int64_t>*>(new_set_handle);
    new_set->insert(old_set->begin(), old_set->end());
  }
}

#endif
