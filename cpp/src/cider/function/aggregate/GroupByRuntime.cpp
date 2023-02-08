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

#include "exec/operator/aggregate/CiderAggHashTable.h"
#include "exec/template/operator/join/hashtable/runtime/JoinHashImpl.h"
#include "function/hash/MurmurHash.h"

extern "C" RUNTIME_EXPORT ALWAYS_INLINE uint32_t key_hash(const int64_t* key,
                                                          const uint32_t key_count,
                                                          const uint32_t key_byte_width) {
  return MurmurHash3(key, key_byte_width * key_count, 0);
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t* get_group_value(
    int64_t* groups_buffer,
    const uint32_t groups_buffer_entry_count,
    const int64_t* key,
    const uint32_t key_count,
    const uint32_t key_width,
    const uint32_t row_size_quad) {
  uint32_t h = key_hash(key, key_count, key_width) % groups_buffer_entry_count;
  int64_t* matching_group = get_matching_group_value(
      groups_buffer, h, key, key_count, key_width, row_size_quad);
  if (matching_group) {
    return matching_group;
  }
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_group = get_matching_group_value(
        groups_buffer, h_probe, key, key_count, key_width, row_size_quad);
    if (matching_group) {
      return matching_group;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
  }
  return NULL;
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t* get_group_value_cider(
    int64_t* agg_hash_table_ptr,
    const uint32_t groups_buffer_entry_count,
    const int64_t* key,
    const uint32_t key_count,
    const uint32_t key_width,
    const uint32_t row_size_quad) {
  CiderAggHashTable* agg_hash_table =
      reinterpret_cast<CiderAggHashTable*>(agg_hash_table_ptr);
  return agg_hash_table->getGroupTargetPtr(key);
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
cider_get_string_id(int64_t* agg_hashtable_ptr, const uint8_t* ptr, uint32_t len) {
  CiderAggHashTable* agg_hash_table =
      reinterpret_cast<CiderAggHashTable*>(agg_hashtable_ptr);
  CiderByteArray str(len, ptr);
  return agg_hash_table->getStringHasher().lookupIdByValue(str);
}

extern "C" ALWAYS_INLINE void set_group_key_slot_int32(int32_t* value_vector,
                                                       uint8_t* null_vector,
                                                       size_t index,
                                                       int32_t value,
                                                       bool is_null) {
  if (is_null) {
    value_vector[index] = 0;
  } else {
    value_vector[index] = value;
    CiderBitUtils::setBitAt(null_vector, index);
  }
}

extern "C" ALWAYS_INLINE void set_group_key_slot_int64(int64_t* value_vector,
                                                       uint8_t* null_vector,
                                                       size_t index,
                                                       int64_t value,
                                                       bool is_null) {
  if (is_null) {
    value_vector[index] = 0;
  } else {
    value_vector[index] = value;
    CiderBitUtils::setBitAt(null_vector, index);
  }
}

extern "C" RUNTIME_EXPORT NEVER_INLINE bool dynamic_watchdog();

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t* get_group_value_with_watchdog(
    int64_t* groups_buffer,
    const uint32_t groups_buffer_entry_count,
    const int64_t* key,
    const uint32_t key_count,
    const uint32_t key_width,
    const uint32_t row_size_quad) {
  uint32_t h = key_hash(key, key_count, key_width) % groups_buffer_entry_count;
  int64_t* matching_group = get_matching_group_value(
      groups_buffer, h, key, key_count, key_width, row_size_quad);
  if (matching_group) {
    return matching_group;
  }
  uint32_t watchdog_countdown = 100;
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_group = get_matching_group_value(
        groups_buffer, h_probe, key, key_count, key_width, row_size_quad);
    if (matching_group) {
      return matching_group;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
    if (--watchdog_countdown == 0) {
      if (dynamic_watchdog()) {
        return NULL;
      }
      watchdog_countdown = 100;
    }
  }
  return NULL;
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int32_t
get_group_value_columnar_slot(int64_t* groups_buffer,
                              const uint32_t groups_buffer_entry_count,
                              const int64_t* key,
                              const uint32_t key_count,
                              const uint32_t key_width) {
  uint32_t h = key_hash(key, key_count, key_width) % groups_buffer_entry_count;
  int32_t matching_slot = get_matching_group_value_columnar_slot(
      groups_buffer, groups_buffer_entry_count, h, key, key_count, key_width);
  if (matching_slot != -1) {
    return h;
  }
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_slot = get_matching_group_value_columnar_slot(
        groups_buffer, groups_buffer_entry_count, h_probe, key, key_count, key_width);
    if (matching_slot != -1) {
      return h_probe;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
  }
  return -1;
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int32_t
get_group_value_columnar_slot_with_watchdog(int64_t* groups_buffer,
                                            const uint32_t groups_buffer_entry_count,
                                            const int64_t* key,
                                            const uint32_t key_count,
                                            const uint32_t key_width) {
  uint32_t h = key_hash(key, key_count, key_width) % groups_buffer_entry_count;
  int32_t matching_slot = get_matching_group_value_columnar_slot(
      groups_buffer, groups_buffer_entry_count, h, key, key_count, key_width);
  if (matching_slot != -1) {
    return h;
  }
  uint32_t watchdog_countdown = 100;
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_slot = get_matching_group_value_columnar_slot(
        groups_buffer, groups_buffer_entry_count, h_probe, key, key_count, key_width);
    if (matching_slot != -1) {
      return h_probe;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
    if (--watchdog_countdown == 0) {
      if (dynamic_watchdog()) {
        return -1;
      }
      watchdog_countdown = 100;
    }
  }
  return -1;
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t* get_group_value_columnar(
    int64_t* groups_buffer,
    const uint32_t groups_buffer_entry_count,
    const int64_t* key,
    const uint32_t key_qw_count) {
  uint32_t h = key_hash(key, key_qw_count, sizeof(int64_t)) % groups_buffer_entry_count;
  int64_t* matching_group = get_matching_group_value_columnar(
      groups_buffer, h, key, key_qw_count, groups_buffer_entry_count);
  if (matching_group) {
    return matching_group;
  }
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_group = get_matching_group_value_columnar(
        groups_buffer, h_probe, key, key_qw_count, groups_buffer_entry_count);
    if (matching_group) {
      return matching_group;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
  }
  return NULL;
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t* get_group_value_columnar_with_watchdog(
    int64_t* groups_buffer,
    const uint32_t groups_buffer_entry_count,
    const int64_t* key,
    const uint32_t key_qw_count) {
  uint32_t h = key_hash(key, key_qw_count, sizeof(int64_t)) % groups_buffer_entry_count;
  int64_t* matching_group = get_matching_group_value_columnar(
      groups_buffer, h, key, key_qw_count, groups_buffer_entry_count);
  if (matching_group) {
    return matching_group;
  }
  uint32_t watchdog_countdown = 100;
  uint32_t h_probe = (h + 1) % groups_buffer_entry_count;
  while (h_probe != h) {
    matching_group = get_matching_group_value_columnar(
        groups_buffer, h_probe, key, key_qw_count, groups_buffer_entry_count);
    if (matching_group) {
      return matching_group;
    }
    h_probe = (h_probe + 1) % groups_buffer_entry_count;
    if (--watchdog_countdown == 0) {
      if (dynamic_watchdog()) {
        return NULL;
      }
      watchdog_countdown = 100;
    }
  }
  return NULL;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t* get_group_value_fast(
    int64_t* groups_buffer,
    const int64_t key,
    const int64_t min_key,
    const int64_t bucket,
    const uint32_t row_size_quad) {
  int64_t key_diff = key - min_key;
  if (bucket) {
    key_diff /= bucket;
  }
  int64_t off = key_diff * row_size_quad;
  if (groups_buffer[off] == empty_key_64) {
    groups_buffer[off] = key;
  }
  return groups_buffer + off + 1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t* get_group_value_fast_with_original_key(
    int64_t* groups_buffer,
    const int64_t key,
    const int64_t orig_key,
    const int64_t min_key,
    const int64_t bucket,
    const uint32_t row_size_quad) {
  int64_t key_diff = key - min_key;
  if (bucket) {
    key_diff /= bucket;
  }
  int64_t off = key_diff * row_size_quad;
  if (groups_buffer[off] == empty_key_64) {
    groups_buffer[off] = orig_key;
  }
  return groups_buffer + off + 1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE uint32_t
get_columnar_group_bin_offset(int64_t* key_base_ptr,
                              const int64_t key,
                              const int64_t min_key,
                              const int64_t bucket) {
  int64_t off = key - min_key;
  if (bucket) {
    off /= bucket;
  }
  if (key_base_ptr[off] == empty_key_64) {
    key_base_ptr[off] = key;
  }
  return off;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t* get_scan_output_slot(
    int64_t* output_buffer,
    const uint32_t output_buffer_entry_count,
    const uint32_t pos,
    const int64_t offset_in_fragment,
    const uint32_t row_size_quad) {
  uint64_t off = static_cast<uint64_t>(pos) * static_cast<uint64_t>(row_size_quad);
  if (pos < output_buffer_entry_count) {
    output_buffer[off] = offset_in_fragment;
    return output_buffer + off + 1;
  }
  return NULL;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int32_t
get_columnar_scan_output_offset(int64_t* output_buffer,
                                const uint32_t output_buffer_entry_count,
                                const uint32_t pos,
                                const int64_t offset_in_fragment) {
  if (pos < output_buffer_entry_count) {
    output_buffer[pos] = offset_in_fragment;
    return pos;
  }
  return -1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
bucketized_hash_join_idx(int64_t hash_buff,
                         int64_t const key,
                         int64_t const min_key,
                         int64_t const max_key,
                         int64_t bucket_normalization) {
  if (key >= min_key && key <= max_key) {
    return *SUFFIX(get_bucketized_hash_slot)(
        reinterpret_cast<int32_t*>(hash_buff), key, min_key, bucket_normalization);
  }
  return -1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t hash_join_idx(int64_t hash_buff,
                                                              const int64_t key,
                                                              const int64_t min_key,
                                                              const int64_t max_key) {
  if (key >= min_key && key <= max_key) {
    return *SUFFIX(get_hash_slot)(reinterpret_cast<int32_t*>(hash_buff), key, min_key);
  }
  return -1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
bucketized_hash_join_idx_nullable(int64_t hash_buff,
                                  const int64_t key,
                                  const int64_t min_key,
                                  const int64_t max_key,
                                  const int64_t null_val,
                                  const int64_t bucket_normalization) {
  return key != null_val ? bucketized_hash_join_idx(
                               hash_buff, key, min_key, max_key, bucket_normalization)
                         : -1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
hash_join_idx_nullable(int64_t hash_buff,
                       const int64_t key,
                       const int64_t min_key,
                       const int64_t max_key,
                       const int64_t null_val) {
  return key != null_val ? hash_join_idx(hash_buff, key, min_key, max_key) : -1;
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
hash_join_idx_nullable_cider(int64_t hash_buff,
                             const int64_t key,
                             const int64_t min_key,
                             const int64_t max_key,
                             const bool is_null) {
  if (is_null) {
    return -1;
  } else {
    return hash_join_idx(hash_buff, key, min_key, max_key);
  }
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
bucketized_hash_join_idx_bitwise(int64_t hash_buff,
                                 const int64_t key,
                                 const int64_t min_key,
                                 const int64_t max_key,
                                 const int64_t null_val,
                                 const int64_t translated_val,
                                 const int64_t bucket_normalization) {
  return key != null_val ? bucketized_hash_join_idx(
                               hash_buff, key, min_key, max_key, bucket_normalization)
                         : bucketized_hash_join_idx(hash_buff,
                                                    translated_val,
                                                    min_key,
                                                    translated_val,
                                                    bucket_normalization);
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
hash_join_idx_bitwise(int64_t hash_buff,
                      const int64_t key,
                      const int64_t min_key,
                      const int64_t max_key,
                      const int64_t null_val,
                      const int64_t translated_val) {
  return key != null_val
             ? hash_join_idx(hash_buff, key, min_key, max_key)
             : hash_join_idx(hash_buff, translated_val, min_key, translated_val);
}

#define DEF_TRANSLATE_NULL_KEY(key_type)                                           \
  extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t translate_null_key_##key_type(    \
      const key_type key, const key_type null_val, const int64_t translated_val) { \
    if (key == null_val) {                                                         \
      return translated_val;                                                       \
    }                                                                              \
    return key;                                                                    \
  }

DEF_TRANSLATE_NULL_KEY(int8_t)
DEF_TRANSLATE_NULL_KEY(int16_t)
DEF_TRANSLATE_NULL_KEY(int32_t)
DEF_TRANSLATE_NULL_KEY(int64_t)

#undef DEF_TRANSLATE_NULL_KEY
