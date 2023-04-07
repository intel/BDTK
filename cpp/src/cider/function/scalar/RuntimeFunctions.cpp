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

#include "exec/module/batch/ArrowABI.h"
#include "type/data/funcannotations.h"
#include "util/CiderBitUtils.h"

#include "exec/nextgen/context/ContextRuntimeFunctions.h"
#include "exec/nextgen/function/CiderDateFunctions.cpp"
#include "exec/nextgen/function/CiderSetFunctions.cpp"
#include "exec/nextgen/function/CiderStringFunctions.cpp"
#include "exec/nextgen/operators/OperatorRuntimeFunctions.h"

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t external_call_test_sum(int32_t a,
                                                                    int32_t b) {
  return a + b;
}

// Return floor(dividend / divisor).
// Assumes 0 < divisor.
extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t floor_div_lhs(const int64_t dividend,
                                                           const int64_t divisor) {
  return (dividend < 0 ? dividend - (divisor - 1) : dividend) / divisor;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t decimal_floor(const int64_t x,
                                                           const int64_t scale) {
  if (x >= 0) {
    return x / scale * scale;
  }
  if (!(x % scale)) {
    return x;
  }
  return x / scale * scale - scale;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t decimal_ceil(const int64_t x,
                                                          const int64_t scale) {
  return decimal_floor(x, scale) + (x % scale ? scale : 0);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE bool check_dictionary_is_null(int8_t* dictionary) {
  bool ret = dictionary == nullptr;
  return ret;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int get_str_length_from_dictionary(
    int8_t* dictionary,
    uint64_t index) {
  const int32_t* offset_buffer = reinterpret_cast<const int32_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
  return offset_buffer[index + 1] - offset_buffer[index];
}

extern "C" RUNTIME_FUNC ALLOW_INLINE const int8_t* get_str_ptr_from_dictionary(
    int8_t* dictionary,
    uint64_t index) {
  const int32_t* offset_buffer = reinterpret_cast<const int32_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
  const int8_t* data_buffer = reinterpret_cast<const int8_t*>(
      reinterpret_cast<ArrowArray*>(dictionary)->buffers[2]);

  return data_buffer + offset_buffer[index];
}

extern "C" RUNTIME_FUNC ALLOW_INLINE const int32_t
get_str_length_from_dictionary_or_buffer(int8_t* dictionary,
                                         uint64_t index,
                                         int32_t* offset_buffer) {
  if (dictionary) {
    const int32_t* actual_offset_buffer = reinterpret_cast<const int32_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
    int32_t index_in_dict = offset_buffer[index];
    int len =
        actual_offset_buffer[index_in_dict + 1] - actual_offset_buffer[index_in_dict];
    return len;
  } else {
    return offset_buffer[index + 1] - offset_buffer[index];
  }
}

extern "C" RUNTIME_FUNC ALLOW_INLINE const int8_t* get_str_ptr_from_dictionary_or_buffer(
    int8_t* dictionary,
    uint64_t index,
    int32_t* offset_buffer,
    int8_t* data_buffer) {
  if (dictionary) {
    int32_t index_in_dict = offset_buffer[index];
    const int32_t* actual_offset_buffer = reinterpret_cast<const int32_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[1]);
    const int8_t* actual_data_buffer = reinterpret_cast<const int8_t*>(
        reinterpret_cast<ArrowArray*>(dictionary)->buffers[2]);
    return actual_data_buffer + actual_offset_buffer[index_in_dict];
  } else {
    return data_buffer + offset_buffer[index];
  }
}

extern "C" RUNTIME_FUNC ALLOW_INLINE bool check_bit_vector_set(uint8_t* bit_vector,
                                                               uint64_t index) {
  return CiderBitUtils::isBitSetAt(bit_vector, index);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE bool check_bit_vector_clear(uint8_t* bit_vector,
                                                                 uint64_t index) {
  bool ans = !CiderBitUtils::isBitSetAt(bit_vector, index);
  return ans;
}
extern "C" RUNTIME_FUNC ALLOW_INLINE bool check_bit_vector_clear_opt(uint8_t* bit_vector,
                                                                     uint64_t index) {
  return CiderBitUtils::isBitClearAt(bit_vector, index);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void set_bit_vector(uint8_t* bit_vector,
                                                         uint64_t index) {
  CiderBitUtils::setBitAt(bit_vector, index);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void clear_bit_vector(uint8_t* bit_vector,
                                                           uint64_t index) {
  CiderBitUtils::clearBitAt(bit_vector, index);
}

// For temporary use
extern "C" RUNTIME_FUNC ALLOW_INLINE void set_null_vector_bit(uint8_t* bit_vector,
                                                              uint64_t index,
                                                              bool is_null) {
  is_null ? CiderBitUtils::clearBitAt(bit_vector, index)
          : CiderBitUtils::setBitAt(bit_vector, index);
}
extern "C" RUNTIME_FUNC ALLOW_INLINE void set_null_vector_bit_opt(uint8_t* bit_vector,
                                                                  uint64_t index,
                                                                  bool is_null) {
  CiderBitUtils::setBitAtUnified(bit_vector, index, is_null);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void do_memcpy(int8_t* dst,
                                                    int8_t* src,
                                                    int32_t len) {
  memcpy(dst, src, len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void bitwise_and_2(uint8_t* output,
                                                        const uint8_t* a,
                                                        const uint8_t* b,
                                                        uint64_t bit_num) {
  CiderBitUtils::bitwiseAnd(output, a, b, bit_num);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void null_buffer_memcpy(int8_t* dst,
                                                             int8_t* src,
                                                             int64_t bit_num) {
  memcpy(dst, src, (bit_num + 7) >> 3);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* extract_str_ptr_arrow(int8_t* data_buffer,
                                                                   int8_t* offset_buffer,
                                                                   uint64_t pos) {
  return (data_buffer + reinterpret_cast<int32_t*>(offset_buffer)[pos]);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t extract_str_len_arrow(int8_t* offset_buffer,
                                                                   uint64_t pos) {
  int32_t* offset = reinterpret_cast<int32_t*>(offset_buffer);
  return offset[pos + 1] - offset[pos];
}
