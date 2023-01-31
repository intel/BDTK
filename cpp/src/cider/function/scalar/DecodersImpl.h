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
#ifndef CIDER_FUNCTION_DECODERSIMPL_H
#define CIDER_FUNCTION_DECODERSIMPL_H

#include <limits>

#include <cstdint>
#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "type/data/funcannotations.h"

extern "C" ALWAYS_INLINE int64_t SUFFIX(fixed_width_int_decode)(const int8_t* byte_stream,
                                                                const int32_t byte_width,
                                                                const int64_t pos) {
#ifdef WITH_DECODERS_BOUNDS_CHECKING
  assert(pos >= 0);
#endif  // WITH_DECODERS_BOUNDS_CHECKING
  switch (byte_width) {
    case 1:
      return static_cast<int64_t>(byte_stream[pos * byte_width]);
    case 2:
      return *(reinterpret_cast<const int16_t*>(&byte_stream[pos * byte_width]));
    case 4:
      return *(reinterpret_cast<const int32_t*>(&byte_stream[pos * byte_width]));
    case 8:
      return *(reinterpret_cast<const int64_t*>(&byte_stream[pos * byte_width]));
    default:
// TODO(alex)
#ifdef _WIN32
      return LLONG_MIN + 1;
#else
      return std::numeric_limits<int64_t>::min() + 1;
#endif
  }
}

extern "C" ALWAYS_INLINE int64_t
SUFFIX(fixed_width_unsigned_decode)(const int8_t* byte_stream,
                                    const int32_t byte_width,
                                    const int64_t pos) {
#ifdef WITH_DECODERS_BOUNDS_CHECKING
  assert(pos >= 0);
#endif  // WITH_DECODERS_BOUNDS_CHECKING
  switch (byte_width) {
    case 1:
      return reinterpret_cast<const uint8_t*>(byte_stream)[pos * byte_width];
    case 2:
      return *(reinterpret_cast<const uint16_t*>(&byte_stream[pos * byte_width]));
    case 4:
      return *(reinterpret_cast<const uint32_t*>(&byte_stream[pos * byte_width]));
    case 8:
      return *(reinterpret_cast<const uint64_t*>(&byte_stream[pos * byte_width]));
    default:
// TODO(alex)
#ifdef _WIN32
      return LLONG_MIN + 1;
#else
      return std::numeric_limits<int64_t>::min() + 1;
#endif
  }
}

extern "C" NEVER_INLINE int64_t
SUFFIX(fixed_width_int_decode_noinline)(const int8_t* byte_stream,
                                        const int32_t byte_width,
                                        const int64_t pos) {
  return SUFFIX(fixed_width_int_decode)(byte_stream, byte_width, pos);
}

extern "C" NEVER_INLINE int64_t
SUFFIX(fixed_width_unsigned_decode_noinline)(const int8_t* byte_stream,
                                             const int32_t byte_width,
                                             const int64_t pos) {
  return SUFFIX(fixed_width_unsigned_decode)(byte_stream, byte_width, pos);
}

extern "C" ALWAYS_INLINE int64_t
SUFFIX(diff_fixed_width_int_decode)(const int8_t* byte_stream,
                                    const int32_t byte_width,
                                    const int64_t baseline,
                                    const int64_t pos) {
  return SUFFIX(fixed_width_int_decode)(byte_stream, byte_width, pos) + baseline;
}

extern "C" ALWAYS_INLINE float SUFFIX(fixed_width_float_decode)(const int8_t* byte_stream,
                                                                const int64_t pos) {
#ifdef WITH_DECODERS_BOUNDS_CHECKING
  assert(pos >= 0);
#endif  // WITH_DECODERS_BOUNDS_CHECKING
  return *(reinterpret_cast<const float*>(&byte_stream[pos * sizeof(float)]));
}

extern "C" NEVER_INLINE float SUFFIX(
    fixed_width_float_decode_noinline)(const int8_t* byte_stream, const int64_t pos) {
  return SUFFIX(fixed_width_float_decode)(byte_stream, pos);
}

extern "C" ALWAYS_INLINE double SUFFIX(
    fixed_width_double_decode)(const int8_t* byte_stream, const int64_t pos) {
#ifdef WITH_DECODERS_BOUNDS_CHECKING
  assert(pos >= 0);
#endif  // WITH_DECODERS_BOUNDS_CHECKING
  return *(reinterpret_cast<const double*>(&byte_stream[pos * sizeof(double)]));
}

extern "C" NEVER_INLINE double SUFFIX(
    fixed_width_double_decode_noinline)(const int8_t* byte_stream, const int64_t pos) {
  return SUFFIX(fixed_width_double_decode)(byte_stream, pos);
}

extern "C" ALWAYS_INLINE int64_t
SUFFIX(fixed_width_small_date_decode)(const int8_t* byte_stream,
                                      const int32_t byte_width,
                                      const int32_t null_val,
                                      const int64_t ret_null_val,
                                      const int64_t pos) {
  auto val = SUFFIX(fixed_width_int_decode)(byte_stream, byte_width, pos);
  return val == null_val ? ret_null_val : val * 86400;
}

extern "C" NEVER_INLINE int64_t
SUFFIX(fixed_width_small_date_decode_noinline)(const int8_t* byte_stream,
                                               const int32_t byte_width,
                                               const int32_t null_val,
                                               const int64_t ret_null_val,
                                               const int64_t pos) {
  return SUFFIX(fixed_width_small_date_decode)(
      byte_stream, byte_width, null_val, ret_null_val, pos);
}

#undef SUFFIX

extern "C" ALWAYS_INLINE const uint8_t* cider_ColDecoder_extractArrowBuffersAt(
    const int8_t* input_descriptor_ptr,
    const int64_t index) {
  const ArrowArray* ptr = reinterpret_cast<const ArrowArray*>(input_descriptor_ptr);
  return reinterpret_cast<const uint8_t*>(ptr->buffers[index]);
}

extern "C" ALWAYS_INLINE void reallocate_string_buffer_if_need(
    const int8_t* input_desc_ptr,
    const int64_t pos) {
  // assumption: this arrow array is already initialized outside(it has 3 buffers, length
  // is set)
  const ArrowArray* ptr = reinterpret_cast<const ArrowArray*>(input_desc_ptr);
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(ptr->private_data);
  int32_t* offset_buffer = (int32_t*)ptr->buffers[1];
  size_t capacity = holder->getBufferSizeAt(2);
  if (capacity == 0) {
    holder->allocBuffer(2, 4096);
  } else if (offset_buffer[pos] >
             capacity * 0.9) {  // do reallocate when reach 90% of capacity
    holder->allocBuffer(2, capacity * 2);
  }
}
#endif  // CIDER_FUNCTION_DECODERSIMPL_H
