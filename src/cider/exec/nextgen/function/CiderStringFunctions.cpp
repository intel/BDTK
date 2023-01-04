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

#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/context/StringHeap.h"
#include "util/DateTimeParser.h"
#include "util/misc.h"

ALWAYS_INLINE uint64_t pack_string(const int8_t* ptr, const int32_t len) {
  return (reinterpret_cast<const uint64_t>(ptr) & 0xffffffffffff) |
         (static_cast<const uint64_t>(len) << 48);
}

ALWAYS_INLINE uint64_t pack_string_t(const string_t& s) {
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

// not in use.
extern "C" RUNTIME_EXPORT int64_t cider_substring(const char* str, int pos, int len) {
  const char* ret_ptr = str + pos - 1;
  return pack_string((const int8_t*)ret_ptr, (const int32_t)len);
}

// pos parameter starts from 1 rather than 0
extern "C" RUNTIME_EXPORT int64_t cider_substring_extra(char* string_heap_ptr,
                                                        const char* str,
                                                        int pos,
                                                        int len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str + pos - 1, len);
  return pack_string_t(s);
}

// pos starts with 1. A negative starting position is interpreted as being relative
// to the end of the string
extern "C" RUNTIME_EXPORT int32_t format_substring_pos(int pos, int str_len) {
  int32_t ret = 1;
  if (pos > 0) {
    if (pos > str_len) {
      ret = str_len + 1;
    } else {
      ret = pos;
    }
  } else if (pos < 0) {
    if (pos + str_len >= 0) {
      ret = str_len + pos + 1;
    }
  }
  return ret;
}

// pos should be [1, str_len+1]
extern "C" RUNTIME_EXPORT int32_t format_substring_len(int pos,
                                                       int str_len,
                                                       int target_len) {
  // already out of range, return empty string.
  if (pos == str_len + 1) {
    return 0;
  }
  // not reach to max str length, return target length
  if (pos + target_len <= str_len + 1) {
    return target_len;
  } else {
    // reach to max str length
    return str_len - pos + 1;
  }
}

// a copy of extract_str_ptr (originally implemented in RuntimeFunctions.cpp)
extern "C" ALWAYS_INLINE int8_t* extract_string_ptr(const uint64_t str_and_len) {
  return reinterpret_cast<int8_t*>(str_and_len & 0xffffffffffff);
}

extern "C" ALWAYS_INLINE int32_t extract_string_len(const uint64_t str_and_len) {
  return static_cast<int64_t>(str_and_len) >> 48;
}

/// NOTE: (YBRua) referenced DuckDb's case conversion implementation,
/// which uses lookup tables for ascii case conversion (probably for leveraging cache?)
const uint8_t ascii_char_upper_map[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
    16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
    32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
    48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
    64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
    80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
    96,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
    80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  123, 124, 125, 126, 127,
    128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
    144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
    192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
    208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
    224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};
const uint8_t ascii_char_lower_map[] = {
    0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
    16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
    32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
    48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
    64,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
    112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 91,  92,  93,  94,  95,
    96,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
    112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127,
    128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
    144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
    192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
    208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
    224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};

extern "C" ALWAYS_INLINE int64_t cider_ascii_lower(int8_t* string_heap_ptr,
                                                   const char* str,
                                                   int str_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(str_len);
  char* sout = s.getDataWriteable();
  for (int i = 0; i < str_len; ++i) {
    sout[i] = ascii_char_lower_map[reinterpret_cast<const uint8_t*>(str)[i]];
  }
  return pack_string_t(s);
}

extern "C" ALWAYS_INLINE int64_t cider_ascii_upper(int8_t* string_heap_ptr,
                                                   const char* str,
                                                   int str_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(str_len);
  char* sout = s.getDataWriteable();
  for (int i = 0; i < str_len; ++i) {
    sout[i] = ascii_char_upper_map[reinterpret_cast<const uint8_t*>(str)[i]];
  }
  return pack_string_t(s);
}

extern "C" void test_to_string(int value) {
  std::printf("test_to_string: %s\n", std::to_string(value).c_str());
}

extern "C" ALWAYS_INLINE int64_t cider_concat(char* string_heap_ptr,
                                              const char* lhs,
                                              int lhs_len,
                                              const char* rhs,
                                              int rhs_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(lhs_len + rhs_len);

  char* buffer_ptr = s.getDataWriteable();
  memcpy(buffer_ptr, lhs, lhs_len);
  memcpy(buffer_ptr + lhs_len, rhs, rhs_len);

  return pack_string_t(s);
}

// to be deprecated.
// rconcat is only used for backward compatibility with template codegen, which only
// supports cases where the first arg is a variable.
// for concat ops like "constant || var", it will be converted to "var || constant" and
// then concatenated in the REVERSED order (RCONCAT).
// However, nextgen allows both arguments to be variables, so this function can be removed
// after full migration to nextgen
extern "C" ALWAYS_INLINE int64_t cider_rconcat(char* string_heap_ptr,
                                               const char* lhs,
                                               int lhs_len,
                                               const char* rhs,
                                               int rhs_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(lhs_len + rhs_len);

  char* buffer_ptr = s.getDataWriteable();
  memcpy(buffer_ptr, rhs, rhs_len);
  memcpy(buffer_ptr + rhs_len, lhs, lhs_len);

  return pack_string_t(s);
}

extern "C" ALWAYS_INLINE int8_t* get_data_buffer_with_realloc_on_demand(
    const int8_t* input_desc_ptr,
    const int32_t current_bytes) {
  const ArrowArray* arrow_array = reinterpret_cast<const ArrowArray*>(input_desc_ptr);
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(arrow_array->private_data);

  // assumes arrow_array is an array for var-size binary (with 3 buffers)
  size_t capacity = holder->getBufferSizeAt(2);
  if (capacity == 0) {
    // initialize buffer with a capacity of 4096 bytes
    holder->allocBuffer(2, 4096);
  } else if (current_bytes >= 0.9 * capacity) {
    // double capacity if current bytes take up 90% of capacity
    // assumes we would have enough space for next input after at most one resize op
    holder->allocBuffer(2, capacity * 2);
  }

  return holder->getBufferAs<int8_t>(2);
}

extern "C" ALWAYS_INLINE int64_t cider_trim(char* string_heap_ptr,
                                            const char* str_ptr,
                                            int str_len,
                                            const int8_t* trim_char_map,
                                            bool ltrim,
                                            bool rtrim) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);

  int start_idx = 0;
  if (ltrim) {
    while (start_idx < str_len &&
           trim_char_map[reinterpret_cast<const uint8_t*>(str_ptr)[start_idx]]) {
      start_idx++;
    }
  }

  int end_idx = str_len - 1;
  if (rtrim) {
    while (end_idx >= start_idx &&
           trim_char_map[reinterpret_cast<const uint8_t*>(str_ptr)[end_idx]]) {
      end_idx--;
    }
  }

  int len = 0;
  if (start_idx > end_idx) {
    // all chars are trimmed away, return an empty string
    len = 0;
  } else {
    len = end_idx - start_idx + 1;
  }

  string_t s = ptr->addString(str_ptr + start_idx, len);
  return pack_string_t(s);
}

#define DEF_CONVERT_INTEGER_TO_STRING(value_type, value_name)                         \
  extern "C" RUNTIME_EXPORT int64_t gen_string_from_##value_name(                     \
      const value_type operand, char* string_heap_ptr) {                              \
    std::string_view str = std::to_string(operand);                                   \
    StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);                 \
    string_t s = ptr->addString(str.data(), str.length());                            \
    return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize()); \
  }
DEF_CONVERT_INTEGER_TO_STRING(int8_t, tinyint)
DEF_CONVERT_INTEGER_TO_STRING(int16_t, smallint)
DEF_CONVERT_INTEGER_TO_STRING(int32_t, int)
DEF_CONVERT_INTEGER_TO_STRING(int64_t, bigint)
#undef DEF_CONVERT_INTEGER_TO_STRING

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t
gen_string_from_float(const float operand, char* string_heap_ptr) {
  std::string str = fmt::format("{:#}", operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_EXPORT NEVER_INLINE int64_t
gen_string_from_double(const double operand, char* string_heap_ptr) {
  std::string str = fmt::format("{:#}", operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
gen_string_from_bool(const int8_t operand, char* string_heap_ptr) {
  std::string str = (operand == 1) ? "true" : "false";
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
gen_string_from_time(const int64_t operand,
                     char* string_heap_ptr,
                     const int32_t dimension) {
  constexpr size_t buf_size = 64;
  char buf[buf_size];
  int32_t str_len = shared::formatHMS(buf, buf_size, operand, dimension);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(buf, str_len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
gen_string_from_timestamp(const int64_t operand,
                          char* string_heap_ptr,
                          const int32_t dimension) {
  constexpr size_t buf_size = 64;
  char buf[buf_size];
  int32_t str_len = shared::formatDateTime(buf, buf_size, operand, dimension);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(buf, str_len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
gen_string_from_date(const int32_t operand, char* string_heap_ptr) {
  constexpr size_t buf_size = 64;
  char buf[buf_size];
  int32_t str_len = shared::formatDays(buf, buf_size, operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(buf, str_len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

#define DEF_CONVERT_STRING_TO_INTEGER(value_type, value_name)          \
  extern "C" RUNTIME_EXPORT value_type convert_string_to_##value_name( \
      const char* str_ptr, const int32_t str_len) {                    \
    std::string from_str(str_ptr, str_len);                            \
    try {                                                              \
      value_type res = std::stoi(from_str);                            \
      if (res >= std::numeric_limits<value_type>::min() &&             \
          res <= std::numeric_limits<value_type>::max())               \
        return res;                                                    \
    } catch (std::exception & err) {                                   \
      CIDER_THROW(CiderRuntimeException, err.what());                  \
    }                                                                  \
    CIDER_THROW(CiderRuntimeException,                                 \
                "runtime error:cast from string to " #value_type);     \
  }

DEF_CONVERT_STRING_TO_INTEGER(int8_t, tinyint)
DEF_CONVERT_STRING_TO_INTEGER(int16_t, smallint)
DEF_CONVERT_STRING_TO_INTEGER(int32_t, int)
DEF_CONVERT_STRING_TO_INTEGER(int64_t, bigint)

#undef DEF_CONVERT_STRING_TO_INTEGER

extern "C" RUNTIME_EXPORT ALWAYS_INLINE float convert_string_to_float(
    const char* str_ptr,
    const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return std::stof(from_str);
}

extern "C" RUNTIME_EXPORT int8_t convert_string_to_bool(const char* str_ptr,
                                                        const int32_t str_len) {
  std::string s(str_ptr, str_len);
  if (s == "t" || s == "T" || s == "1" || to_upper(std::string(s)) == "TRUE") {
    return 1;
  } else if (s == "f" || s == "F" || s == "0" || to_upper(std::string(s)) == "FALSE") {
    return 0;
  }
  CIDER_THROW(CiderRuntimeException, "cast from string to bool runtime error");
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE double convert_string_to_double(
    const char* str_ptr,
    const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return std::stod(from_str);
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int32_t
convert_string_to_date(const char* str_ptr, const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return parseDateInDays(from_str);
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
convert_string_to_timestamp(const char* str_ptr,
                            const int32_t str_len,
                            const int32_t dim) {
  std::string from_str(str_ptr, str_len);
  return dateTimeParse<kTIMESTAMP>(from_str, dim);
}

extern "C" RUNTIME_EXPORT ALWAYS_INLINE int64_t
convert_string_to_time(const char* str_ptr, const int32_t str_len, const int32_t dim) {
  std::string from_str(str_ptr, str_len);
  return dateTimeParse<kTIME>(from_str, dim);
}
