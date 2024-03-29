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

#include "exec/nextgen/function/CiderStringFunction.h"

#include <re2/re2.h>
#include <algorithm>

#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/context/StringHeap.h"
#include "util/DateTimeParser.h"
#include "util/misc.h"

namespace {
uint64_t pack_string(const int8_t* ptr, const int32_t len) {
  return (reinterpret_cast<const uint64_t>(ptr) & 0xffffffffffff) |
         (static_cast<const uint64_t>(len) << 48);
}

uint64_t pack_string_t(const string_t& s) {
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

void do_lower(char* __restrict out, const char* __restrict in, int str_len) {
#pragma clang loop vectorize(enable)
  for (int i = 0; i < str_len; ++i) {
    if (in[i] >= 'A' && in[i] <= 'Z') {
      out[i] = in[i] + 0x20;
    } else {
      out[i] = in[i];
    }
  }
}

void do_upper(char* __restrict out, const char* __restrict in, int str_len) {
#pragma clang loop vectorize(enable)
  for (int i = 0; i < str_len; ++i) {
    if (in[i] >= 'a' && in[i] <= 'z') {
      out[i] = in[i] - 0x20;
    } else {
      out[i] = in[i];
    }
  }
}

static const size_t npos = -1;

std::pair<size_t, size_t> cider_find_nth_regex_match(const char* input_ptr,
                                                     int input_len,
                                                     const re2::StringPiece& pattern,
                                                     int start_pos,
                                                     int occurrence) {
  RE2 re(pattern);

  // record start_pos and length for each matched substring
  std::vector<std::pair<size_t, size_t>> matched_pos;
  int string_pos = start_pos;
  int matched_index = 0;
  while (string_pos < input_len) {
    re2::StringPiece submatch;
    re.Match(re2::StringPiece(input_ptr, input_len),
             string_pos,
             input_len,
             RE2::UNANCHORED,
             &submatch,
             1);

    if (submatch.data() == nullptr) {
      // not found
      break;
    } else {
      size_t matched_start_pos = submatch.data() - input_ptr;  // addr - addr
      matched_pos.push_back({matched_start_pos, submatch.size()});
      if (matched_index++ == occurrence) {
        return matched_pos.back();
      }
      string_pos = matched_start_pos + submatch.size();  // ??
    }
  }
  int wrapped_match = occurrence >= 0 ? occurrence : matched_index + occurrence;
  if (wrapped_match < 0 || wrapped_match >= matched_index) {
    return std::make_pair(npos, npos);
  }
  return matched_pos[wrapped_match];
}

int32_t StringCompare(const char* s1,
                      const int32_t s1_len,
                      const char* s2,
                      const int32_t s2_len) {
  const char* s1_ = s1;
  const char* s2_ = s2;

  while (s1_ < s1 + s1_len && s2_ < s2 + s2_len && *s1_ == *s2_) {
    s1_++;
    s2_++;
  }

  unsigned char c1 = (s1_ < s1 + s1_len) ? (*(unsigned char*)s1_) : 0;
  unsigned char c2 = (s2_ < s2 + s2_len) ? (*(unsigned char*)s2_) : 0;

  return c1 - c2;
}

enum LikeStatus {
  kLIKE_TRUE,
  kLIKE_FALSE,
  kLIKE_ABORT,  // means we run out of string characters to match against pattern, can
                // abort early
  kLIKE_ERROR   // error condition
};

static int inline lowercase(char c) {
  if ('A' <= c && c <= 'Z') {
    return 'a' + (c - 'A');
  }
  return c;
}

// internal recursive function for performing LIKE matching.
// when is_ilike is true, pattern is assumed to be already converted to all lowercase
static LikeStatus string_like_match(const char* str,
                                    const int32_t str_len,
                                    const char* pattern,
                                    const int32_t pat_len,
                                    const char escape_char,
                                    const bool is_ilike) {
  const char* s = str;
  int slen = str_len;
  const char* p = pattern;
  int plen = pat_len;

  while (slen > 0 && plen > 0) {
    if (*p == escape_char) {
      // next pattern char must match literally, whatever it is
      p++;
      plen--;
      if (plen <= 0) {
        return kLIKE_ERROR;
      }
      if ((!is_ilike && *s != *p) || (is_ilike && lowercase(*s) != *p)) {
        return kLIKE_FALSE;
      }
    } else if (*p == '%') {
      char firstpat;
      p++;
      plen--;
      while (plen > 0) {
        if (*p == '%') {
          p++;
          plen--;
        } else if (*p == '_') {
          if (slen <= 0) {
            return kLIKE_ABORT;
          }
          s++;
          slen--;
          p++;
          plen--;
        } else {
          break;
        }
      }
      if (plen <= 0) {
        return kLIKE_TRUE;
      }
      if (*p == escape_char) {
        if (plen < 2) {
          return kLIKE_ERROR;
        }
        firstpat = p[1];
      } else {
        firstpat = *p;
      }

      while (slen > 0) {
        bool match = false;
        if (firstpat == '[' && *p != escape_char) {
          const char* pp = p + 1;
          int pplen = plen - 1;
          while (pplen > 0 && *pp != ']') {
            if ((!is_ilike && *s == *pp) || (is_ilike && lowercase(*s) == *pp)) {
              match = true;
              break;
            }
            pp++;
            pplen--;
          }
          if (pplen <= 0) {
            return kLIKE_ERROR;  // malformed
          }
        } else if ((!is_ilike && *s == firstpat) ||
                   (is_ilike && lowercase(*s) == firstpat)) {
          match = true;
        }
        if (match) {
          LikeStatus status = string_like_match(s, slen, p, plen, escape_char, is_ilike);
          if (status != kLIKE_FALSE) {
            return status;
          }
        }
        s++;
        slen--;
      }
      return kLIKE_ABORT;
    } else if (*p == '_') {
      s++;
      slen--;
      p++;
      plen--;
      continue;
    } else if (*p == '[') {
      const char* pp = p + 1;
      int pplen = plen - 1;
      bool match = false;
      while (pplen > 0 && *pp != ']') {
        if ((!is_ilike && *s == *pp) || (is_ilike && lowercase(*s) == *pp)) {
          match = true;
          break;
        }
        pp++;
        pplen--;
      }
      if (match) {
        s++;
        slen--;
        pplen--;
        const char* x;
        for (x = pp + 1; *x != ']' && pplen > 0; x++, pplen--) {
        }
        if (pplen <= 0) {
          return kLIKE_ERROR;  // malformed
        }
        plen -= (x - p + 1);
        p = x + 1;
        continue;
      } else {
        return kLIKE_FALSE;
      }
    } else if ((!is_ilike && *s != *p) || (is_ilike && lowercase(*s) != *p)) {
      return kLIKE_FALSE;
    }
    s++;
    slen--;
    p++;
    plen--;
  }
  if (slen > 0) {
    return kLIKE_FALSE;
  }
  while (plen > 0 && *p == '%') {
    p++;
    plen--;
  }
  if (plen <= 0) {
    return kLIKE_TRUE;
  }
  return kLIKE_ABORT;
}
};  // namespace

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_like(const char* str,
                                                      const int32_t str_len,
                                                      const char* pattern,
                                                      const int32_t pat_len,
                                                      const char escape_char) {
  // @TODO(wei/alex) add runtime error handling
  LikeStatus status =
      string_like_match(str, str_len, pattern, pat_len, escape_char, false);
  return status == kLIKE_TRUE;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_lt(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) < 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_le(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) <= 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_gt(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) > 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_ge(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) >= 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_eq(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) == 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE bool string_ne(const char* lhs,
                                                    const int32_t lhs_len,
                                                    const char* rhs,
                                                    const int32_t rhs_len) {
  return StringCompare(lhs, lhs_len, rhs, rhs_len) != 0;
}

#define STR_CMP_NULLABLE(base_func)                                 \
  extern "C" RUNTIME_FUNC NEVER_INLINE int8_t base_func##_nullable( \
      const char* lhs,                                              \
      const int32_t lhs_len,                                        \
      const char* rhs,                                              \
      const int32_t rhs_len,                                        \
      const int8_t bool_null) {                                     \
    if (!lhs || !rhs) {                                             \
      return bool_null;                                             \
    }                                                               \
    return base_func(lhs, lhs_len, rhs, rhs_len) ? 1 : 0;           \
  }

STR_CMP_NULLABLE(string_lt)
STR_CMP_NULLABLE(string_le)
STR_CMP_NULLABLE(string_gt)
STR_CMP_NULLABLE(string_ge)
STR_CMP_NULLABLE(string_eq)
STR_CMP_NULLABLE(string_ne)

#undef STR_CMP_NULLABLE

// pos parameter starts from 1 rather than 0
extern "C" RUNTIME_FUNC ALLOW_INLINE uint64_t cider_substring_extra(char* string_heap_ptr,
                                                                    const char* str,
                                                                    int pos,
                                                                    int len) {
  return pack_string((const int8_t*)(str + pos - 1), len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE const char* cider_substring_extra_ptr(
    const char* str,
    int pos) {
  return str + pos - 1;
}

// pos starts with 1. A negative starting position is interpreted as being relative
// to the end of the string
extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t format_substring_pos(int pos, int str_len) {
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
extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t format_substring_len(int pos,
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
extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* extract_string_ptr(
    const uint64_t str_and_len) {
  return reinterpret_cast<int8_t*>(str_and_len & 0xffffffffffff);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t
extract_string_len(const uint64_t str_and_len) {
  return static_cast<int64_t>(str_and_len) >> 48;
}

extern "C" RUNTIME_FUNC NEVER_INLINE uint64_t
cider_ascii_lower(int8_t* string_heap_ptr, const char* __restrict str, int str_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(str_len);
  char* sout = s.getDataWriteable();

  do_lower(sout, str, str_len);
  return pack_string_t(s);
}

extern "C" RUNTIME_FUNC NEVER_INLINE void cider_ascii_lower_ptr(
    char* __restrict buffer_ptr,
    const char* __restrict str,
    int str_len) {
  do_lower(buffer_ptr, str, str_len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t cider_ascii_lower_len(int str_len) {
  return str_len;
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
cider_ascii_upper(int8_t* __restrict string_heap_ptr,
                  const char* __restrict str,
                  int str_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(str_len);
  char* sout = s.getDataWriteable();
  do_upper(sout, str, str_len);
  return pack_string_t(s);
}

extern "C" RUNTIME_FUNC NEVER_INLINE void cider_ascii_upper_ptr(
    char* __restrict buffer_ptr,
    const char* __restrict str,
    int str_len) {
  do_upper(buffer_ptr, str, str_len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t cider_ascii_upper_len(int str_len) {
  return str_len;
}

extern "C" void RUNTIME_FUNC NEVER_INLINE test_to_string(int value) {
  std::printf("test_to_string: %s\n", std::to_string(value).c_str());
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t cider_concat(char* string_heap_ptr,
                                                          const char* __restrict lhs,
                                                          int lhs_len,
                                                          const char* __restrict rhs,
                                                          int rhs_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(lhs_len + rhs_len);

  char* buffer_ptr = s.getDataWriteable();
  memcpy(buffer_ptr, lhs, lhs_len);
  memcpy(buffer_ptr + lhs_len, rhs, rhs_len);

  return pack_string_t(s);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t cider_concat_len(int lhs_len, int rhs_len) {
  return lhs_len + rhs_len;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void cider_concat_ptr(char* buffer_ptr,
                                                           const char* __restrict lhs,
                                                           int lhs_len,
                                                           const char* __restrict rhs,
                                                           int rhs_len) {
  memcpy(buffer_ptr, lhs, lhs_len);
  memcpy(buffer_ptr + lhs_len, rhs, rhs_len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* allocate_from_string_heap(
    char* string_heap_ptr,
    int len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(len);
  return (int8_t*)s.getDataWriteable();
}
// to be deprecated.
// rconcat is only used for backward compatibility with template codegen, which only
// supports cases where the first arg is a variable.
// for concat ops like "constant || var", it will be converted to "var || constant" and
// then concatenated in the REVERSED order (RCONCAT).
// However, nextgen allows both arguments to be variables, so this function can be
// removed after full migration to nextgen
extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t cider_rconcat(char* string_heap_ptr,
                                                           const char* __restrict lhs,
                                                           int lhs_len,
                                                           const char* __restrict rhs,
                                                           int rhs_len) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->emptyString(lhs_len + rhs_len);

  char* buffer_ptr = s.getDataWriteable();
  memcpy(buffer_ptr, rhs, rhs_len);
  memcpy(buffer_ptr + rhs_len, lhs, lhs_len);

  return pack_string_t(s);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t cider_rconcat_len(int lhs_len, int rhs_len) {
  return lhs_len + rhs_len;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void cider_rconcat_ptr(char* buffer_ptr,
                                                            const char* __restrict lhs,
                                                            int lhs_len,
                                                            const char* __restrict rhs,
                                                            int rhs_len) {
  memcpy(buffer_ptr, rhs, rhs_len);
  memcpy(buffer_ptr + rhs_len, lhs, lhs_len);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_buffer_without_realloc(
    const int8_t* input_desc_ptr,
    const int32_t index) {
  const ArrowArray* arrow_array = reinterpret_cast<const ArrowArray*>(input_desc_ptr);
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(arrow_array->private_data);

  return holder->getBufferAs<int8_t>(index);
}

extern "C" RUNTIME_FUNC NEVER_INLINE void copy_string_buffer(const int8_t* input_desc_ptr,
                                                             int8_t* __restrict buffer,
                                                             int64_t total_row) {
  const int64_t* offset_buffer = reinterpret_cast<const int64_t*>(
      reinterpret_cast<const ArrowArray*>(input_desc_ptr)->buffers[1]);
  int32_t* actual_writable_offset_buffer =
      reinterpret_cast<int32_t*>(const_cast<int64_t*>(offset_buffer));
  int32_t cur_offset = 0;
  for (int i = 0; i < total_row; i++) {
    int32_t cur_len = (offset_buffer[i] >> 48);
    memcpy(buffer + cur_offset,
           reinterpret_cast<int8_t*>(offset_buffer[i] & 0xffffffffffff),
           cur_len);
    cur_offset += cur_len;
    actual_writable_offset_buffer[i + 1] = cur_offset;
  }
  actual_writable_offset_buffer[0] = 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE int32_t calculate_size(int8_t* arrow_pointer,
                                                            int64_t total_row) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  const int64_t* buffer = reinterpret_cast<const int64_t*>(array->buffers[1]);
  int32_t ret = 0;
  for (int i = 0; i < total_row; i++) {
    ret += (buffer[i] >> 48);
  }
  return ret;
}

extern "C" RUNTIME_FUNC NEVER_INLINE int8_t* get_buffer_with_allocate(
    const int8_t* input_desc_ptr,
    const int32_t current_bytes,
    const int32_t index) {
  const ArrowArray* arrow_array = reinterpret_cast<const ArrowArray*>(input_desc_ptr);
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(arrow_array->private_data);
  holder->allocBuffer(index, current_bytes);
  return holder->getBufferAs<int8_t>(index);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int8_t* get_buffer_with_realloc_on_demand(
    const int8_t* input_desc_ptr,
    const int32_t current_bytes,
    const int32_t index) {
  const ArrowArray* arrow_array = reinterpret_cast<const ArrowArray*>(input_desc_ptr);
  CiderArrowArrayBufferHolder* holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(arrow_array->private_data);

  // assumes arrow_array is an array for var-size binary (with 3 buffers)
  size_t capacity = holder->getBufferSizeAt(index);
  if (capacity == 0) {
    // initialize buffer with a capacity of 16384 bytes
    holder->allocBuffer(index, 16384);
  } else if (current_bytes >= 0.9 * capacity) {
    // double capacity if current bytes take up 90% of capacity
    // assumes we would have enough space for next input after at most one resize op
    holder->allocBuffer(index, capacity * 1.5);
  }

  return holder->getBufferAs<int8_t>(index);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
cider_trim(char* string_heap_ptr,
           const char* __restrict str_ptr,
           int str_len,
           const int8_t* __restrict trim_char_map,
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

extern "C" RUNTIME_FUNC NEVER_INLINE int32_t
cider_trim_start(const char* __restrict str_ptr,
                 int str_len,
                 const int8_t* __restrict trim_char_map,
                 bool ltrim) {
  int start_idx = 0;
  if (ltrim) {
    while (start_idx < str_len &&
           trim_char_map[reinterpret_cast<const uint8_t*>(str_ptr)[start_idx]]) {
      start_idx++;
    }
  }
  return start_idx;
}

extern "C" RUNTIME_FUNC NEVER_INLINE int32_t
cider_trim_len(const char* __restrict str_ptr,
               int str_len,
               const int8_t* __restrict trim_char_map,
               bool ltrim,
               bool rtrim) {
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
  return len;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void cider_trim_ptr(char* __restrict buffer_ptr,
                                                         const char* __restrict str_ptr,
                                                         int start_idx,
                                                         int len) {
  memcpy(buffer_ptr, str_ptr + start_idx, len);
}

#define DEF_CONVERT_INTEGER_TO_STRING(value_type, value_name)                         \
  extern "C" RUNTIME_FUNC NEVER_INLINE int64_t gen_string_from_##value_name(          \
      const value_type operand, char* string_heap_ptr) {                              \
    std::string str = std::to_string(operand);                                        \
    StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);                 \
    string_t s = ptr->addString(str.data(), str.length());                            \
    return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize()); \
  }
DEF_CONVERT_INTEGER_TO_STRING(int8_t, tinyint)
DEF_CONVERT_INTEGER_TO_STRING(int16_t, smallint)
DEF_CONVERT_INTEGER_TO_STRING(int32_t, int)
DEF_CONVERT_INTEGER_TO_STRING(int64_t, bigint)
#undef DEF_CONVERT_INTEGER_TO_STRING

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
gen_string_from_float(const float operand, char* string_heap_ptr) {
  std::string str = fmt::format("{:#}", operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
gen_string_from_double(const double operand, char* string_heap_ptr) {
  std::string str = fmt::format("{:#}", operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t gen_string_from_bool(const int8_t operand,
                                                                  char* string_heap_ptr) {
  std::string str = (operand == 1) ? "true" : "false";
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(str.data(), str.length());
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
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

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
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

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t gen_string_from_date(const int32_t operand,
                                                                  char* string_heap_ptr) {
  constexpr size_t buf_size = 64;
  char buf[buf_size];
  int32_t str_len = shared::formatDays(buf, buf_size, operand);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  string_t s = ptr->addString(buf, str_len);
  return pack_string((const int8_t*)s.getDataUnsafe(), (const int32_t)s.getSize());
}

#define DEF_CONVERT_STRING_TO_INTEGER(value_type, value_name)                     \
  extern "C" RUNTIME_FUNC NEVER_INLINE value_type convert_string_to_##value_name( \
      const char* str_ptr, const int32_t str_len) {                               \
    std::string from_str(str_ptr, str_len);                                       \
    value_type res = std::stoi(from_str);                                         \
    if (res > std::numeric_limits<value_type>::min() &&                           \
        res <= std::numeric_limits<value_type>::max())                            \
      return res;                                                                 \
    CIDER_THROW(CiderRuntimeException,                                            \
                "value is out of range when cast from string to " #value_type);   \
  }

DEF_CONVERT_STRING_TO_INTEGER(int8_t, tinyint)
DEF_CONVERT_STRING_TO_INTEGER(int16_t, smallint)
DEF_CONVERT_STRING_TO_INTEGER(int32_t, int)
#undef DEF_CONVERT_STRING_TO_INTEGER

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
convert_string_to_bigint(const char* str_ptr, const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return std::stoll(from_str);
}

extern "C" RUNTIME_FUNC NEVER_INLINE float convert_string_to_float(
    const char* str_ptr,
    const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return std::stof(from_str);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int8_t
convert_string_to_bool(const char* str_ptr, const int32_t str_len) {
  std::string s(str_ptr, str_len);
  if (s == "t" || s == "T" || s == "1" || to_upper(std::string(s)) == "TRUE") {
    return 1;
  } else if (s == "f" || s == "F" || s == "0" || to_upper(std::string(s)) == "FALSE") {
    return 0;
  }
  CIDER_THROW(CiderRuntimeException, "cast from string to bool runtime error");
}

extern "C" RUNTIME_FUNC NEVER_INLINE double convert_string_to_double(
    const char* str_ptr,
    const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return std::stod(from_str);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int32_t
convert_string_to_date(const char* str_ptr, const int32_t str_len) {
  std::string from_str(str_ptr, str_len);
  return parseDateInDays(from_str);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
convert_string_to_timestamp(const char* str_ptr,
                            const int32_t str_len,
                            const int32_t dim) {
  std::string from_str(str_ptr, str_len);
  return dateTimeParse<kTIMESTAMP>(from_str, dim);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t convert_string_to_time(const char* str_ptr,
                                                                    const int32_t str_len,
                                                                    const int32_t dim) {
  std::string from_str(str_ptr, str_len);
  return dateTimeParse<kTIME>(from_str, dim);
}

extern "C" RUNTIME_FUNC NEVER_INLINE size_t
cider_find_str_from_left(const char* str1,
                         size_t str1_len,
                         const char* str2,
                         size_t str2_len,
                         size_t start_pos = npos) {
  if (str1_len < str2_len) {
    return npos;
  }
  size_t real_start_pos = (start_pos == npos) ? 0 : start_pos;
  for (size_t i = real_start_pos; i < str1_len - str2_len; ++i) {
    if (!std::memcmp(str1 + i, str2, str2_len)) {
      return i;
    }
  }
  return npos;
}

extern "C" RUNTIME_FUNC NEVER_INLINE size_t
cider_find_str_from_right(const char* str1,
                          size_t str1_len,
                          const char* str2,
                          size_t str2_len,
                          size_t start_pos = npos) {
  if (str1_len < str2_len) {
    return npos;
  }
  size_t real_start_pos = (start_pos == npos) ? 0 : start_pos;
  for (size_t i = real_start_pos; i >= str2_len; --i) {
    if (!std::memcmp(str1 + i - str2_len, str2, str2_len)) {
      return i - str2_len;
    }
  }
  return npos;
}

// Split a string into a list of strings, based on a specified `separator` character.
// str_ptr & str_len: the input string
// delimiter_ptr & delimiter_len: A character used for splitting the string.
// reverse: default value is false, will be true if split_part < 0.
// limit: Must be positive. Returns an array of size at most 'limit', and the last
// element in array contains everything left in the string. split_part: Field index to
// be returned. Index starts from 1. If the index is larger than the number of fields, a
// null string is returned.
extern "C" RUNTIME_FUNC NEVER_INLINE int64_t cider_split(char* string_heap_ptr,
                                                         const char* str_ptr,
                                                         int str_len,
                                                         const char* delimiter_ptr,
                                                         int delimiter_len,
                                                         bool reverse,
                                                         int limit,
                                                         int split_part) {
  // If split_part is negative then it is taken as the number
  // of split parts from the end of the string
  split_part = split_part == 0 ? 1UL : std::abs(split_part);
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  if (delimiter_len == 0) {
    string_t s = ptr->addString(str_ptr, str_len);
    return pack_string_t(s);
  }

  if (limit == 1) {
    // should return a list with only 1 string (which should not be splitted)
    if (split_part == 1) {
      string_t s = ptr->addString(str_ptr, str_len);
      return pack_string_t(s);
    } else {
      // out of range, should return null;
      return 0;
    }
  }
  size_t delimiter_pos = reverse ? str_len : 0UL;
  size_t last_delimiter_pos;
  size_t delimiter_idx = 0UL;
  size_t limit_counter = 0UL;

  do {
    last_delimiter_pos = delimiter_pos;

    delimiter_pos =
        reverse ? cider_find_str_from_right(
                      str_ptr, str_len, delimiter_ptr, delimiter_len, delimiter_pos)
                : cider_find_str_from_left(
                      str_ptr,
                      str_len,
                      delimiter_ptr,
                      delimiter_len,
                      // shouldn't skip delimiter length on first search attempt
                      delimiter_pos == 0 ? 0 : delimiter_pos + delimiter_len);
    // do ++limit_counter in the loop to prevent bugs caused by shortcut execution
    ++limit_counter;
    // however, we still keep ++delimiter_idx in while condition check to ensure
    // the property that delimiter_idx == 0 if delimiter does not exist in input string
  } while (delimiter_pos != npos && ++delimiter_idx < split_part &&
           (limit == 0 || limit_counter < limit));
  if (limit && limit_counter == limit) {
    // split has reached maximum split limit
    // treat whatever remains as a whole by extending delimiter_pos to end-of-string
    delimiter_pos = npos;
  }

  if (delimiter_idx == 0 && split_part == 1) {
    // delimiter does not exist, but the first split is requested, return the entire str
    string_t s = ptr->addString(str_ptr, str_len);
    return pack_string_t(s);
  }

  if (delimiter_pos == npos &&
      (delimiter_idx < split_part - 1UL || delimiter_idx < 1UL)) {
    // split_part_ was out of range
    return 0;  // null string
  }

  if (reverse) {
    const size_t substr_start =
        delimiter_pos == npos ? 0UL : delimiter_pos + delimiter_len;
    string_t s =
        ptr->addString(str_ptr + substr_start, last_delimiter_pos - substr_start);
    return pack_string_t(s);
  } else {
    const size_t substr_start =
        split_part == 1UL ? 0UL : last_delimiter_pos + delimiter_len;
    size_t len;
    if (-1 == delimiter_pos) {
      len = str_len - substr_start;
    } else {
      len = delimiter_pos - substr_start;
    }

    string_t s = ptr->addString(str_ptr + substr_start, len);
    return pack_string_t(s);
  }
}

// Search a string for a substring that matches a given regular expression pattern and
// replace it with a replacement string.
// str_ptr & str_len: input string.
// regex_pattern_ptr & regex_pattern_len: the regular expression to search for within
// the input string. replace_ptr & replace_len: the replacement string. start_pos: the
// position to start the search. occurrence: which occurrence of the match to replace.
extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
cider_regexp_replace(char* string_heap_ptr,
                     const char* str_ptr,
                     int str_len,
                     const char* regex_pattern_ptr,
                     int regex_pattern_len,
                     const char* replace_ptr,
                     const int replace_len,
                     int start_pos,
                     int occurrence) {
  start_pos = start_pos > 0 ? start_pos - 1 : start_pos;
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  const size_t wrapped_start = static_cast<size_t>(
      std::min(start_pos >= 0 ? start_pos : std::max(str_len + start_pos, 0), str_len));
  // construct for re2 lib - first memory copy
  std::string input(str_ptr + wrapped_start, str_len - wrapped_start);
  re2::StringPiece pattern(regex_pattern_ptr, regex_pattern_len);
  re2::StringPiece replace(replace_ptr, replace_len);
  if (occurrence == 0L) {
    // occurrence_ == 0: replace all occurrences
    int cnt = RE2::GlobalReplace(&input, pattern, replace);
    string_t res = ptr->emptyString(wrapped_start + input.length());
    // construct result string - second memory copy
    std::memcpy(res.getDataWriteable(), str_ptr, wrapped_start);
    std::memcpy(res.getDataWriteable() + wrapped_start, input.c_str(), input.length());
    return pack_string_t(res);
  } else {
    // only replace n-th occurrence
    std::pair<size_t, size_t> match_pos =
        cider_find_nth_regex_match(str_ptr,
                                   str_len,
                                   pattern,
                                   start_pos,
                                   occurrence > 0 ? occurrence - 1 : occurrence);
    if (match_pos.first == npos) {
      // no match found, return origin string
      string_t res = ptr->addString(str_ptr, str_len);
      return pack_string_t(res);
    } else {
      string_t res = ptr->emptyString(str_len - match_pos.second + replace_len);
      std::memcpy(res.getDataWriteable(), str_ptr, match_pos.first);
      std::memcpy(res.getDataWriteable() + match_pos.first, replace_ptr, replace_len);
      std::memcpy(res.getDataWriteable() + match_pos.first + replace_len,
                  str_ptr + match_pos.first + match_pos.second,
                  str_len - (match_pos.first + match_pos.second));
      return pack_string_t(res);
    }
  }

  return 0;
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
cider_regexp_extract(char* string_heap_ptr,
                     const char* str_ptr,
                     int str_len,
                     const char* regex_pattern_ptr,
                     int regex_pattern_len,
                     int group) {
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);

  std::string group_string = "\\" + std::to_string(group);
  std::string out;
  re2::StringPiece input(str_ptr, str_len);
  re2::StringPiece pattern(regex_pattern_ptr, regex_pattern_len);
  RE2 re(pattern);
  RE2::Extract(input, re, group_string, &out);

  string_t res = ptr->addString(out.c_str(), out.length());
  return pack_string_t(res);
}

extern "C" RUNTIME_FUNC NEVER_INLINE int64_t
cider_regexp_substring(char* string_heap_ptr,
                       const char* str_ptr,
                       int str_len,
                       const char* regex_pattern_ptr,
                       int regex_pattern_len,
                       int occurrence,
                       int start_pos) {
  start_pos = start_pos > 0 ? start_pos - 1 : str_len + start_pos;
  StringHeap* ptr = reinterpret_cast<StringHeap*>(string_heap_ptr);
  const size_t wrapped_start = static_cast<size_t>(
      std::min(start_pos >= 0 ? start_pos : std::max(str_len + start_pos, 0), str_len));
  std::string input(str_ptr + wrapped_start, str_len - wrapped_start);
  re2::StringPiece pattern(regex_pattern_ptr, regex_pattern_len);

  std::pair<size_t, size_t> match_pos = cider_find_nth_regex_match(
      str_ptr, str_len, pattern, start_pos, occurrence > 0 ? occurrence - 1 : occurrence);
  if (match_pos.first == npos) {
    // no match found, return empty
    string_t res = ptr->emptyString(0);
    return pack_string_t(res);
  } else {
    string_t res = ptr->emptyString(match_pos.second);
    std::memcpy(res.getDataWriteable(), str_ptr + match_pos.first, match_pos.second);

    return pack_string_t(res);
  }
}

extern "C" RUNTIME_FUNC NEVER_INLINE int32_t cast_date_string_to_int(const char* str_ptr,
                                                                     int str_len) {
  // extract year, month, day, only support format like 'yyyy-mm-dd' or 'yyyy/mm/dd'
  int y = std::stoi(std::string(str_ptr, 4));
  int m = std::stoi(std::string(str_ptr + 5, 2));
  int d = std::stoi(std::string(str_ptr + 8, 2));

  y -= m <= 2;
  const int era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = static_cast<unsigned>(y - era * 400);            // [0, 399]
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;  // [0, 365]
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;           // [0, 146096]
  return era * 146097 + static_cast<int>(doe) - 719468;
}
