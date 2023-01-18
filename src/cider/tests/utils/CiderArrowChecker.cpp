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

#include "tests/utils/CiderArrowChecker.h"

#include <cstring>

#include "util/Logger.h"

namespace cider::test::util {

namespace {

template <typename T>
bool checkArrowBuffer(const struct ArrowArray* expect_array,
                      const struct ArrowArray* actual_array) {
  auto expect_value_buffer = reinterpret_cast<const T*>(expect_array->buffers[1]);
  auto actual_value_buffer = reinterpret_cast<const T*>(actual_array->buffers[1]);
  if (expect_value_buffer == nullptr && actual_value_buffer == nullptr) {
    return true;
  }
  if (expect_value_buffer == nullptr || actual_value_buffer == nullptr) {
    return false;
  }

  auto expect_null_buffer = reinterpret_cast<const uint8_t*>(expect_array->buffers[0]);
  auto actual_null_buffer = reinterpret_cast<const uint8_t*>(actual_array->buffers[0]);

  if (expect_null_buffer && actual_null_buffer) {
    for (int64_t i = 0; i < expect_array->length; i++) {
      bool expect_valid = CiderBitUtils::isBitSetAt(expect_null_buffer, i);
      bool actual_valid = CiderBitUtils::isBitSetAt(actual_null_buffer, i);
      if (expect_valid != actual_valid) {
        LOG(INFO) << "ArrowArray null bit not equal: "
                  << "Expected: " << expect_valid << ". Actual: " << actual_valid;
        return false;
      }
      if (expect_valid) {
        if (expect_value_buffer[i] != actual_value_buffer[i]) {
          return false;
        }
      }
    }
  } else {
    if (!(expect_null_buffer == nullptr && actual_null_buffer == nullptr)) {
      LOG(INFO) << "One ArrowArray null buffer is null in checkArrowBuffer.";
    }
    return !memcmp(
        expect_value_buffer, actual_value_buffer, sizeof(T) * expect_array->length);
  }
  return true;
}

template <typename T>
bool absoluteToleranceCompare(T x, T y) {
  if (x == std::numeric_limits<T>::infinity() &&
      y == std::numeric_limits<T>::infinity()) {
    return true;
  }
  return std::fabs(x - y) <= std::numeric_limits<T>::epsilon();
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value, bool> = true>
bool checkArrowBufferFp(const struct ArrowArray* expect_array,
                        const struct ArrowArray* actual_array) {
  auto expect_value_buffer = reinterpret_cast<const T*>(expect_array->buffers[1]);
  auto actual_value_buffer = reinterpret_cast<const T*>(actual_array->buffers[1]);
  if (expect_value_buffer == nullptr && actual_value_buffer == nullptr) {
    return true;
  }
  if (expect_value_buffer == nullptr || actual_value_buffer == nullptr) {
    return false;
  }

  auto expect_null_buffer = reinterpret_cast<const uint8_t*>(expect_array->buffers[0]);
  auto actual_null_buffer = reinterpret_cast<const uint8_t*>(actual_array->buffers[0]);

  if (expect_null_buffer && actual_null_buffer) {
    for (int64_t i = 0; i < expect_array->length; i++) {
      bool expect_valid = CiderBitUtils::isBitSetAt(expect_null_buffer, i);
      bool actual_valid = CiderBitUtils::isBitSetAt(actual_null_buffer, i);
      if (expect_valid != actual_valid) {
        LOG(INFO) << "ArrowArray null bit not equal: "
                  << "Expected: " << expect_valid << ". Actual: " << actual_valid;
        return false;
      }
      if (expect_valid) {
        if (!absoluteToleranceCompare<T>(expect_value_buffer[i],
                                         actual_value_buffer[i])) {
          return false;
        }
      }
    }
  } else {
    if (!(expect_null_buffer == nullptr && actual_null_buffer == nullptr)) {
      LOG(INFO) << "One ArrowArray null buffer is null in checkArrowBuffer.";
    }
    for (int64_t i = 0; i < expect_array->length; i++) {
      if (!absoluteToleranceCompare<T>(expect_value_buffer[i], actual_value_buffer[i])) {
        return false;
      }
    }
  }
  return true;
}

template <>
bool checkArrowBuffer<bool>(const struct ArrowArray* expect_array,
                            const struct ArrowArray* actual_array) {
  auto expect_value_buffer = reinterpret_cast<const uint8_t*>(expect_array->buffers[1]);
  auto actual_value_buffer = reinterpret_cast<const uint8_t*>(actual_array->buffers[1]);
  if (expect_value_buffer == nullptr && actual_value_buffer == nullptr) {
    return true;
  }
  if (expect_value_buffer == nullptr || actual_value_buffer == nullptr) {
    return false;
  }
  auto row_num = expect_array->length;
  auto bytes = ((row_num + 7) >> 3);

  auto expect_null_buffer = reinterpret_cast<const uint8_t*>(expect_array->buffers[0]);
  auto actual_null_buffer = reinterpret_cast<const uint8_t*>(actual_array->buffers[0]);

  for (int i = 0; i < bytes - 1; ++i) {
    // apply bitwise AND masking
    uint8_t expected_masked = expect_null_buffer
                                  ? expect_value_buffer[i] & expect_null_buffer[i]
                                  : expect_value_buffer[i];
    uint8_t actual_masked = actual_null_buffer
                                ? actual_value_buffer[i] & actual_null_buffer[i]
                                : actual_value_buffer[i];

    if (expected_masked != actual_masked) {
      // we expect all bits here are equal, i.e. the uint8 value should be equal
      return false;
    }
  }

  // the last byte require some extra processing
  // because the trailing padding values are uninitialized and can be different
  uint8_t expected_masked =
      expect_null_buffer ? expect_value_buffer[bytes - 1] & expect_null_buffer[bytes - 1]
                         : expect_value_buffer[bytes - 1];
  uint8_t actual_masked =
      actual_null_buffer ? actual_value_buffer[bytes - 1] & actual_null_buffer[bytes - 1]
                         : actual_value_buffer[bytes - 1];
  // clear padding values. least-significant bit ordering, clear most significant bits
  auto n_paddings = 8 * bytes - row_num;
  expected_masked = expected_masked & (0xFF >> n_paddings);
  actual_masked = actual_masked & (0xFF >> n_paddings);

  if (expected_masked != actual_masked) {
    return false;
  }

  return true;
}

bool checkStringEq(const int8_t* expect_data_buffer,
                   const int32_t* expect_offset_buffer,
                   const int8_t* actual_data_buffer,
                   const int32_t* actual_offset_buffer,
                   int idx) {
  int32_t expect_offset = expect_offset_buffer[idx];
  int32_t expect_length = expect_offset_buffer[idx + 1] - expect_offset_buffer[idx];

  int32_t actual_offset = actual_offset_buffer[idx];
  int32_t actual_length = actual_offset_buffer[idx + 1] - actual_offset_buffer[idx];

  if (expect_length != actual_length || memcmp(expect_data_buffer + expect_offset,
                                               actual_data_buffer + actual_offset,
                                               expect_length)) {
    return false;
  }
  return true;
}

bool checkArrowStringBuffer(const struct ArrowArray* expect_array,
                            const struct ArrowArray* actual_array) {
  auto length = expect_array->length;
  auto expect_data_buffer = reinterpret_cast<const int8_t*>(expect_array->buffers[2]);
  auto actual_data_buffer = reinterpret_cast<const int8_t*>(actual_array->buffers[2]);
  if (expect_data_buffer == nullptr && actual_data_buffer == nullptr) {
    return true;
  }
  if (expect_data_buffer == nullptr || actual_data_buffer == nullptr) {
    return false;
  }

  auto expect_offset_buffer = reinterpret_cast<const int32_t*>(expect_array->buffers[1]);
  auto actual_offset_buffer = reinterpret_cast<const int32_t*>(actual_array->buffers[1]);

  auto expect_null_buffer = reinterpret_cast<const uint8_t*>(expect_array->buffers[0]);
  auto actual_null_buffer = reinterpret_cast<const uint8_t*>(actual_array->buffers[0]);
  if (expect_null_buffer && actual_null_buffer) {
    for (int64_t i = 0; i < expect_array->length; i++) {
      bool expect_valid = CiderBitUtils::isBitSetAt(expect_null_buffer, i);
      bool actual_valid = CiderBitUtils::isBitSetAt(actual_null_buffer, i);
      if (expect_valid != actual_valid) {
        LOG(INFO) << "ArrowArray null bit not equal: "
                  << "Expected: " << expect_valid << ". Actual: " << actual_valid;
        return false;
      }
      if (expect_valid) {
        if (!checkStringEq(expect_data_buffer,
                           expect_offset_buffer,
                           actual_data_buffer,
                           actual_offset_buffer,
                           i)) {
          return false;
        }
      }
    }
    return true;
  } else {
    if (!(expect_null_buffer == nullptr && actual_null_buffer == nullptr)) {
      LOG(INFO) << "One ArrowArray null buffer is null in checkArrowBuffer.";
    }
    for (int i = 0; i < expect_array->length; ++i) {
      if (!checkStringEq(expect_data_buffer,
                         expect_offset_buffer,
                         actual_data_buffer,
                         actual_offset_buffer,
                         i)) {
        return false;
      }
    }
    return true;
  }
  return false;
}

}  // namespace

bool checkOneScalarArrowEqual(const struct ArrowArray* expect_array,
                              const struct ArrowArray* actual_array,
                              const struct ArrowSchema* expect_schema,
                              const struct ArrowSchema* actual_schema) {
  if (!expect_schema || !actual_schema) {
    LOG(INFO) << "One or more Arrowschema are null in checkOneScalarArrowEqual.";
    return false;
  }

  if (expect_schema->n_children != 0 || actual_schema->n_children != 0) {
    LOG(INFO) << "checkOneScalarArrowEqual only support ArrowSchema without children";
    return false;
  }

  if (expect_schema->format != actual_schema->format) {
    LOG(INFO) << "ArrowSchema format not equal: "
              << "Expected: " << expect_schema->format
              << ". Actual: " << actual_schema->format;
    // temp workaround for non-groupby, as duckdb convert agg result to decimal. Will
    // enable after support cast or decimal return false;
  }

  if (!expect_array || !actual_array) {
    LOG(INFO) << "One or more Arrowarray are null in checkOneScalarArrowEqual.";
    return false;
  }

  if (expect_array->n_children != 0 || actual_array->n_children != 0) {
    LOG(INFO) << "checkOneScalarArrowEqual only support ArrowArray without children";
    return false;
  }

  if (expect_array->null_count != actual_array->null_count) {
    LOG(INFO) << "ArrowArray null_count not equal: "
              << "Expected: " << expect_array->null_count
              << ". Actual: " << actual_array->null_count;
    return false;
  }

  if (expect_array->n_buffers != actual_array->n_buffers) {
    LOG(INFO) << "ArrowArray n_buffers not equal: "
              << "Expected: " << expect_array->n_buffers
              << ". Actual: " << actual_array->n_buffers;
    return false;
  }

  if (expect_array->length != actual_array->length) {
    LOG(INFO) << "ArrowArray length not equal: "
              << "Expected: " << expect_array->length
              << ". Actual: " << actual_array->length;
    return false;
  }

  switch (expect_schema->format[0]) {
    case 'b':
      return checkArrowBuffer<bool>(expect_array, actual_array);
    case 'c':
    case 'C':
      return checkArrowBuffer<int8_t>(expect_array, actual_array);
    case 's':
    case 'S':
      return checkArrowBuffer<int16_t>(expect_array, actual_array);
    case 'i':
    case 'I':
      return checkArrowBuffer<int32_t>(expect_array, actual_array);
    case 'l':
    case 'L':
      return checkArrowBuffer<int64_t>(expect_array, actual_array);
    case 'f':
      return checkArrowBufferFp<float>(expect_array, actual_array);
    case 'g':
      return checkArrowBufferFp<double>(expect_array, actual_array);
    case 'd':
      // workaround for non-groupby agg test
      return checkArrowBuffer<int64_t>(expect_array, actual_array);
    case 't': {
      if (expect_schema->format[1] == 'd' && expect_schema->format[2] == 'D') {
        return checkArrowBuffer<int32_t>(expect_array, actual_array);
      }
      if (expect_schema->format[1] == 't' && expect_schema->format[2] == 'u') {
        return checkArrowBuffer<int64_t>(expect_array, actual_array);
      }
      if (expect_schema->format[1] == 's' && expect_schema->format[2] == 'u') {
        return checkArrowBuffer<int64_t>(expect_array, actual_array);
      }
      LOG(ERROR) << "Not supported time type";
    }
    case 'u':
      return checkArrowStringBuffer(expect_array, actual_array);
    case 'e':
    case 'z':
    case 'Z':
    case 'U':
    case 'w':
    default:
      LOG(ERROR) << "ArrowArray value buffer check not support for type: "
                 << expect_schema->format;
  }
  return false;
}

bool CiderArrowChecker::checkArrowEq(const struct ArrowArray* expect_array,
                                     const struct ArrowArray* actual_array,
                                     const struct ArrowSchema* expect_schema,
                                     const struct ArrowSchema* actual_schema) {
  if (!expect_array || !actual_array) {
    LOG(INFO) << "One or more Arrowarray are null_ptr in checkArrowEq. ";
    return false;
  }
  if (expect_array->n_children != actual_array->n_children) {
    LOG(INFO) << "ArrowArray n_children not equal: "
              << "Expected: " << expect_array->n_children
              << ". Actual: " << actual_array->n_children;
    return false;
  }
  if (expect_array->length != actual_array->length) {
    LOG(INFO) << "ArrowArray length not equal: "
              << "Expected: " << expect_array->length
              << ". Actual: " << actual_array->length;
    return false;
  }

  if (!expect_schema || !actual_schema) {
    LOG(INFO) << "One or more Arrowschema are null_ptr in checkArrowEq. ";
    return false;
  }
  if (expect_schema->n_children != actual_schema->n_children) {
    LOG(INFO) << "ArrowSchema n_children not equal: "
              << "Expected: " << expect_schema->n_children
              << ". Actual: " << actual_schema->n_children;
    return false;
  }
  if (strcmp(expect_schema->format, actual_schema->format)) {
    LOG(INFO) << "ArrowSchema format not equal: "
              << "Expected: " << expect_schema->format
              << ". Actual: " << actual_schema->format;
    return false;
  }

  if (expect_array->n_children == 0) {
    return checkOneScalarArrowEqual(
        expect_array, actual_array, expect_schema, actual_schema);
  }

  for (int64_t i = 0; i < expect_array->n_children; i++) {
    bool child_arrow_eq = checkOneScalarArrowEqual(expect_array->children[i],
                                                   actual_array->children[i],
                                                   expect_schema->children[i],
                                                   actual_schema->children[i]);
    if (!child_arrow_eq) {
      return false;
    }
  }

  return true;
}

bool CiderArrowChecker::checkArrowEqIgnoreOrder(const struct ArrowArray* expect_array,
                                                const struct ArrowArray* actual_array,
                                                const struct ArrowSchema* expect_schema,
                                                const struct ArrowSchema* actual_schema) {
  auto expected_rowvec = toConcatenatedRowVector(expect_array, expect_schema);
  auto actual_rowvec = toConcatenatedRowVector(actual_array, actual_schema);
  return compareRowVectors(expected_rowvec, actual_rowvec);
}

std::vector<ConcatenatedRow> CiderArrowChecker::toConcatenatedRowVector(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema) {
  std::vector<ConcatenatedRow> total_row;
  auto col_num = array->n_children;
  ArrowStructStringifier root;
  root.init(array, schema);
  for (int row_index = 0; row_index < array->length; ++row_index) {
    auto str = root.stringifyValueAt(array, schema, row_index);
    ConcatenatedRow row(col_num, str);
    row.finish();
    total_row.push_back(row);
  }
  return total_row;
}

bool CiderArrowChecker::compareRowVectors(
    std::vector<ConcatenatedRow>& expected_row_vector,
    std::vector<ConcatenatedRow>& actual_row_vector,
    bool ignore_order) {
  if (expected_row_vector.size() == actual_row_vector.size()) {
    std::map<size_t, int64_t> check_map;
    if (ignore_order) {
      for (int row = 0; row < actual_row_vector.size(); row++) {
        auto actual_key = actual_row_vector[row].getHashValue();
        auto expected_key = expected_row_vector[row].getHashValue();
        check_map[actual_key] =
            check_map.find(actual_key) == check_map.end() ? 1 : check_map[actual_key] + 1;
        check_map[expected_key] = check_map.find(expected_key) == check_map.end()
                                      ? -1
                                      : check_map[expected_key] - 1;
      }
      for (auto entry : check_map) {
        if (entry.second) {
          std::cout << "Non-ordered data not match." << std::endl;
          return false;
        }
      }
      return true;
    } else {
      for (int row = 0; row < actual_row_vector.size(); row++) {
        auto actual_key = actual_row_vector[row].getHashValue();
        auto expected_key = expected_row_vector[row].getHashValue();
        if (actual_key != expected_key) {
          std::cout << "Ordered data not match." << std::endl;
          return false;
        }
      }
    }
  } else {
    std::cout << "The number of row does not match. "
              << "Expected row vector size is " << expected_row_vector.size()
              << ", while actual row vector size is " << actual_row_vector.size()
              << std::endl;
    return false;
  }
  return true;
}

}  // namespace cider::test::util
