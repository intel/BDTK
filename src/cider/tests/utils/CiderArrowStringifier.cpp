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

#include "tests/utils/CiderArrowStringifier.h"
namespace cider::test::util {

#define NULL_VALUE "null"

void ArrowStructStringifier::init(const struct ArrowArray* array,
                                  const struct ArrowSchema* schema) {
  children_stringifiers_.clear();
  CHECK_EQ(schema->format[0], '+');
  CHECK_EQ(schema->format[1], 's');

  auto col_num = array->n_children;
  for (auto col_index = 0; col_index < col_num; ++col_index) {
    auto child_array = array->children[col_index];
    auto child_schema = schema->children[col_index];
    switch (child_schema->format[0]) {
      case 'b':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<bool>>());
        break;
      case 'c':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<int8_t>>());
        break;
      case 's':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<int16_t>>());
        break;
      case 'i':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<int32_t>>());
        break;
      case 'l':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<int64_t>>());
        break;
      case 'f':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<float>>());
        break;
      case 'g':
        children_stringifiers_.emplace_back(
            std::make_unique<ArrowScalarStringifier<double>>());
        break;
      case 'u':
        children_stringifiers_.emplace_back(std::make_unique<ArrowVarcharStringifier>());
        break;
      default:
        CIDER_THROW(CiderRuntimeException, "not supported type: " + schema->format[0]);
    }
  }
}

std::string ArrowStructStringifier::stringifyValueAt(const struct ArrowArray* array,
                                                     const struct ArrowSchema* schema,
                                                     int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }
  int col_num = array->n_children;
  ConcatenatedRow row;
  for (auto col_index = 0; col_index < col_num; ++col_index) {
    auto child_array = array->children[col_index];
    auto child_schema = schema->children[col_index];
    auto& col_stringifier = children_stringifiers_[col_index];
    std::string value_str =
        col_stringifier->stringifyValueAt(child_array, child_schema, row_index);
    row.addCol(value_str);
  }
  row.finish();
  return row.getString();
}

bool ArrowStringifier::isNullAt(const struct ArrowArray* array, int row_index) {
  const uint8_t* null_bitmap = reinterpret_cast<const uint8_t*>(array->buffers[0]);
  if (null_bitmap && !CiderBitUtils::isBitSetAt(null_bitmap, row_index)) {
    return true;
  }
  return false;
}

template <typename T>
std::string ArrowScalarStringifier<T>::stringifyValueAt(const struct ArrowArray* array,
                                                        const struct ArrowSchema* schema,
                                                        int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }

  const T* data_buffer = reinterpret_cast<const T*>(array->buffers[1]);
  T value = data_buffer[row_index];
  return std::to_string(static_cast<int64_t>(value));
}

template <>
std::string ArrowScalarStringifier<float>::stringifyValueAt(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema,
    int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }
  const float* data_buffer = reinterpret_cast<const float*>(array->buffers[1]);
  std::stringstream fps;
  fps.clear();
  float value = data_buffer[row_index];
  fps << std::setprecision(16) << value;
  return fps.str();
}

template <>
std::string ArrowScalarStringifier<double>::stringifyValueAt(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema,
    int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }
  const double* data_buffer = reinterpret_cast<const double*>(array->buffers[1]);
  std::stringstream fps;
  fps.clear();
  double value = data_buffer[row_index];
  fps << std::setprecision(16) << value;
  return fps.str();
}

template <>
std::string ArrowScalarStringifier<bool>::stringifyValueAt(
    const struct ArrowArray* array,
    const struct ArrowSchema* schema,
    int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }
  const uint8_t* data_buffer = reinterpret_cast<const uint8_t*>(array->buffers[1]);
  return std::to_string(
      static_cast<int64_t>(CiderBitUtils::isBitSetAt(data_buffer, row_index)));
}

std::string ArrowVarcharStringifier::stringifyValueAt(const struct ArrowArray* array,
                                                      const struct ArrowSchema* schema,
                                                      int row_index) {
  if (isNullAt(array, row_index)) {
    return NULL_VALUE;
  }
  const uint8_t* offset_buffer = reinterpret_cast<const uint8_t*>(array->buffers[1]);
  const int32_t* data_buffer = reinterpret_cast<const int32_t*>(array->buffers[2]);
  int32_t start = offset_buffer[row_index];
  int32_t end = offset_buffer[row_index + 1];
  int32_t len = end - start;
  return std::string(reinterpret_cast<const char*>(data_buffer) + start, len);
}
}  // namespace cider::test::util
