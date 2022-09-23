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

#include "ArrowConvertorUtils.h"
#include "BitUtils.h"
#include "TypeConversions.h"

namespace facebook::velox::plugin {

template <typename T>
int8_t* convertToCiderImpl(const ArrowSchema& arrowSchema,
                           const ArrowArray& arrowArray,
                           int num_rows) {
  const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
  // cannot directly change arrow buffers as const array, need memcpy
  T* column = (T*)std::malloc(sizeof(T) * num_rows);
  memcpy(column, arrowArray.buffers[1], sizeof(T) * num_rows);
  // set null value
  T nullValue = getNullValue<T>();
  // null_count MAY be -1 if not yet computed.
  if (arrowArray.null_count != 0) {
    for (auto pos = 0; pos < num_rows; pos++) {
      if (!isBitSet(nulls, pos)) {
        column[pos] = nullValue;
      }
    }
  }
  return reinterpret_cast<int8_t*>(column);
}

int8_t* convertTimestamp(const char* arrow_type,
                         const ArrowSchema& arrowSchema,
                         const ArrowArray& arrowArray,
                         int num_rows) {
  if (arrow_type[1] == 't') {
    if (arrow_type[2] == 's' || arrow_type[2] == 'm') {
      return convertToCiderImpl<int32_t>(arrowSchema, arrowArray, num_rows);
    } else if (arrow_type[2] == 'u' || arrow_type[2] == 'n') {
      return convertToCiderImpl<int64_t>(arrowSchema, arrowArray, num_rows);
    }
  }
  VELOX_UNSUPPORTED("Conversion is not supported yet, arrow_type is {}", arrow_type);
}

int8_t* convertToCider(const ArrowSchema& arrowSchema,
                       const ArrowArray& arrowArray,
                       int num_rows) {
  const char* arrow_type = arrowSchema.format;
  switch (arrow_type[0]) {
    case 'b':
      return convertToCiderImpl<bool>(arrowSchema, arrowArray, num_rows);
    case 'c':
      return convertToCiderImpl<int8_t>(arrowSchema, arrowArray, num_rows);
    case 's':
      return convertToCiderImpl<int16_t>(arrowSchema, arrowArray, num_rows);
    case 'i':
      return convertToCiderImpl<int32_t>(arrowSchema, arrowArray, num_rows);
    case 'l':
    case 'd':  // decimal
      return convertToCiderImpl<int64_t>(arrowSchema, arrowArray, num_rows);
    case 'f':
      return convertToCiderImpl<float>(arrowSchema, arrowArray, num_rows);
    case 'g':
      return convertToCiderImpl<double>(arrowSchema, arrowArray, num_rows);
    case 't':
      return convertTimestamp(arrow_type, arrowSchema, arrowArray, num_rows);
    default:
      VELOX_UNSUPPORTED("Conversion is not supported yet, arrow_type is {}", arrow_type);
  }
}

static void releaseArray(ArrowArray* array) {
  // Free the buffers, for basic types buffer size should be 2 (null and value
  // buffer)
  // free((void*)array->buffers[0]);
  // free((void*)array->buffers[1]);
  // free(array->buffers);
  // Mark released
  array->release = nullptr;
  array->private_data = nullptr;
}

template <typename T>
void convertToArrowImpl(ArrowArray& arrowArray, const int8_t* data_buffer, int num_rows) {
  int64_t nullCount = 0;
  uint64_t* nulls = (uint64_t*)std::malloc(sizeof(uint64_t*) * num_rows);
  const T* srcValues = reinterpret_cast<const T*>(data_buffer);
  T nullValue = getNullValue<T>();
  for (auto pos = 0; pos < num_rows; pos++) {
    if (srcValues[pos] == nullValue) {
      clearBit(nulls, pos);
      nullCount++;
    } else {
      setBit(nulls, pos);
    }
  }
  // 2 for null buffer and value buffer
  arrowArray.n_buffers = 2;
  const void** buffers = (const void**)malloc(sizeof(void*) * arrowArray.n_buffers);
  buffers[0] = (nullCount == 0) ? nullptr : (const void*)nulls;
  buffers[1] = (num_rows == 0) ? nullptr : (const void*)srcValues;
  arrowArray.buffers = buffers;

  arrowArray.length = num_rows;
  arrowArray.null_count = nullCount;
  arrowArray.offset = 0;

  arrowArray.n_children = 0;
  arrowArray.children = nullptr;
  arrowArray.dictionary = nullptr;

  arrowArray.release = releaseArray;
}

static void releaseSchema(ArrowSchema* arrowSchema) {
  arrowSchema->release = nullptr;
}

void convertTimestamp(const char* arrow_type,
                      ArrowArray& arrowArray,
                      const int8_t* data_buffer,
                      int num_rows) {
  if (arrow_type[1] == 't') {
    if (arrow_type[2] == 's' || arrow_type[2] == 'm') {
      convertToArrowImpl<int32_t>(arrowArray, data_buffer, num_rows);
    } else if (arrow_type[2] == 'u' || arrow_type[2] == 'n') {
      convertToArrowImpl<int64_t>(arrowArray, data_buffer, num_rows);
    }
  }
}

void convertToArrow(ArrowArray& arrowArray,
                    ArrowSchema& arrowSchema,
                    const int8_t* data_buffer,
                    ::substrait::Type col_type,
                    int num_rows) {
  arrowSchema = {
      getArrowFormat(col_type),
      nullptr,
      nullptr,
      0,
      0,
      nullptr,
      nullptr,
      releaseSchema,
  };
  const char* arrow_type = arrowSchema.format;
  switch (arrow_type[0]) {
    case 'b':
      return convertToArrowImpl<bool>(arrowArray, data_buffer, num_rows);
    case 'c':
      return convertToArrowImpl<int8_t>(arrowArray, data_buffer, num_rows);
    case 's':
      return convertToArrowImpl<int16_t>(arrowArray, data_buffer, num_rows);
    case 'i':
      return convertToArrowImpl<int32_t>(arrowArray, data_buffer, num_rows);
    case 'l':
    case 'd':  // decimal
      return convertToArrowImpl<int64_t>(arrowArray, data_buffer, num_rows);
    case 'f':
      return convertToArrowImpl<float>(arrowArray, data_buffer, num_rows);
    case 'g':
      return convertToArrowImpl<double>(arrowArray, data_buffer, num_rows);
    case 't':
      return convertTimestamp(arrow_type, arrowArray, data_buffer, num_rows);
    default:
      VELOX_UNSUPPORTED("Conversion is not supported yet, arrow_type is {}", arrow_type);
  }
}

}  // namespace facebook::velox::plugin
