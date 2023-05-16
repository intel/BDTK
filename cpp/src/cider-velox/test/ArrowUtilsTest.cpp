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

#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>
#include "ArrowConvertorUtils.h"
#include "BitUtils.h"

using namespace facebook::velox::plugin;

class ArrowUtilsTest : public testing::Test {
 public:
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_{
      facebook::velox::memory::getDefaultMemoryPool()};

  template <typename T>
  void testCiderResult(const int8_t* data_buffer,
                       substrait::Type col_type,
                       int num_rows,
                       int null_count,
                       int32_t dimen = 0) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    convertToArrow(arrowArray, arrowSchema, data_buffer, col_type, num_rows, pool_.get());
    EXPECT_EQ(num_rows, arrowArray.length);
    EXPECT_EQ(null_count, arrowArray.null_count);
    EXPECT_EQ(2, arrowArray.n_buffers);
    EXPECT_EQ(0, arrowArray.offset);
    EXPECT_EQ(0, arrowArray.n_children);

    EXPECT_EQ(nullptr, arrowArray.children);
    EXPECT_EQ(nullptr, arrowArray.dictionary);

    const uint64_t* nulls = static_cast<const uint64_t*>(arrowArray.buffers[0]);
    const T* values = static_cast<const T*>(arrowArray.buffers[1]);
    const T* col = reinterpret_cast<const T*>(data_buffer);
    T nullValue = getNullValue<T>();
    for (auto idx = 0; idx < num_rows; idx++) {
      if (col[idx] == nullValue) {
        EXPECT_TRUE(!isBitSet(nulls, idx));
      } else {
        EXPECT_TRUE(isBitSet(nulls, idx));
        EXPECT_EQ(values[idx], col[idx]);
      }
    }

    arrowArray.release(&arrowArray);
    EXPECT_EQ(nullptr, arrowArray.release);
    EXPECT_EQ(nullptr, arrowArray.private_data);
  }

  template <typename T>
  void testArrowArray(const char* format,
                      const std::vector<std::optional<T>>& data_buffer) {
    int num_rows = data_buffer.size();
    int64_t nullCount = 0;
    uint64_t* nulls =
        reinterpret_cast<uint64_t*>(pool_->allocate(sizeof(uint64_t*) * num_rows));
    T* srcValues = reinterpret_cast<T*>(pool_->allocate(sizeof(T) * num_rows));
    T nullValue = getNullValue<T>();
    for (int i = 0; i < num_rows; i++) {
      if (data_buffer[i] == std::nullopt) {
        clearBit(nulls, i);
        nullCount++;
      } else {
        setBit(nulls, i);
        srcValues[i] = *data_buffer[i];
      }
    }
    int64_t n_buffers = 2;
    const void** buffers =
        reinterpret_cast<const void**>(pool_->allocate(sizeof(void*) * n_buffers));
    buffers[0] = (nullCount == 0) ? nullptr : (const void*)nulls;
    buffers[1] = (num_rows == 0) ? nullptr : (const void*)srcValues;

    ArrowArray arrowArray = {
        num_rows,
        nullCount,
        0,
        n_buffers,
        0,
        buffers,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
    };

    ArrowSchema arrowSchema = {
        format,
        nullptr,
        nullptr,
        0,
        0,
        nullptr,
        nullptr,
        nullptr,
    };

    int8_t* results = convertToCider(arrowSchema, arrowArray, num_rows, pool_.get());
    T* col = reinterpret_cast<T*>(results);
    for (auto idx = 0; idx < num_rows; idx++) {
      if (data_buffer[idx] == std::nullopt) {
        EXPECT_EQ(nullValue, col[idx]);
      } else {
        EXPECT_EQ(*data_buffer[idx], col[idx]);
      }
    }
  }
};

TEST_F(ArrowUtilsTest, toArrowInteger) {
  int32_t* col = reinterpret_cast<int32_t*>(pool_->allocate(sizeof(int32_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col[i] = i;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col[i] = facebook::velox::plugin::inline_int_null_value<int32_t>();
  }
  int null_count = 3;
  const int8_t* col_buffer = reinterpret_cast<const int8_t*>(col);

  substrait::Type col_type;
  std::string type_json = R"(
    {
      "i32": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  testCiderResult<int32_t>(col_buffer, col_type, num_rows, null_count);
}

TEST_F(ArrowUtilsTest, toArrowDouble) {
  double* col = reinterpret_cast<double*>(pool_->allocate(sizeof(double) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col[i] = i * 3.14;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col[i] = DBL_MIN;
  }
  int null_count = 3;
  const int8_t* col_buffer = reinterpret_cast<const int8_t*>(col);

  substrait::Type col_type;
  std::string type_json = R"(
    {
      "fp64": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  testCiderResult<double>(col_buffer, col_type, num_rows, null_count);
}

TEST_F(ArrowUtilsTest, toArrowTimestamp) {
  int64_t* col = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col[i] = i + 86400000000;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col[i] = facebook::velox::plugin::inline_int_null_value<int64_t>();
  }
  int null_count = 3;
  const int8_t* col_buffer = reinterpret_cast<const int8_t*>(col);

  int32_t dimen = 6;
  substrait::Type col_type;
  std::string type_json = R"(
    {
      "timestamp": {
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  testCiderResult<int64_t>(col_buffer, col_type, num_rows, null_count, dimen);
}

TEST_F(ArrowUtilsTest, toArrowDecimal) {
  int64_t* col = reinterpret_cast<int64_t*>(pool_->allocate(sizeof(int64_t) * 10));
  int num_rows = 10;
  for (int i = 0; i < num_rows; i++) {
    col[i] = i * 1.00;
  }
  for (int i = 3; i < num_rows; i += 3) {
    col[i] = facebook::velox::plugin::inline_int_null_value<int64_t>();
  }
  int null_count = 3;
  const int8_t* col_buffer = reinterpret_cast<const int8_t*>(col);

  substrait::Type col_type;
  std::string type_json = R"(
    {
      "decimal": {
        "scale": 2,
        "precision": 19,
        "typeVariationReference": 0,
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
    )";
  google::protobuf::util::JsonStringToMessage(type_json, &col_type);
  testCiderResult<int64_t>(col_buffer, col_type, num_rows, null_count);
}

TEST_F(ArrowUtilsTest, toCiderInteger) {
  testArrowArray<int32_t>("i", {});
  testArrowArray<int32_t>("i", {std::nullopt});
  testArrowArray<int32_t>("i", {std::nullopt, std::nullopt});
  testArrowArray<int32_t>("i", {0, 2, 1, 3, 5, -1234, -99, -999, 1000, -1});
  testArrowArray<int32_t>(
      "i", {0, std::nullopt, 1, 3, std::nullopt, -1234, -99, -999, 1000, -1});
}

TEST_F(ArrowUtilsTest, toCiderDouble) {
  testArrowArray<double>("g", {});
  testArrowArray<double>("g", {std::nullopt});
  testArrowArray<double>("g", {std::nullopt, std::nullopt});
  testArrowArray<double>("g", {0.5, 1, 1024, -123456, -99.99});
  testArrowArray<double>(
      "g", {0.5, 1, std::nullopt, 3.14, 1024, -123456, -99.99, -999, std::nullopt, -1});
}

TEST_F(ArrowUtilsTest, toCiderTimestamp) {
  testArrowArray<int32_t>("tts", {});
  testArrowArray<int32_t>("ttm", {std::nullopt});
  testArrowArray<int32_t>("tts", {std::nullopt, std::nullopt});
  testArrowArray<int32_t>("tts", {94671, 28800, 9467, -21600, 95716});
  testArrowArray<int32_t>("ttm",
                          {94671,
                           28800,
                           std::nullopt,
                           94675,
                           1024,
                           -1234,
                           -21600,
                           95716,
                           std::nullopt,
                           7200});

  testArrowArray<int64_t>("ttn", {});
  testArrowArray<int64_t>("ttu", {std::nullopt});
  testArrowArray<int64_t>("ttn", {std::nullopt, std::nullopt});
  testArrowArray<int64_t>("ttn", {946713600, 28800, 946758116, -21600, 957164400});
  testArrowArray<int64_t>("ttu",
                          {946713600,
                           28800,
                           std::nullopt,
                           946758116,
                           1024,
                           -123456,
                           -21600,
                           957164400,
                           std::nullopt,
                           7200});
}

TEST_F(ArrowUtilsTest, toCiderDecimal) {
  testArrowArray<int64_t>("d:19,2", {});
  testArrowArray<int64_t>("d:19,2", {std::nullopt});
  testArrowArray<int64_t>("d:19,2", {std::nullopt, std::nullopt});
  testArrowArray<int64_t>("d:19,2", {0.5, 1, 1024, -123456, -99.99});
  testArrowArray<int64_t>(
      "d:19,2",
      {0.5, 1, std::nullopt, 3.14, 1024, -123456, -99.99, -999, std::nullopt, -1});
}
