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

#ifndef CIDER_QUERYARROWDATAGENERATOR_H
#define CIDER_QUERYARROWDATAGENERATOR_H

#include <limits>
#include <random>
#include <string>
#include "ArrowArrayBuilder.h"
#include "QueryDataGenerator.h"
#include "Utils.h"
#include "cider/CiderBatch.h"
#include "cider/CiderTypes.h"
#include "substrait/type.pb.h"

// test date data within [970-01-01..2970-01-01].
static constexpr int32_t kMinDay = -1000 * 365;
static constexpr int32_t kMaxDay = 1000 * 365;

// test time data since the beginning of any day range of [0..86,399,999,999] microseconds
static constexpr int64_t kMinTime = 0;
static constexpr int64_t kMaxTime = 86399999999;

// test timestamp data within [970-01-01 00:00:00.000000..2970-01-01 23:59:59.999999],
// with microsecond precision.
static constexpr int64_t kMinTimestamp =
    kMinDay * kSecondsInOneDay * kMicrosecondsInSecond;
static constexpr int64_t kMaxTimestamp =
    kMaxDay * kSecondsInOneDay * kMicrosecondsInSecond;

// TODO(yizhong): Enable this after QueryDataGenerator is deleted.
// enum GeneratePatternArrow { SequenceArrow, RandomArrow };

#define GENERATE_AND_ADD_COLUMN(C_TYPE)                                       \
  {                                                                           \
    std::vector<C_TYPE> col_data;                                             \
    std::vector<bool> null_data;                                              \
    std::tie(col_data, null_data) =                                           \
        value_min > value_max                                                 \
            ? generateAndFillVector<C_TYPE>(row_num, pattern, null_chance[i]) \
            : generateAndFillVector<C_TYPE>(                                  \
                  row_num, pattern, null_chance[i], value_min, value_max);    \
    builder = builder.addColumn<C_TYPE>(names[i], type, col_data, null_data); \
    break;                                                                    \
  }

// set value_min and value_max both to 0 or 1 to generate all true or all false vector.
// Otherwise, the vector would contain both
#define GENERATE_AND_ADD_BOOL_COLUMN(C_TYPE)                                       \
  {                                                                                \
    std::vector<C_TYPE> col_data;                                                  \
    std::vector<bool> null_data;                                                   \
    std::tie(col_data, null_data) =                                                \
        (value_min == value_max && value_min == 1) ||                              \
                (value_min == value_max && value_min == 0)                         \
            ? generateAndFillBoolVector<C_TYPE>(row_num,                           \
                                                GeneratePattern::Random,           \
                                                null_chance[i],                    \
                                                value_min,                         \
                                                value_min)                         \
            : generateAndFillBoolVector<C_TYPE>(row_num, pattern, null_chance[i]); \
    builder = builder.addBoolColumn<C_TYPE>(names[i], col_data, null_data);        \
    break;                                                                         \
  }

#define GENERATE_AND_ADD_UTF8_COLUMN()                                           \
  {                                                                              \
    std::vector<bool> null_data;                                                 \
    std::vector<int32_t> offset_data;                                            \
    std::string col_data;                                                        \
    std::tie(null_data, offset_data, col_data) = generateAndFillStringVector(    \
        row_num, pattern, null_chance[i], value_min, value_max);                 \
    builder = builder.addUTF8Column(names[i], col_data, offset_data, null_data); \
    break;                                                                       \
  }

#define GENERATE_AND_ADD_TIMING_COLUMN(C_TYPE, DEFAULT_MIN_VAL, DEFAULT_MAX_VAL)      \
  {                                                                                   \
    std::vector<C_TYPE> col_data;                                                     \
    std::vector<bool> null_data;                                                      \
    std::tie(col_data, null_data) =                                                   \
        value_min > value_max                                                         \
            ? generateAndFillVector<C_TYPE>(                                          \
                  row_num, pattern, null_chance[i], DEFAULT_MIN_VAL, DEFAULT_MAX_VAL) \
            : generateAndFillVector<C_TYPE>(                                          \
                  row_num, pattern, null_chance[i], value_min, value_max);            \
    builder = builder.addColumn<C_TYPE>(names[i], type, col_data, null_data);         \
    break;                                                                            \
  }

#define GENERATE_AND_ADD_ARRAY_COLUMN(C_TYPE)                                      \
  {                                                                                \
    std::vector<std::vector<C_TYPE>> col_data;                                     \
    std::vector<bool> null_data;                                                   \
    std::vector<std::vector<bool>> array_null_data;                                \
    std::tie(col_data, null_data, array_null_data) =                               \
        value_min > value_max                                                      \
            ? generateAndFillArrayVector<C_TYPE>(row_num, pattern, null_chance[i]) \
            : generateAndFillArrayVector<C_TYPE>(                                  \
                  row_num, pattern, null_chance[i], value_min, value_max);         \
    builder = builder.addSingleDimensionArrayColumn<C_TYPE>(                       \
        names[i], type, col_data, null_data, array_null_data);                     \
    break;                                                                         \
  }

#define N_MAX std::numeric_limits<T>::max()

#define N_MIN std::numeric_limits<T>::min()

class QueryArrowDataGenerator {
 public:
  static void generateBatchByTypes(ArrowSchema*& schema,
                                   ArrowArray*& array,
                                   const size_t row_num,
                                   const std::vector<std::string>& names,
                                   const std::vector<::substrait::Type>& types,
                                   std::vector<int32_t> null_chance = {},
                                   GeneratePattern pattern = GeneratePattern::Sequence,
                                   const int64_t value_min = 0,
                                   const int64_t value_max = -1) {
    if (null_chance.empty()) {
      null_chance = std::vector<int32_t>(types.size(), 0);
    }
    ArrowArrayBuilder builder;
    builder = builder.setRowNum(row_num);
    for (auto i = 0; i < types.size(); ++i) {
      ::substrait::Type type = types[i];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
          GENERATE_AND_ADD_BOOL_COLUMN(bool)
        case ::substrait::Type::KindCase::kI8:
          GENERATE_AND_ADD_COLUMN(int8_t)
        case ::substrait::Type::KindCase::kI16:
          GENERATE_AND_ADD_COLUMN(int16_t)
        case ::substrait::Type::KindCase::kI32:
          GENERATE_AND_ADD_COLUMN(int32_t)
        case ::substrait::Type::KindCase::kI64:
          GENERATE_AND_ADD_COLUMN(int64_t)
        case ::substrait::Type::KindCase::kFp32:
          GENERATE_AND_ADD_COLUMN(float)
        case ::substrait::Type::KindCase::kFp64:
          GENERATE_AND_ADD_COLUMN(double)
        case ::substrait::Type::KindCase::kString:
        case ::substrait::Type::KindCase::kVarchar:
        case ::substrait::Type::KindCase::kFixedChar:
          GENERATE_AND_ADD_UTF8_COLUMN()
        case ::substrait::Type::KindCase::kDate:
          GENERATE_AND_ADD_TIMING_COLUMN(int32_t, kMinDay, kMaxDay)
        case ::substrait::Type::KindCase::kTime:
          GENERATE_AND_ADD_TIMING_COLUMN(int64_t, kMinTime, kMaxTime)
        case ::substrait::Type::KindCase::kTimestamp:
          GENERATE_AND_ADD_TIMING_COLUMN(int64_t, kMinTimestamp, kMaxTimestamp)
        case ::substrait::Type::KindCase::kList:
          switch (type.list().type().kind_case()) {
            case ::substrait::Type::KindCase::kI8:
              GENERATE_AND_ADD_ARRAY_COLUMN(int8_t)
            case ::substrait::Type::KindCase::kI16:
              GENERATE_AND_ADD_ARRAY_COLUMN(int16_t)
            case ::substrait::Type::KindCase::kI32:
              GENERATE_AND_ADD_ARRAY_COLUMN(int32_t)
            case ::substrait::Type::KindCase::kI64:
              GENERATE_AND_ADD_ARRAY_COLUMN(int64_t)
            case ::substrait::Type::KindCase::kFp32:
              GENERATE_AND_ADD_ARRAY_COLUMN(float)
            case ::substrait::Type::KindCase::kFp64:
              GENERATE_AND_ADD_ARRAY_COLUMN(double)
            default:
              CIDER_THROW(CiderCompileException, "Type not supported.");
          }
          break;
        default:
          CIDER_THROW(CiderCompileException, "Type not supported.");
      }
    }

    auto schema_and_array = builder.build();
    schema = std::get<0>(schema_and_array);
    array = std::get<1>(schema_and_array);
  }

 private:
  template <typename T>
  static std::tuple<std::vector<T>, std::vector<bool>> generateAndFillVector(
      const size_t row_num,
      const GeneratePattern pattern,
      const int32_t
          null_chance,  // Null chance for each column, -1 represents for unnullable
                        // column, 0 represents for nullable column but all data is not
                        // null, 1 represents for all rows are null, values >= 2 means
                        // each row has 1/x chance to be null.
      const T value_min = N_MIN,
      const T value_max = N_MAX) {
    std::vector<T> col_data(row_num);
    std::vector<bool> null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    switch (pattern) {
      case GeneratePattern::Sequence:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? (col_data[i] = 0, true)
                                                         : (col_data[i] = i, false);
        }
        break;
      case GeneratePattern::Random:
        if (std::is_integral<T>::value) {
          // default type is int32_t. should not replace with T due to cannot gen float
          // type template. Same for below.
          for (auto i = 0; i < col_data.size(); ++i) {
            null_data[i] = Random::oneIn(null_chance, rng)
                               ? (col_data[i] = 0, true)
                               : (col_data[i] = static_cast<T>(
                                      Random::randInt64(value_min, value_max, rng)),
                                  false);
          }
        } else if (std::is_floating_point<T>::value) {
          for (auto i = 0; i < col_data.size(); ++i) {
            null_data[i] = Random::oneIn(null_chance, rng)
                               ? (col_data[i] = 0, true)
                               : (col_data[i] = static_cast<T>(
                                      Random::randFloat(value_min, value_max, rng)),
                                  false);
          }
        } else {
          std::string str = "Unexpected type:";
          str.append(typeid(T).name()).append(", could not generate data.");
          CIDER_THROW(CiderCompileException, str);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }

  template <typename T, std::enable_if_t<std::is_same<T, bool>::value, bool> = true>
  static std::tuple<std::vector<bool>, std::vector<bool>> generateAndFillBoolVector(
      const size_t row_num,
      const GeneratePattern pattern,
      const int32_t null_chance,
      const int64_t value_min = 0,
      const int64_t value_max = 1) {
    std::vector<T> col_data(row_num);
    std::vector<bool> null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    switch (pattern) {
      case GeneratePattern::Sequence:
        for (auto i = 0; i < row_num; ++i) {
          // for Sequence pattern, the boolean values will be cross-generated
          null_data[i] = Random::oneIn(null_chance, rng) ? (col_data[i] = 0, true)
                                                         : (col_data[i] = i % 2, false);
        }
        break;
      case GeneratePattern::Random:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = 0, true)
                             : (col_data[i] = static_cast<T>(
                                    Random::randInt64(value_min, value_max, rng)),
                                false);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }

  static std::tuple<std::vector<bool>, std::vector<int32_t>, std::string>
  generateAndFillStringVector(const size_t row_num,
                              const GeneratePattern pattern,
                              const int32_t null_chance,
                              const int64_t min_len = 0,
                              const int64_t max_len = 0) {
    std::string col_data = "";
    std::vector<bool> null_data(row_num);
    std::vector<int32_t> offset_data;
    std::mt19937 rng(std::random_device{}());  // NOLINT
    size_t default_strlen = (max_len < min_len || max_len <= 0) ? 10 : max_len;
    switch (pattern) {
      case GeneratePattern::Sequence:
        offset_data.push_back(0);
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
          col_data += (sequence_string(default_strlen, i));
          offset_data.push_back(offset_data[i] + default_strlen);
        }
        break;
      case GeneratePattern::Random:
        offset_data.push_back(0);
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
          size_t str_len = (max_len - min_len + 1) > 0
                               ? (rand() % default_strlen)  // NOLINT
                               : default_strlen;
          col_data += (random_string(str_len));
          offset_data.push_back(offset_data[i] + str_len);
        }
        break;
      case GeneratePattern::Special_Date_format_String:
        offset_data.push_back(0);
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
          col_data += (gen_date_format_string(default_strlen));
          offset_data.push_back(offset_data[i] + default_strlen);
        }
        break;
    }
    return std::make_tuple(null_data, offset_data, col_data);
  }

  template <typename T>
  static std::tuple<std::vector<std::vector<T>>,
                    std::vector<bool>,
                    std::vector<std::vector<bool>>>
  generateAndFillArrayVector(
      const size_t row_num,
      const GeneratePattern pattern,
      const int32_t
          null_chance,  // Null chance for each column, -1 represents for unnullable
                        // column, 0 represents for nullable column but all data is not
                        // null, 1 represents for all rows are null, values >= 2 means
                        // each row has 1/x chance to be null.
      const T value_min = N_MIN,
      const T value_max = N_MAX) {
    std::vector<std::vector<T>> col_data(row_num);
    std::vector<bool> null_data(row_num);
    std::vector<std::vector<bool>> array_null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    switch (pattern) {
      case GeneratePattern::Random:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
          if (!null_data[i]) {
            auto arr_len = static_cast<T>(Random::randInt64(0, 10, rng));
            for (auto j = 0; j < arr_len; j++) {
              array_null_data[i].push_back(
                  Random::oneIn(null_chance, rng)
                      ? (col_data[i].push_back(0), true)
                      : (col_data[i].push_back(static_cast<T>(
                             Random::randInt64(value_min, value_max, rng))),
                         false));
            }
          }
        }
        break;
      case GeneratePattern::Sequence:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
          if (!null_data[i]) {
            for (auto j = 0; j < 10; j++) {
              array_null_data[i].push_back(Random::oneIn(null_chance, rng)
                                               ? (col_data[i].push_back(0), true)
                                               : (col_data[i].push_back(j), false));
            }
          }
        }
        break;
    }
    return std::make_tuple(col_data, null_data, array_null_data);
  }

  static std::string random_string(size_t length) {
    auto randchar = []() -> char {
      const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
      const size_t max_index = (sizeof(charset) - 1);
      return charset[rand() % max_index];  // NOLINT
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
  }

  static std::string sequence_string(size_t length, size_t index) {
    auto randchar = [index]() -> char {
      const char charset[] =
          "0123456789"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";
      const size_t mod = (sizeof(charset) - 1);
      return charset[index % mod];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
  }

  static std::string gen_date_format_string(size_t length) {
    std::string buf(length, 0);
    char base = '0';
    std::mt19937 rng(std::random_device{}());  // NOLINT
    buf[0] = base + Random::randInt32(1, 2, rng);
    buf[1] = base + Random::randInt32(0, 9, rng);
    buf[2] = base + Random::randInt32(0, 9, rng);
    buf[3] = base + Random::randInt32(0, 9, rng);
    buf[4] = '-';
    int month = Random::randInt32(1, 12, rng);
    buf[5] = base + month / 10;
    buf[6] = base + month % 10;
    buf[7] = '-';
    int day = Random::randInt32(1, 28, rng);
    buf[8] = base + day / 10;
    buf[9] = base + day % 10;
    return buf;
  }
};

#endif  // CIDER_QUERYARROWDATAGENERATOR_H
