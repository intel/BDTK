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

// TODO: generate VARCHAR DATA&TIME column

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
};

#endif  // CIDER_QUERYARROWDATAGENERATOR_H
