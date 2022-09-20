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

#ifndef CIDER_QUERYDATAGENERATOR_H
#define CIDER_QUERYDATAGENERATOR_H

#include <limits>
#include <random>
#include <string>
#include "CiderBatchBuilder.h"
#include "Utils.h"
#include "cider/CiderBatch.h"
#include "cider/CiderTypes.h"
#include "substrait/type.pb.h"

enum GeneratePattern { Sequence, Random, Special_Date_format_String };

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
#define GENERATE_AND_ADD_BOOL_COLUMN(TYPE)                                       \
  {                                                                              \
    std::vector<TYPE> col_data;                                                  \
    std::vector<bool> null_data;                                                 \
    std::tie(col_data, null_data) =                                              \
        (value_min == value_max && value_min == 1) ||                            \
                (value_min == value_max && value_min == 0)                       \
            ? generateAndFillBoolVector<TYPE>(row_num,                           \
                                              GeneratePattern::Random,           \
                                              null_chance[i],                    \
                                              value_min,                         \
                                              value_min)                         \
            : generateAndFillBoolVector<TYPE>(row_num, pattern, null_chance[i]); \
    builder = builder.addColumn<TYPE>(names[i], type, col_data, null_data);      \
    break;                                                                       \
  }

#define GENERATE_AND_ADD_TIMING_COLUMN(TYPE)                                    \
  {                                                                             \
    std::vector<bool> null_data;                                                \
    std::vector<TYPE> col_data;                                                 \
    std::tie(col_data, null_data) =                                             \
        value_min > value_max                                                   \
            ? generateAndFillDateVector<TYPE>(row_num, pattern, null_chance[i]) \
            : generateAndFillDateVector<TYPE>(                                  \
                  row_num, pattern, null_chance[i], value_min, value_max);      \
    builder = builder.addColumn<TYPE>(names[i], type, col_data, null_data);     \
    break;                                                                      \
  }

#define GENERATE_AND_ADD_VARCHAR_COLUMN(TYPE)                               \
  {                                                                         \
    std::vector<bool> null_data;                                            \
    std::vector<TYPE> col_data;                                             \
    std::tie(col_data, null_data) = generateAndFillCharVector<TYPE>(        \
        row_num, pattern, null_chance[i], value_min, value_max);            \
    builder = builder.addColumn<TYPE>(names[i], type, col_data, null_data); \
    break;                                                                  \
  }

#define N_MAX std::numeric_limits<T>::max()

#define N_MIN std::numeric_limits<T>::min()

class QueryDataGenerator {
 public:
  static CiderBatch generateBatchByTypes(
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
    CiderBatchBuilder builder;
    builder = builder.setRowNum(row_num);
    for (auto i = 0; i < types.size(); ++i) {
      ::substrait::Type type = types[i];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
          GENERATE_AND_ADD_BOOL_COLUMN(int8_t)
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
          GENERATE_AND_ADD_VARCHAR_COLUMN(CiderByteArray)
        case ::substrait::Type::KindCase::kDate:
          GENERATE_AND_ADD_TIMING_COLUMN(CiderDateType)
        // FIXME(jikunshang): add timestamp support, Kaidi is WIP.
        case ::substrait::Type::KindCase::kTime:
          //          GENERATE_AND_ADD_TIMING_COLUMN(CiderTimeType)
        case ::substrait::Type::KindCase::kTimestamp:
          //          GENERATE_AND_ADD_TIMING_COLUMN(CiderTimeStampType)
        default:
          throw std::runtime_error("Type not supported.");
      }
    }
    auto batch = builder.build();
    return batch;
  }

  static std::vector<bool> generateAndFillNullVector(const size_t row_num,
                                                     const int32_t null_chance) {
    std::vector<bool> null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    for (auto i = 0; i < row_num; ++i) {
      null_data[i] = Random::oneIn(null_chance, rng) ? true : false;
    }
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
          null_data[i] = Random::oneIn(null_chance, rng) ? (col_data[i] = N_MIN, true)
                                                         : (col_data[i] = i, false);
        }
        break;
      case GeneratePattern::Special_Date_format_String:
      case GeneratePattern::Random:
        if (std::is_integral<T>::value) {
          // default type is int32_t. should not replace with T due to cannot gen float
          // type template. Same for below.
          for (auto i = 0; i < col_data.size(); ++i) {
            null_data[i] = Random::oneIn(null_chance, rng)
                               ? (col_data[i] = N_MIN, true)
                               : (col_data[i] = static_cast<T>(
                                      Random::randInt64(value_min, value_max, rng)),
                                  false);
          }
        } else if (std::is_floating_point<T>::value) {
          for (auto i = 0; i < col_data.size(); ++i) {
            null_data[i] = Random::oneIn(null_chance, rng)
                               ? (col_data[i] = N_MIN, true)
                               : (col_data[i] = static_cast<T>(
                                      Random::randFloat(value_min, value_max, rng)),
                                  false);
          }
        } else {
          std::string str = "Unexpected type:";
          str.append(typeid(T).name()).append(", could not generate data.");
          throw std::runtime_error(str);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }

  static CiderByteArray genRandomCiderByteArray(int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    int mod = sizeof(alphanum) - 1;
    char* buf = (char*)std::malloc(len);
    for (int i = 0; i < len; i++) {
      buf[i] = alphanum[rand() % mod];
    }
    return CiderByteArray(len, (const uint8_t*)buf);
  }

  static CiderByteArray genSequenceCiderByteArray(int len, int index) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    int mod = sizeof(alphanum) - 1;
    char* buf = (char*)std::malloc(len);
    for (int i = 0; i < len; i++) {
      buf[i] = alphanum[index % mod];
    }
    return CiderByteArray(len, (const uint8_t*)buf);
  }

  static CiderByteArray genDateFormatCiderByteArray(int len) {
    char* buf = (char*)std::malloc(len);
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
    buf[8] = base + month / 10;
    buf[9] = base + month % 10;
    return CiderByteArray(len, (const uint8_t*)buf);
  }

  template <typename T,
            std::enable_if_t<std::is_same<T, CiderByteArray>::value, bool> = true>
  static std::tuple<std::vector<CiderByteArray>, std::vector<bool>>
  generateAndFillCharVector(const size_t row_num,
                            const GeneratePattern pattern,
                            const int32_t null_chance,
                            const int64_t min_len = 0,
                            const int64_t max_len = -1) {
    CHECK_GE(min_len, 0);
    const int default_char_len = (max_len < min_len || max_len <= 0) ? 10 : max_len;
    std::vector<CiderByteArray> col_data(row_num);
    std::vector<bool> null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    switch (pattern) {
      case GeneratePattern::Sequence:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] =
              Random::oneIn(null_chance, rng)
                  ? (col_data[i] = CiderByteArray(), true)
                  : (col_data[i] = genSequenceCiderByteArray(default_char_len, i), false);
        }
        break;
      case GeneratePattern::Random:
        for (auto i = 0; i < row_num; ++i) {
          int len = rand() % (default_char_len - min_len + 1) + min_len;  // NOLINT
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = CiderByteArray(), true)
                             : (col_data[i] = genRandomCiderByteArray(len), false);
        }
        break;
      case GeneratePattern::Special_Date_format_String:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = CiderByteArray(), true)
                             : (col_data[i] = genDateFormatCiderByteArray(10), false);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }

  template <typename T, std::enable_if_t<std::is_same<T, int8_t>::value, bool> = true>
  static std::tuple<std::vector<int8_t>, std::vector<bool>> generateAndFillBoolVector(
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
          null_data[i] = Random::oneIn(null_chance, rng) ? (col_data[i] = N_MIN, true)
                                                         : (col_data[i] = i % 2, false);
        }
        break;
      case GeneratePattern::Special_Date_format_String:
      case GeneratePattern::Random:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data[i] = N_MIN, true)
                             : (col_data[i] = static_cast<T>(
                                    Random::randInt64(value_min, value_max, rng)),
                                false);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }

#define MIN_DAYS -1000 * 365
#define MAX_DAYS 1000 * 365
  // For CiderDateType, random value is days(rather than seconds, will multiply manually)
  template <typename T,
            std::enable_if_t<std::is_same<T, CiderDateType>::value, bool> = true>
  static std::tuple<std::vector<CiderDateType>, std::vector<bool>>
  generateAndFillDateVector(const size_t row_num,
                            const GeneratePattern pattern,
                            const int32_t null_chance,
                            const int64_t value_min = MIN_DAYS,
                            const int64_t value_max = MAX_DAYS) {
    std::vector<CiderDateType> col_data;
    col_data.reserve(row_num);
    std::vector<bool> null_data(row_num);
    std::mt19937 rng(std::random_device{}());  // NOLINT
    switch (pattern) {
      case GeneratePattern::Sequence:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] =
              Random::oneIn(null_chance, rng)
                  ? (col_data.push_back(
                         CiderDateType(std::numeric_limits<int64_t>::min())),
                     true)
                  : (col_data.push_back(CiderDateType(i * kSecondsInOneDay)), false);
        }
        break;
      case GeneratePattern::Special_Date_format_String:
      case GeneratePattern::Random:
        for (auto i = 0; i < row_num; ++i) {
          null_data[i] = Random::oneIn(null_chance, rng)
                             ? (col_data.push_back(
                                    CiderDateType(std::numeric_limits<int64_t>::min())),
                                true)
                             : (col_data.push_back(CiderDateType(
                                    kSecondsInOneDay *
                                    Random::randInt64(value_min, value_max, rng))),
                                false);
        }
        break;
    }
    return std::make_tuple(col_data, null_data);
  }
};

// NOTE: decimal => double, data => int, VARCHAR/Char(n) => VarChar.
// Please update this class when these types are supported.
class TpcHDataGenerator : public QueryDataGenerator {
 public:
  static CiderBatch genLineitem(const int row_num,
                                std::vector<int32_t> null_chance = {},
                                GeneratePattern pattern = GeneratePattern::Sequence) {
    std::vector<std::string> names{"L_ORDERKEY",
                                   "L_PARTKEY",
                                   "L_SUPPKEY",
                                   "L_LINENUMBER",
                                   "L_QUANTITY",
                                   "L_EXTENDEDPRICE",
                                   "L_DISCOUNT",
                                   "L_TAX",
                                   "L_RETURNFLAG",
                                   "L_LINESTATUS",
                                   "L_SHIPDATE",
                                   "L_COMMITDATE",
                                   "L_RECEIPTDATE",
                                   "L_SHIPINSTRUCT",
                                   "L_SHIPMODE",
                                   "L_COMMENT"};

    std::vector<::substrait::Type> types{
        CREATE_SUBSTRAIT_TYPE(I64),      // bigint
        CREATE_SUBSTRAIT_TYPE(I64),      // bigint
        CREATE_SUBSTRAIT_TYPE(I64),      // bigint
        CREATE_SUBSTRAIT_TYPE(I32),      // int
        CREATE_SUBSTRAIT_TYPE(Fp64),     // decimal
        CREATE_SUBSTRAIT_TYPE(Fp64),     // decimal
        CREATE_SUBSTRAIT_TYPE(Fp64),     // decimal
        CREATE_SUBSTRAIT_TYPE(Fp64),     // decimal
        CREATE_SUBSTRAIT_TYPE(Varchar),  // char(1)
        CREATE_SUBSTRAIT_TYPE(Varchar),  // char(1)
        CREATE_SUBSTRAIT_TYPE(Date),     // date
        CREATE_SUBSTRAIT_TYPE(Date),     // date
        CREATE_SUBSTRAIT_TYPE(Date),     // date
        CREATE_SUBSTRAIT_TYPE(Varchar),  // char(25)
        CREATE_SUBSTRAIT_TYPE(Varchar),  // char(10)
        CREATE_SUBSTRAIT_TYPE(Varchar),  // char(44)
    };

    if (null_chance.size() == 0) {
      null_chance = std::vector<int32_t>(types.size(), 0);
    }
    return generateBatchByTypes(row_num, names, types, null_chance, pattern);
  }
};
#endif  // CIDER_QUERYDATAGENERATOR_H
