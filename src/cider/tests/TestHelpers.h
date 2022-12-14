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

#ifndef TEST_HELPERS_H_
#define TEST_HELPERS_H_

#include "exec/template/TargetValue.h"
#include "util/Logger.h"

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/variant.hpp>

namespace TestHelpers {

template <class T>
void compare_array(const TargetValue& r,
                   const std::vector<T>& arr,
                   const double tol = -1.) {
  auto array_tv = boost::get<ArrayTargetValue>(&r);
  CHECK(array_tv);
  if (!array_tv->is_initialized()) {
    ASSERT_EQ(size_t(0), arr.size());
    return;
  }
  const auto& scalar_tv_vector = array_tv->get();
  ASSERT_EQ(scalar_tv_vector.size(), arr.size());
  size_t ctr = 0;
  for (const ScalarTargetValue& scalar_tv : scalar_tv_vector) {
    auto p = boost::get<T>(&scalar_tv);
    CHECK(p);
    if (tol < 0.) {
      ASSERT_EQ(*p, arr[ctr++]);
    } else {
      ASSERT_NEAR(*p, arr[ctr++], tol);
    }
  }
}

template <>
void compare_array(const TargetValue& r,
                   const std::vector<std::string>& arr,
                   const double tol) {
  auto array_tv = boost::get<ArrayTargetValue>(&r);
  CHECK(array_tv);
  if (!array_tv->is_initialized()) {
    ASSERT_EQ(size_t(0), arr.size());
    return;
  }
  const auto& scalar_tv_vector = array_tv->get();
  ASSERT_EQ(scalar_tv_vector.size(), arr.size());
  size_t ctr = 0;
  for (const ScalarTargetValue& scalar_tv : scalar_tv_vector) {
    auto ns = boost::get<NullableString>(&scalar_tv);
    CHECK(ns);
    auto str = boost::get<std::string>(ns);
    CHECK(str);
    ASSERT_TRUE(*str == arr[ctr++]);
  }
}

template <class T>
void compare_array(const std::vector<T>& a,
                   const std::vector<T>& b,
                   const double tol = -1.) {
  CHECK_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); i++) {
    if (tol < 0.) {
      ASSERT_EQ(a[i], b[i]);
    } else {
      ASSERT_NEAR(a[i], b[i], tol);
    }
  }
}

template <class T>
T v(const TargetValue& r) {
  auto scalar_r = boost::get<ScalarTargetValue>(&r);
  CHECK(scalar_r);
  auto p = boost::get<T>(scalar_r);
  CHECK(p);
  return *p;
}

template <typename T>
inline std::string convert(const T& t) {
  return std::to_string(t);
}

template <std::size_t N>
inline std::string convert(const char (&t)[N]) {
  return std::string(t);
}

template <>
inline std::string convert(const std::string& t) {
  return t;
}

bool is_null_tv(const TargetValue& tv, const SQLTypeInfo& ti) {
  if (ti.get_notnull()) {
    return false;
  }
  const auto scalar_tv = boost::get<ScalarTargetValue>(&tv);
  if (!scalar_tv) {
    CHECK(ti.is_array());
    const auto array_tv = boost::get<ArrayTargetValue>(&tv);
    CHECK(array_tv);
    return !array_tv->is_initialized();
  }
  if (boost::get<int64_t>(scalar_tv)) {
    int64_t data = *(boost::get<int64_t>(scalar_tv));
    switch (ti.get_type()) {
      case kBOOLEAN:
        return data == NULL_BOOLEAN;
      case kTINYINT:
        return data == NULL_TINYINT;
      case kSMALLINT:
        return data == NULL_SMALLINT;
      case kINT:
        return data == NULL_INT;
      case kDECIMAL:
      case kNUMERIC:
      case kBIGINT:
        return data == NULL_BIGINT;
      case kTIME:
      case kTIMESTAMP:
      case kDATE:
      case kINTERVAL_DAY_TIME:
      case kINTERVAL_YEAR_MONTH:
        return data == NULL_BIGINT;
      default:
        CHECK(false);
    }
  } else if (boost::get<double>(scalar_tv)) {
    double data = *(boost::get<double>(scalar_tv));
    if (ti.get_type() == kFLOAT) {
      return data == NULL_FLOAT;
    } else {
      return data == NULL_DOUBLE;
    }
  } else if (boost::get<float>(scalar_tv)) {
    CHECK_EQ(kFLOAT, ti.get_type());
    float data = *(boost::get<float>(scalar_tv));
    return data == NULL_FLOAT;
  } else if (boost::get<NullableString>(scalar_tv)) {
    auto s_n = boost::get<NullableString>(scalar_tv);
    auto s = boost::get<std::string>(s_n);
    return !s;
  }
  CHECK(false);
  return false;
}

struct ValuesGenerator {
  explicit ValuesGenerator(const std::string& table_name) : table_name_(table_name) {}

  template <typename... COL_ARGS>
  std::string operator()(COL_ARGS&&... args) const {
    std::vector<std::string> vals({convert(std::forward<COL_ARGS>(args))...});
    return std::string("INSERT INTO " + table_name_ + " VALUES(" +
                       boost::algorithm::join(vals, ",") + ");");
  }

  const std::string table_name_;
};

void init_logger_stderr_only(int argc, char const* const* argv) {
  logger::LogOptions log_options(argv[0]);
  log_options.max_files_ = 0;  // stderr only by default
  log_options.parse_command_line(argc, argv);
  logger::init(log_options);
}

void init_logger_stderr_only() {
  logger::LogOptions log_options(nullptr);
  log_options.max_files_ = 0;  // stderr only by default
  logger::init(log_options);
}

struct SharedDictionaryInfo {
  const std::string col;
  const std::string ref_table;
  const std::string ref_col;
};

std::string build_create_table_statement(
    const std::string& columns_definition,
    const std::string& table_name,
    const std::vector<SharedDictionaryInfo>& shared_dict_info,
    const size_t fragment_size,
    const bool use_temporary_tables,
    const bool replicated = false) {
  std::vector<std::string> shared_dict_def;
  if (shared_dict_info.size() > 0) {
    for (size_t idx = 0; idx < shared_dict_info.size(); ++idx) {
      shared_dict_def.push_back(", SHARED DICTIONARY (" + shared_dict_info[idx].col +
                                ") REFERENCES " + shared_dict_info[idx].ref_table + "(" +
                                shared_dict_info[idx].ref_col + ")");
    }
  }

  std::ostringstream with_statement_assembly;
  with_statement_assembly << "fragment_size=" << fragment_size;

  const std::string replicated_def{(!replicated) ? "" : ", PARTITIONS='REPLICATED' "};

  const std::string create_def{use_temporary_tables ? "CREATE TEMPORARY TABLE "
                                                    : "CREATE TABLE "};

  return create_def + table_name + "(" + columns_definition +
         boost::algorithm::join(shared_dict_def, "") + ") WITH (" +
         with_statement_assembly.str() + replicated_def + ");";
}

}  // namespace TestHelpers

#endif  // TEST_HELPERS_H_
