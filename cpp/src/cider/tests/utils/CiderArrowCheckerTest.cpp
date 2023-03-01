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

#include "tests/utils/CiderArrowChecker.h"

#include <gtest/gtest.h>

#include "exec/plan/parser/TypeUtils.h"
#include "util/ArrowArrayBuilder.h"

using namespace cider::test::util;

TEST(CiderArrowCheckerTest, withoutNullCheck) {
  std::vector<bool> bool_vector{
      true, false, true, false, true, false, true, false, true, false};
  std::vector<int8_t> i8_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<int16_t> i16_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<int32_t> i32_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<int64_t> i64_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  ArrowArray* expect_array = nullptr;
  ArrowSchema* expect_schema = nullptr;
  std::tie(expect_schema, expect_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_vector)
          .build();

  ArrowArray* actual_array = nullptr;
  ArrowSchema* actual_schema = nullptr;
  std::tie(actual_schema, actual_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_vector)
          .build();

  EXPECT_TRUE(CiderArrowChecker::checkArrowEq(
      expect_array, actual_array, expect_schema, actual_schema));
  actual_array->release(actual_array);
  actual_schema->release(actual_schema);

  std::vector<int64_t> i64_noeq_vector{1, 2, 3, 0, 4, 5, 6, 7, 8, 9};
  std::tie(actual_schema, actual_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_noeq_vector)
          .build();
  EXPECT_FALSE(CiderArrowChecker::checkArrowEq(
      expect_array, actual_array, expect_schema, actual_schema));
  actual_array->release(actual_array);
  actual_schema->release(actual_schema);
  expect_array->release(expect_array);
  expect_schema->release(expect_schema);
}

TEST(CiderArrowCheckerTest, withNullCheck) {
  std::vector<bool> bool_vector{
      true, false, true, false, true, false, true, false, true, false};
  std::vector<bool> bool_null{
      true, true, true, true, true, false, false, false, false, true};
  std::vector<int8_t> i8_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<bool> i8_null{
      true, false, true, true, true, false, false, false, true, false};
  std::vector<int16_t> i16_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<bool> i16_null{
      true, true, false, true, true, false, false, true, false, false};
  std::vector<int32_t> i32_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<bool> i32_null{
      true, true, true, false, true, false, true, false, false, false};
  std::vector<int64_t> i64_vector{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<bool> i64_null{
      true, true, true, true, false, true, false, false, false, false};

  ArrowArray* expect_array = nullptr;
  ArrowSchema* expect_schema = nullptr;
  std::tie(expect_schema, expect_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector, i8_null)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector, i16_null)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector, i32_null)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_vector, i64_null)
          .build();

  ArrowArray* actual_array = nullptr;
  ArrowSchema* actual_schema = nullptr;
  std::tie(actual_schema, actual_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector, i8_null)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector, i16_null)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector, i32_null)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_vector, i64_null)
          .build();

  EXPECT_TRUE(CiderArrowChecker::checkArrowEq(
      expect_array, actual_array, expect_schema, actual_schema));
  actual_array->release(actual_array);
  actual_schema->release(actual_schema);

  std::tie(actual_schema, actual_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector, i8_null)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector, i16_null)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector, i64_null)
          .addColumn<int64_t>("col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_vector, i64_null)
          .build();
  EXPECT_FALSE(CiderArrowChecker::checkArrowEq(
      expect_array, actual_array, expect_schema, actual_schema));
  actual_array->release(actual_array);
  actual_schema->release(actual_schema);

  std::vector<int64_t> i64_noeq_vector{1, 2, 3, 0, 4, 5, 6, 7, 8, 9};
  std::vector<bool> i64_noeq_null{
      true, true, true, false, false, true, false, false, false, false};
  std::tie(actual_schema, actual_array) =
      ArrowArrayBuilder()
          .setRowNum(10)
          .addColumn<int8_t>("col_i8", CREATE_SUBSTRAIT_TYPE(I8), i8_vector, i8_null)
          .addColumn<int16_t>("col_i16", CREATE_SUBSTRAIT_TYPE(I16), i16_vector, i16_null)
          .addColumn<int32_t>("col_i32", CREATE_SUBSTRAIT_TYPE(I32), i32_vector, i32_null)
          .addColumn<int64_t>(
              "col_i64", CREATE_SUBSTRAIT_TYPE(I64), i64_noeq_vector, i64_noeq_null)
          .build();
  EXPECT_FALSE(CiderArrowChecker::checkArrowEq(
      expect_array, actual_array, expect_schema, actual_schema));
  actual_array->release(actual_array);
  actual_schema->release(actual_schema);
  expect_array->release(expect_array);
  expect_schema->release(expect_schema);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
