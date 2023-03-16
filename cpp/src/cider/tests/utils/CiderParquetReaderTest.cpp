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

#include <gtest/gtest.h>
#include "tests/utils/CiderArrowChecker.h"
#include "util/CiderParquetReader.h"

using namespace CiderParquetReader;
using namespace cider::test::util;

std::string getParquetFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/../parquet_files/";
}

// ┌──────────┐
// │ col_bool │
// ├──────────┤
// │ true     │
// │          │
// │ false    │
// │          │
// │ true     │
// └──────────┘
TEST(CiderParquetReaderTest, bool_single_col) {
  Reader* reader = new Reader();
  reader->init(getParquetFilesPath() + "bool_single_col.parquet", {"col_bool"}, 0, 1);
  ArrowSchema* actual_schema;
  ArrowArray* actual_array;
  reader->readBatch(5, actual_schema, actual_array);
  auto schema_and_array =
      ArrowArrayBuilder()
          .setRowNum(5)
          .addBoolColumn<bool>(
              "", {true, true, false, false, true}, {false, true, false, true, false})
          .build();
  CHECK(CiderArrowChecker::checkArrowEq(std::get<1>(schema_and_array),
                                        actual_array,
                                        std::get<0>(schema_and_array),
                                        actual_schema));
}

// ┌──────────┐
// │ col_type │
// ├──────────┤
// │ 0        │
// │          │
// │ 2        │
// │          │
// │ 4        │
// └──────────┘
#define GENERATE_READER_TEST(type_name, substrait_type, c_type)               \
  TEST(CiderParquetReaderTest, type_name##_single_col) {                      \
    Reader* reader = new Reader();                                            \
    reader->init(getParquetFilesPath() + "" #type_name "_single_col.parquet", \
                 {"col_" #type_name ""},                                      \
                 0,                                                           \
                 1);                                                          \
    ArrowSchema* actual_schema;                                               \
    ArrowArray* actual_array;                                                 \
    reader->readBatch(5, actual_schema, actual_array);                        \
    auto schema_and_array =                                                   \
        ArrowArrayBuilder()                                                   \
            .setRowNum(5)                                                     \
            .addColumn<c_type>("",                                            \
                               CREATE_SUBSTRAIT_TYPE(substrait_type),         \
                               {0, 1, 2, 3, 4},                               \
                               {false, true, false, true, false})             \
            .build();                                                         \
    CHECK(CiderArrowChecker::checkArrowEq(std::get<1>(schema_and_array),      \
                                          actual_array,                       \
                                          std::get<0>(schema_and_array),      \
                                          actual_schema));                    \
  }

GENERATE_READER_TEST(i8, I8, int8_t)
GENERATE_READER_TEST(i16, I16, int16_t)
GENERATE_READER_TEST(i32, I32, int32_t)
GENERATE_READER_TEST(i64, I64, int64_t)
GENERATE_READER_TEST(float, Fp32, float)
GENERATE_READER_TEST(double, Fp64, double)

// ┌────────┬─────────┬─────────┬─────────┬───────────┬────────────┐
// │ col_i8 │ col_i16 │ col_i32 │ col_i64 │ col_float │ col_double │
// │  int8  │  int16  │  int32  │  int64  │   float   │   double   │
// ├────────┼─────────┼─────────┼─────────┼───────────┼────────────┤
// │      0 │       0 │       0 │       0 │       0.0 │        0.0 │
// │        │         │         │         │           │            │
// │      2 │       2 │       2 │       2 │       2.0 │        2.0 │
// │        │         │         │         │           │            │
// │      4 │       4 │       4 │       4 │       4.0 │        4.0 │
// └────────┴─────────┴─────────┴─────────┴───────────┴────────────┘
TEST(CiderParquetReaderTest, mixed_cols) {
  Reader* reader = new Reader();
  reader->init(getParquetFilesPath() + "mixed_cols.parquet",
               {"col_i8", "col_i16", "col_i32", "col_i64", "col_float", "col_double"},
               0,
               1);
  ArrowSchema* actual_schema;
  ArrowArray* actual_array;
  reader->readBatch(5, actual_schema, actual_array);
  auto schema_and_array = ArrowArrayBuilder()
                              .setRowNum(5)
                              .addColumn<int8_t>("",
                                                 CREATE_SUBSTRAIT_TYPE(I8),
                                                 {0, 1, 2, 3, 4},
                                                 {false, true, false, true, false})
                              .addColumn<int16_t>("",
                                                  CREATE_SUBSTRAIT_TYPE(I16),
                                                  {0, 1, 2, 3, 4},
                                                  {false, true, false, true, false})
                              .addColumn<int32_t>("",
                                                  CREATE_SUBSTRAIT_TYPE(I32),
                                                  {0, 1, 2, 3, 4},
                                                  {false, true, false, true, false})
                              .addColumn<int64_t>("",
                                                  CREATE_SUBSTRAIT_TYPE(I64),
                                                  {0, 1, 2, 3, 4},
                                                  {false, true, false, true, false})
                              .addColumn<float>("",
                                                CREATE_SUBSTRAIT_TYPE(Fp32),
                                                {0, 1, 2, 3, 4},
                                                {false, true, false, true, false})
                              .addColumn<double>("",
                                                 CREATE_SUBSTRAIT_TYPE(Fp64),
                                                 {0, 1, 2, 3, 4},
                                                 {false, true, false, true, false})
                              .build();
  CHECK(CiderArrowChecker::checkArrowEq(std::get<1>(schema_and_array),
                                        actual_array,
                                        std::get<0>(schema_and_array),
                                        actual_schema));
}

// ┌──────────┐
// │ col_char │
// │ varchar  │
// ├──────────┤
// │ hello    │
// │ world    │
// │          │
// │ bdtk     │
// │ cider    │
// └──────────┘
TEST(CiderParquetReaderTest, char_single_col) {
  Reader* reader = new Reader();
  reader->init(getParquetFilesPath() + "char_single_col.parquet", {"col_char"}, 0, 1);
  ArrowSchema* actual_schema;
  ArrowArray* actual_array;
  reader->readBatch(5, actual_schema, actual_array);
  auto schema_and_array = ArrowArrayBuilder()
                              .setRowNum(5)
                              .addUTF8Column("",
                                             "helloworldbdtkcider",
                                             {0, 5, 10, 10, 14, 19},
                                             {false, false, true, false, false})
                              .build();
  CHECK(CiderArrowChecker::checkArrowEq(std::get<1>(schema_and_array),
                                        actual_array,
                                        std::get<0>(schema_and_array),
                                        actual_schema));
}
TEST(CiderParquetReaderTest, varchar_single_col) {
  Reader* reader = new Reader();
  reader->init(
      getParquetFilesPath() + "varchar_single_col.parquet", {"col_varchar"}, 0, 1);
  ArrowSchema* actual_schema;
  ArrowArray* actual_array;
  reader->readBatch(5, actual_schema, actual_array);
  auto schema_and_array = ArrowArrayBuilder()
                              .setRowNum(5)
                              .addUTF8Column("",
                                             "helloworldbdtkcider",
                                             {0, 5, 10, 10, 14, 19},
                                             {false, false, true, false, false})
                              .build();
  CHECK(CiderArrowChecker::checkArrowEq(std::get<1>(schema_and_array),
                                        actual_array,
                                        std::get<0>(schema_and_array),
                                        actual_schema));
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
