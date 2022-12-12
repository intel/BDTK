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

#include <gtest/gtest.h>
#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/CiderTestBase.h"

using namespace cider::exec::nextgen;

class DateTypeQueryTest : public CiderTestBase {
 public:
  DateTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT NOT NULL, col_b DATE NOT NULL);";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        366,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(Date)});
  }
};

class DateRandomQueryTest : public CiderTestBase {
 public:
  DateRandomQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE NOT NULL, col_b DATE NOT NULL);";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {},
        GeneratePattern::Random);
  }
};

class DateRandomAndNullQueryTest : public CiderTestBase {
 public:
  DateRandomAndNullQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {2, 3},
        GeneratePattern::Random,
        -36500,
        36500);
  }
};

TEST_F(DateTypeQueryTest, SimpleDateTest) {
  assertQueryArrow("SELECT col_a FROM test where col_b > date '1970-01-01' ");
  assertQueryArrow("SELECT col_a FROM test where col_b >= date '1970-01-01' ");

  assertQueryArrow("SELECT col_b FROM test where col_b < date '1970-02-01' ");
  assertQueryArrow("SELECT col_b FROM test where col_b <= date '1970-02-01' ");

  assertQueryArrow("SELECT col_a, col_b FROM test where col_b <> date '1970-01-01' ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b >= date '1970-01-01' and col_b < date "
      "'1970-02-01' ");
  GTEST_SKIP() << "Test skipped since agg not ready";
  assertQueryArrow("SELECT SUM(col_a) FROM test where col_b <= date '1980-01-01' ");
}

TEST_F(DateRandomQueryTest, SimpleRandomDateTest) {
  assertQueryArrow("SELECT col_a FROM test where col_b > date '1999-12-01' ");
  assertQueryArrow("SELECT col_b FROM test where col_a < date '1999-12-01' ");
  assertQueryArrow("SELECT col_b FROM test where col_b < date '2077-07-07' ");
  assertQueryArrow("SELECT col_a, col_b FROM test where col_b > date '2066-06-06' ");
  assertQueryArrow("SELECT col_a, col_b FROM test where col_b <> date '1971-02-02' ");
  assertQueryArrow(
      "SELECT col_a FROM test where col_b >= date '1900-01-01' and col_b < date "
      "'2077-07-07' ");
}

TEST_F(DateRandomAndNullQueryTest, NullDateTest) {
  assertQueryArrow("SELECT col_a FROM test where col_b > date '1990-11-03' ");
  assertQueryArrow("SELECT col_b FROM test where col_a < date '1990-11-03' ");
  assertQueryArrow("SELECT col_b FROM test where col_b < date '2027-07-07' ");
  assertQueryArrow("SELECT col_a, col_b FROM test where col_b < date '1980-01-01' ");
  assertQueryArrow("SELECT col_a, col_b FROM test where col_b <> date '1970-02-02' ");
  assertQueryArrow(
      "SELECT col_a FROM test where col_b >= date '1900-01-01' and col_b < date "
      "'2077-02-01' ");
}

TEST_F(DateTypeQueryTest, DateAddYearMonthTest) {
  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' year ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '2' year ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' month ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '10' month ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b > date '1971-01-01' - interval '1' year ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b > date '1971-01-01' - interval '12' month ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b >= date '1970-01-01' + interval '1' month  and "
      "col_b < date '1970-01-01' + interval '2' month");
}

TEST_F(DateTypeQueryTest, DateAddDayTest) {
  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' day ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '80' day ");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b > date '1970-02-01' - interval '10' day ");
}

TEST_F(DateRandomAndNullQueryTest, DateAddOnColumnTest) {
  assertQueryArrow(
      "SELECT col_a - interval '10' day FROM test where col_a > date '1970-02-01'");

  assertQueryArrow(
      "SELECT col_a FROM test where col_b  + interval '1' month > date '1970-03-01'");

  assertQueryArrow(
      "SELECT col_b + interval '1' year  FROM test where col_b + interval '1' year > "
      "date '1971-02-01' ");
}

class TimeTypeQueryTest : public CiderTestBase {
 public:
  TimeTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_date DATE, col_time TIME, col_timestamp TIMESTAMP);";
    QueryArrowDataGenerator::generateBatchByTypes(
        schema_,
        array_,
        100,
        {"col_date", "col_time", "col_timestamp"},
        {CREATE_SUBSTRAIT_TYPE(Date),
         CREATE_SUBSTRAIT_TYPE(Time),
         CREATE_SUBSTRAIT_TYPE(Timestamp)},
        {2, 2, 2},
        GeneratePattern::Random);
  }
};

TEST_F(TimeTypeQueryTest, MultiTimeTypeTest) {
  assertQueryArrow("SELECT col_timestamp + INTERVAL '1' MONTH FROM test",
                   "add_timestamp_interval_month.json");
  assertQueryArrow("SELECT col_timestamp + INTERVAL '1' DAY FROM test",
                   "add_timestamp_interval_day.json");
  assertQueryArrow("SELECT col_timestamp + INTERVAL '1' SECOND FROM test",
                   "add_timestamp_interval_second.json");

  // multiple columns with carry-out
  assertQueryArrow(
      "SELECT col_timestamp + INTERVAL '20' MONTH, col_timestamp + INTERVAL '50' DAY, "
      "col_timestamp + INTERVAL '5000' SECOND FROM test",
      "add_timestamp_interval_mixed.json");

  assertQueryArrow("SELECT CAST(col_date AS TIMESTAMP) FROM test",
                   "cast_date_as_timestamp.json");
  // equals to date trunc
  assertQueryArrow("SELECT CAST(col_timestamp AS DATE) FROM test",
                   "cast_timestamp_as_date.json");

  assertQueryArrow(
      "SELECT col_timestamp FROM test WHERE col_timestamp > DATE '1970-01-01'",
      "cast_literal_timestamp.json");

  GTEST_SKIP() << "Test skipped since extract not ready";
  // equals to date trunc
  assertQueryArrow("SELECT EXTRACT(microsecond FROM col_timestamp) FROM test",
                   "extract/microsecond_of_timestamp.json");
  assertQueryArrow("SELECT EXTRACT(second FROM col_time) FROM test",
                   "extract/second_of_time.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
