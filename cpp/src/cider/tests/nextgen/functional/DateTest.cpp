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
#include "exec/nextgen/Nextgen.h"
#include "exec/plan/parser/TypeUtils.h"
#include "tests/utils/CiderNextgenTestBase.h"

using namespace cider::exec::nextgen;
using namespace cider::test::util;

class DateTypeQueryTest : public CiderNextgenTestBase {
 public:
  DateTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT NOT NULL, col_b DATE NOT NULL);";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        366,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(Date)});
  }
};

class DateRandomQueryTest : public CiderNextgenTestBase {
 public:
  DateRandomQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE NOT NULL, col_b DATE NOT NULL);";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {},
        GeneratePattern::Random);
  }
};

class DateRandomAndNullQueryTest : public CiderNextgenTestBase {
 public:
  DateRandomAndNullQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {2, 3},
        GeneratePattern::Random,
        -36500,
        36500);
  }
};

// using year 2009 for week extracting test to avoid the conficts of
// ISO week date calendar (eg. YYYY-Www-dd)
// and Gregorian calendar (eg. YYYY-MM-dd)
class DateRandomAndNullQueryOf2009Test : public CiderNextgenTestBase {
 public:
  DateRandomAndNullQueryOf2009Test() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {2, 3},
        GeneratePattern::Random,
        14245,
        14609);
  }
};

TEST_F(DateTypeQueryTest, SimpleDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > date '1970-01-01' ");
  assertQuery("SELECT col_a FROM test where col_b >= date '1970-01-01' ");

  assertQuery("SELECT col_b FROM test where col_b < date '1970-02-01' ");
  assertQuery("SELECT col_b FROM test where col_b <= date '1970-02-01' ");

  assertQuery("SELECT col_a, col_b FROM test where col_b <> date '1970-01-01' ");

  assertQuery(
      "SELECT col_a FROM test where col_b >= date '1970-01-01' and col_b < date "
      "'1970-02-01' ");
  GTEST_SKIP() << "Test skipped since agg not ready";
  assertQuery("SELECT SUM(col_a) FROM test where col_b <= date '1980-01-01' ");
}

TEST_F(DateRandomAndNullQueryTest, FunctionTest) {
  assertQuery("SELECT extract(year from col_b) FROM test", "functions/date/year.json");
  assertQuery("SELECT extract(quarter from col_b) FROM test",
              "functions/date/quarter.json");
  assertQuery("SELECT extract(month from col_b) FROM test", "functions/date/month.json");
  assertQuery("SELECT extract(day from col_b) FROM test", "functions/date/day.json");
  assertQuery("SELECT extract(dayofweek from col_b) FROM test",
              "functions/date/day_of_week.json");
  assertQuery("SELECT extract(doy from col_b) FROM test",
              "functions/date/day_of_year.json");
}

TEST_F(DateTypeQueryTest, SimpleExtractDateTest) {
  assertQuery("SELECT extract(year from col_b) FROM test");
  assertQuery("SELECT extract(quarter from col_b) FROM test",
              "extract/quarter_not_null.json");
  assertQuery("SELECT extract(month from col_b) FROM test");
  assertQuery("SELECT extract(day from col_b) FROM test");
  assertQuery("SELECT extract(dayofweek from col_b) FROM test",
              "extract/day_of_week_not_null.json");
  assertQuery("SELECT extract(isodow from col_b) FROM test",
              "extract/iso_day_of_week_not_null.json");
  assertQuery("SELECT extract(doy from col_b) FROM test",
              "extract/day_of_year_not_null.json");
}

// extract function of week in cider is based on Gregorian calendar (eg. YYYY-MM-dd).
// while extract function of week in duckdb and presto are based on
// ISO week date calendar (eg. YYYY-Www-dd) : @link:
// https://en.wikipedia.org/wiki/ISO_week_date
// using 2009 for test because ISO week calendar covers Gregorian calendar in 2009
TEST_F(DateRandomAndNullQueryOf2009Test, SimpleExtractDateTest2) {
  assertQuery("SELECT extract(week from col_b) FROM test", "extract/week.json");
  assertQuery("SELECT extract(week from col_b) FROM test", "functions/date/week.json");
}

TEST_F(DateRandomQueryTest, SimpleExtractDateTest2) {
  assertQuery("SELECT extract(year from col_b) FROM test");
  assertQuery("SELECT extract(quarter from col_b) FROM test",
              "extract/quarter_not_null.json");
  assertQuery("SELECT extract(month from col_b) FROM test");
  assertQuery("SELECT extract(day from col_b) FROM test");
  assertQuery("SELECT extract(dayofweek from col_b) FROM test",
              "extract/day_of_week_not_null.json");
  assertQuery("SELECT extract(isodow from col_b) FROM test",
              "extract/iso_day_of_week_not_null.json");
  assertQuery("SELECT extract(doy from col_b) FROM test",
              "extract/day_of_year_not_null.json");
}

TEST_F(DateRandomAndNullQueryTest, SimpleExtractDateTest3) {
  assertQuery("SELECT extract(year from col_b) FROM test");
  assertQuery("SELECT extract(quarter from col_b) FROM test", "extract/quarter.json");
  assertQuery("SELECT extract(month from col_b) FROM test");
  assertQuery("SELECT extract(day from col_b) FROM test");
  assertQuery("SELECT extract(dayofweek from col_b) FROM test",
              "extract/day_of_week.json");
  assertQuery("SELECT extract(isodow from col_b) FROM test",
              "extract/iso_day_of_week.json");
  assertQuery("SELECT extract(doy from col_b) FROM test", "extract/day_of_year.json");
}

TEST_F(DateRandomAndNullQueryTest, ExtractDateWithAggTest) {
  GTEST_SKIP() << "Test skipped since agg not ready";
  assertQuery("SELECT MIN(extract(year from col_b)) FROM test");
  assertQuery("SELECT MAX(extract(year from col_b)) FROM test");
  assertQuery("SELECT MIN(extract(day from col_b)) FROM test");
  assertQuery("SELECT MAX(extract(day from col_b)) FROM test");
}

TEST_F(DateRandomQueryTest, SimpleRandomDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > date '1999-12-01' ");
  assertQuery("SELECT col_b FROM test where col_a < date '1999-12-01' ");
  assertQuery("SELECT col_b FROM test where col_b < date '2077-07-07' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b > date '2066-06-06' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b <> date '1971-02-02' ");
  assertQuery(
      "SELECT col_a FROM test where col_b >= date '1900-01-01' and col_b < date "
      "'2077-07-07' ");
}

TEST_F(DateRandomAndNullQueryTest, NullDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > date '1990-11-03' ");
  assertQuery("SELECT col_b FROM test where col_a < date '1990-11-03' ");
  assertQuery("SELECT col_b FROM test where col_b < date '2027-07-07' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b < date '1980-01-01' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b <> date '1970-02-02' ");
  assertQuery(
      "SELECT col_a FROM test where col_b >= date '1900-01-01' and col_b < date "
      "'2077-02-01' ");
}

TEST_F(DateTypeQueryTest, DateAddYearMonthTest) {
  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' year ");

  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '2' year ");

  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' month ");

  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '10' month ");

  assertQuery(
      "SELECT col_a FROM test where col_b > date '1971-01-01' - interval '1' year ");

  assertQuery(
      "SELECT col_a FROM test where col_b > date '1971-01-01' - interval '12' month ");

  assertQuery(
      "SELECT col_a FROM test where col_b >= date '1970-01-01' + interval '1' month  and "
      "col_b < date '1970-01-01' + interval '2' month");
}

TEST_F(DateTypeQueryTest, DateAddDayTest) {
  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '1' day ");

  assertQuery(
      "SELECT col_a FROM test where col_b < date '1970-01-01' + interval '80' day ");

  assertQuery(
      "SELECT col_a FROM test where col_b > date '1970-02-01' - interval '10' day ");
}

TEST_F(DateRandomAndNullQueryTest, DateAddOnColumnTest) {
  assertQuery(
      "SELECT col_a - interval '10' day FROM test where col_a > date '1970-02-01'");

  assertQuery(
      "SELECT col_a FROM test where col_b  + interval '1' month > date '1970-03-01'");

  assertQuery(
      "SELECT col_b + interval '1' year  FROM test where col_b + interval '1' year > "
      "date '1971-02-01' ");
}

TEST_F(DateRandomAndNullQueryTest, DateOpTest) {
  assertQuery("SELECT * FROM test where extract(day from col_b) > 15");

  assertQuery(
      "SELECT extract(day from col_b + interval '10' day) FROM test where extract(day "
      "from col_b + interval '10' day) > 25");

  assertQuery("SELECT col_a + interval '10' day + interval '1' month FROM test");

  assertQuery(
      "SELECT extract(day from col_a), extract(month from col_b) , extract(year from "
      "col_b) FROM test");

  assertQuery(
      "SELECT col_a + interval '10' day, extract(day from col_b) FROM test where col_a > "
      "date '1970-02-01' and extract(day from col_b) > 20");

  GTEST_SKIP() << "Test skipped since agg not ready";
  assertQuery(
      "SELECT MIN(extract(year from col_b)) FROM test where extract(year from col_b) > "
      "1972");

  assertQuery(
      "SELECT MIN(extract(day from col_b)), MAX(extract(day from col_b)), "
      "SUM(extract(day from col_b)) FROM test where col_b > date '1970-01-01' and col_b "
      "< date '1980-01-01'");
}

class TimeTypeQueryTest : public CiderNextgenTestBase {
 public:
  TimeTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ =
        "CREATE TABLE test(col_date DATE, col_time TIME, col_timestamp TIMESTAMP);";
    QueryArrowDataGenerator::generateBatchByTypes(
        input_schema_,
        input_array_,
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
  assertQuery("SELECT col_timestamp + INTERVAL '1' MONTH FROM test",
              "add_timestamp_interval_month.json");
  assertQuery("SELECT col_timestamp + INTERVAL '1' DAY FROM test",
              "add_timestamp_interval_day.json");
  assertQuery("SELECT col_timestamp + INTERVAL '1' SECOND FROM test",
              "add_timestamp_interval_second.json");
  // multiple columns with carry-out
  assertQuery(
      "SELECT col_timestamp + INTERVAL '20' MONTH, col_timestamp + INTERVAL '50' DAY, "
      "col_timestamp + INTERVAL '5000' SECOND FROM test",
      "add_timestamp_interval_mixed.json");
  assertQuery("SELECT CAST(col_date AS TIMESTAMP) FROM test",
              "cast_date_as_timestamp.json");
  assertQuery("SELECT CAST(CAST(col_timestamp AS VARCHAR) as TIMESTAMP) FROM test",
              "cast_string_to_timestamp.json");
  assertQuery("SELECT CAST(CAST(col_time AS VARCHAR) as TIME) FROM test",
              "cast_string_to_time.json");
  assertQuery("SELECT EXTRACT(microsecond FROM col_timestamp) FROM test",
              "extract/microsecond_of_timestamp.json");
  assertQuery("SELECT EXTRACT(second FROM col_time) FROM test",
              "extract/second_of_time.json");
  assertQuery("SELECT EXTRACT(quarter FROM col_timestamp) FROM test",
              "extract/quarter_of_timestamp.json");
  assertQuery("SELECT EXTRACT(month FROM col_timestamp) FROM test",
              "extract/month_of_timestamp.json");
  assertQuery("SELECT EXTRACT(year FROM col_timestamp) FROM test",
              "extract/year_of_timestamp.json");
  // equals to date trunc
  assertQuery("SELECT CAST(col_timestamp AS DATE) FROM test",
              "cast_timestamp_as_date.json");
  assertQuery("SELECT col_timestamp FROM test WHERE col_timestamp > DATE '1970-01-01'",
              "cast_literal_timestamp.json");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // gflags::ParseCommandLineFlags(&argc, &argv, true);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
