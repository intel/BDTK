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
#include "tests/utils/CiderTestBase.h"

class DateTypeQueryTest : public CiderTestBase {
 public:
  DateTypeQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a BIGINT, col_b DATE);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        366,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(I64), CREATE_SUBSTRAIT_TYPE(Date)}))};
  }
};

class DateRandomQueryTest : public CiderTestBase {
 public:
  DateRandomQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {},
        GeneratePattern::Random))};
  }
};

class DateRandomAndNullQueryTest : public CiderTestBase {
 public:
  DateRandomAndNullQueryTest() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {2, 3},
        GeneratePattern::Random,
        -36500,
        36500))};
  }
};

// using year 2009 for week extracting test to avoid the conficts of
// ISO week date calendar (eg. YYYY-Www-dd)
// and Gregorian calendar (eg. YYYY-MM-dd)
class DateRandomAndNullQueryOf2009Test : public CiderTestBase {
 public:
  DateRandomAndNullQueryOf2009Test() {
    table_name_ = "test";
    create_ddl_ = "CREATE TABLE test(col_a DATE, col_b DATE);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        500,
        {"col_a", "col_b"},
        {CREATE_SUBSTRAIT_TYPE(Date), CREATE_SUBSTRAIT_TYPE(Date)},
        {2, 3},
        GeneratePattern::Random,
        14245,
        14609))};
  }
};

TEST_F(DateTypeQueryTest, yearTest) {
  assertQuery("SELECT extract(year from col_b) FROM test", "functions/date/year.json");
}

TEST_F(DateTypeQueryTest, SimpleExtractDateTest) {
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

// extract function of week in cider is based on Gregorian calendar (eg. YYYY-MM-dd).
// while extract function of week in duckdb and presto are based on
// ISO week date calendar (eg. YYYY-Www-dd) : @link:
// https://en.wikipedia.org/wiki/ISO_week_date
// using 2009 for test because ISO week calendar covers Gregorian calendar in 2009
TEST_F(DateRandomAndNullQueryOf2009Test, SimpleExtractDateTest2) {
  assertQuery("SELECT extract(week from col_b) FROM test", "extract/week.json");
}

TEST_F(DateRandomQueryTest, SimpleExtractDateTest2) {
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
  assertQuery("SELECT MIN(extract(year from col_b)) FROM test");
  assertQuery("SELECT MAX(extract(year from col_b)) FROM test");
  assertQuery("SELECT MIN(extract(day from col_b)) FROM test");
  assertQuery("SELECT MAX(extract(day from col_b)) FROM test");
}

TEST_F(DateTypeQueryTest, SimpleDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > '1970-01-01' ");
  assertQuery("SELECT col_a FROM test where col_b >= '1970-01-01' ");

  assertQuery("SELECT col_b FROM test where col_b < '1970-02-01' ");
  assertQuery("SELECT col_b FROM test where col_b <= '1970-02-01' ");

  assertQuery("SELECT SUM(col_a) FROM test where col_b <= '1980-01-01' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b <> '1970-01-01' ");

  assertQuery(
      "SELECT col_a FROM test where col_b >= '1970-01-01' and col_b < '1970-02-01' ");
}

TEST_F(DateRandomQueryTest, SimpleRandomDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > '1999-12-01' ");
  assertQuery("SELECT col_b FROM test where col_a < '1999-12-01' ");
  assertQuery("SELECT col_b FROM test where col_b < '2077-07-07' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b > '2066-06-06' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b <> '1971-02-02' ");
  assertQuery(
      "SELECT col_a FROM test where col_b >= '1900-01-01' and col_b < '2077-07-07' ");
}

TEST_F(DateRandomAndNullQueryTest, NullDateTest) {
  assertQuery("SELECT col_a FROM test where col_b > '1990-11-03' ");
  assertQuery("SELECT col_b FROM test where col_a < '1990-11-03' ");
  assertQuery("SELECT col_b FROM test where col_b < '2027-07-07' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b < '1980-01-01' ");
  assertQuery("SELECT col_a, col_b FROM test where col_b <> '1970-02-02' ");
  assertQuery(
      "SELECT col_a FROM test where col_b >= '1900-01-01' and col_b < '2077-02-01' ");
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
      "SELECT col_a FROM test where col_b  + interval '1' month > date '1970-02-01'");

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

  assertQuery(
      "SELECT MIN(extract(year from col_b)) FROM test where extract(year from col_b) > "
      "1972");

  assertQuery(
      "SELECT MIN(extract(day from col_b)), MAX(extract(day from col_b)), "
      "SUM(extract(day from col_b)) FROM test where col_b > date '1970-01-01' and col_b "
      "< date '1980-01-01'");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
  }
  return err;
}
