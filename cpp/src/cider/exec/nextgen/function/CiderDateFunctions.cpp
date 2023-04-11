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

#include "function/datetime/DateAdd.h"

namespace {

struct OffsetUnit {
  unsigned dom;  // day-of-month (0-based)
  unsigned sod;  // second-of-day
  unsigned doe;  // day-of-era
  unsigned yoe;  // year-of-era
  unsigned doy;  // day-of-year
  unsigned moy;  // month-of-year
  unsigned moe;  // month-of-era
};

constexpr int64_t pow10[10]{1, 0, 0, 1000, 0, 0, 1000 * 1000, 0, 0, 1000 * 1000 * 1000};
constexpr int64_t quarter[12]{1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 1, 1};
// Number of days until Wednesday (because 2000-03-01 is a Wednesday.)
constexpr unsigned MONDAY = 2;
constexpr unsigned SUNDAY = 3;
constexpr unsigned SATURDAY = 4;

// Clamp day-of-month to max day of the month. E.g. April 31 -> 30.
static unsigned clampDom(unsigned yoe, unsigned moy, unsigned dom) {
  constexpr unsigned max_days[11]{30, 29, 30, 29, 30, 30, 29, 30, 29, 30, 30};
  if (dom < 28) {
    return dom;
  } else {
    unsigned const max_day = moy == 11
                                 ? 27 + (++yoe % 4 == 0 && (yoe % 100 != 0 || yoe == 400))
                                 : max_days[moy];
    return dom < max_day ? dom : max_day;
  }
}

// If OFFSET=MONDAY,
// then return day-of-era of the Monday of ISO 8601 week 1 in the given year-of-era.
// Similarly for SUNDAY and SATURDAY. In all cases, week 1 always contains Jan 4.
template <unsigned OFFSET>
unsigned week_start_from_yoe(unsigned const yoe) {
  unsigned const march1 = yoe * 365 + yoe / 4 - yoe / 100;
  unsigned const jan4 = march1 + (MARJAN + 3);
  unsigned const jan4dow = (jan4 + OFFSET) % 7;
  return jan4 - jan4dow;
}

class MonthDaySecond {
  int64_t months_;  // Number of months since 2000 March 1.
  int64_t era_;
  OffsetUnit offset_unit_;

 public:
  explicit MonthDaySecond(int32_t const date, unsigned sod = 0) {
    offset_unit_.sod = sod;
    era_ = floor_div(date - kEpochAdjustedDays, kDaysPer400Years);
    offset_unit_.doe = date - kEpochAdjustedDays - era_ * kDaysPer400Years;
    offset_unit_.yoe = (offset_unit_.doe - offset_unit_.doe / 1460 +
                        offset_unit_.doe / 36524 - offset_unit_.doe / 146096) /
                       365;
    offset_unit_.doy = offset_unit_.doe - (365 * offset_unit_.yoe + offset_unit_.yoe / 4 -
                                           offset_unit_.yoe / 100);
    offset_unit_.moy = (5 * offset_unit_.doy + 2) / 153;
    offset_unit_.dom = offset_unit_.doy - (153 * offset_unit_.moy + 2) / 5;
    months_ = (era_ * 400 + offset_unit_.yoe) * 12 + offset_unit_.moy;
  }

  MonthDaySecond& addMonths(int64_t const months) {
    months_ += months;
    updateOffset();
    return *this;
  }

  void updateOffset() {
    era_ = floor_div(months_, 12 * 400);
    offset_unit_.moe = months_ - era_ * (12 * 400);
    offset_unit_.yoe = offset_unit_.moe / 12;
    offset_unit_.moy = offset_unit_.moe % 12;
    offset_unit_.doy = (153 * offset_unit_.moy + 2) / 5 +
                       clampDom(offset_unit_.yoe, offset_unit_.moy, offset_unit_.dom);
    offset_unit_.doe = offset_unit_.yoe * 365 + offset_unit_.yoe / 4 -
                       offset_unit_.yoe / 100 + offset_unit_.doy;
  }

  // Return number of seconds since 1 January 1970.
  int64_t getTime() const { return getDate() * kSecsPerDay + offset_unit_.sod; }

  // Return dates since 1 January 1970.
  int32_t getDate() const {
    return (kEpochAdjustedDays + era_ * kDaysPer400Years + offset_unit_.doe);
  }

  int64_t extractYear() const {
    return 2000 + era_ * 400 + offset_unit_.yoe + (MARJAN <= offset_unit_.doy);
  }

  int64_t extractDay() const {
    return offset_unit_.doy - (153 * offset_unit_.moy + 2) / 5 + 1;
  }

  int64_t extractMonth() const {
    return offset_unit_.moy + (offset_unit_.moy < 10 ? 3 : -9);
  }

  int64_t extractQuarter() const { return quarter[offset_unit_.moy]; }

  int64_t extractDayOfYear() const {
    return offset_unit_.doy +
           (offset_unit_.doy < MARJAN
                ? 1 + JANMAR +
                      (offset_unit_.yoe % 4 == 0 &&
                       (offset_unit_.yoe % 100 != 0 || offset_unit_.yoe == 0))
                : 1 - MARJAN);
  }

  template <unsigned OFFSET>
  int64_t extract_week() const {
    unsigned week_start = week_start_from_yoe<OFFSET>(offset_unit_.yoe);
    if (offset_unit_.doe < week_start) {
      if (offset_unit_.yoe == 0) {
        // 2000-03-01 is OFFSET days from start of week, week + 9.
        return (offset_unit_.doe + OFFSET) / 7 + 9;
      } else {
        week_start = week_start_from_yoe<OFFSET>(offset_unit_.yoe - 1);
      }
    }
    return (offset_unit_.doe - week_start) / 7 + 1;
  }
};

}  // namespace

// interval type: kINTERVAL_DAY_TIME(add day/minute/second, second unit)
// interval type: kINTERVAL_YEAR_MONTH(add month/year, month unit)
extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t date_add_seconds(const int32_t date,
                                                              const int64_t interval) {
  // interval must be divisible by kSecsPerDay
  return date + interval / kSecsPerDay;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_add_seconds(const int64_t time,
                                                              const int64_t interval) {
  return time + interval;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int32_t date_add_months(const int32_t date,
                                                             const int64_t interval) {
  return MonthDaySecond(date).addMonths(interval).getDate();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_add_months(const int64_t time,
                                                             const int64_t interval) {
  int64_t const date = floor_div(time, kSecsPerDay);
  return MonthDaySecond(date, time - date * kSecsPerDay).addMonths(interval).getTime();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_add_months_high_precision(const int64_t time,
                               const int64_t interval,
                               const int32_t dim) {
  int64_t const scale = pow10[dim];
  return time_add_months(floor_div(time, scale), interval) * scale +
         unsigned_mod(time, scale);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_add_seconds_high_precision(const int64_t time,
                                const int64_t interval,
                                const int32_t dim) {
  int64_t const scale = pow10[dim];
  return time_add_seconds(floor_div(time, scale), interval) * scale +
         unsigned_mod(time, scale);
}

// date extract  (days~year)
extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_year(const int32_t date) {
  return MonthDaySecond(date).extractYear();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_day(const int32_t date) {
  return MonthDaySecond(date).extractDay();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_dow(const int32_t date) {
  return unsigned_mod(date + 4, kDaysPerWeek);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_isodow(const int32_t date) {
  return unsigned_mod(date + 3, kDaysPerWeek) + 1;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_month(const int32_t date) {
  return MonthDaySecond(date).extractMonth();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t date_extract_quarter(const int32_t date) {
  return MonthDaySecond(date).extractQuarter();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
date_extract_day_of_year(const int32_t date) {
  return MonthDaySecond(date).extractDayOfYear();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
date_extract_week_monday(const int32_t date) {
  return MonthDaySecond(date).extract_week<MONDAY>();
}

// timestamp extract (nanosecond~year)
extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_hour(const int64_t time) {
  return unsigned_mod(time, kSecsPerDay) / kSecsPerHour;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_minute(const int64_t time) {
  return unsigned_mod(time, kSecsPerHour) / kSecsPerMin;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_second(const int64_t time) {
  return unsigned_mod(time, kSecsPerMin);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_extract_millisecond(const int64_t time) {
  return unsigned_mod(time, kSecsPerMin * kMilliSecsPerSec);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_extract_microsecond(const int64_t time) {
  return unsigned_mod(time, kSecsPerMin * kMicroSecsPerSec);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_nanosecond(const int64_t time) {
  return unsigned_mod(time, kSecsPerMin * kNanoSecsPerSec);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_dow(const int64_t time) {
  int64_t const days_past_epoch = floor_div(time, kSecsPerDay);
  return unsigned_mod(days_past_epoch + 4, kDaysPerWeek);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_isodow(const int64_t time) {
  int64_t const days_past_epoch = floor_div(time, kSecsPerDay);
  return unsigned_mod(days_past_epoch + 3, kDaysPerWeek) + 1;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_extract_day_of_year(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extractDayOfYear();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_day(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extractDay();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
time_extract_week_monday(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extract_week<MONDAY>();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_month(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extractMonth();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_quarter(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extractQuarter();
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t time_extract_year(const int64_t time) {
  return MonthDaySecond(floor_div(time, kSecsPerDay)).extractYear();
}
