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

#include "function/datetime/DateAdd.h"

namespace {
class MonthDaySecond {
  int64_t months;  // Number of months since 2000 March 1.
  unsigned dom;    // day-of-month (0-based)
  unsigned sod;    // second-of-day

  // Clamp day-of-month to max day of the month. E.g. April 31 -> 30.
  static unsigned clampDom(unsigned yoe, unsigned moy, unsigned dom) {
    constexpr unsigned max_days[11]{30, 29, 30, 29, 30, 30, 29, 30, 29, 30, 30};
    if (dom < 28) {
      return dom;
    } else {
      unsigned const max_day =
          moy == 11 ? 27 + (++yoe % 4 == 0 && (yoe % 100 != 0 || yoe == 400))
                    : max_days[moy];
      return dom < max_day ? dom : max_day;
    }
  }

 public:
  MonthDaySecond(int64_t const timeval) {
    int64_t const day = floor_div(timeval, kSecsPerDay);
    int64_t const era = floor_div(day - kEpochAdjustedDays, kDaysPer400Years);
    sod = timeval - day * kSecsPerDay;
    unsigned const doe = day - kEpochAdjustedDays - era * kDaysPer400Years;
    unsigned const yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned const moy = (5 * doy + 2) / 153;
    dom = doy - (153 * moy + 2) / 5;
    months = (era * 400 + yoe) * 12 + moy;
  }

  MonthDaySecond const& addMonths(int64_t const months) {
    this->months += months;
    return *this;
  }

  // Return number of seconds since 1 January 1970.
  int64_t unixtime() const { return getDays() * kSecsPerDay + sod; }

  // Return days since 1 January 1970.
  int32_t getDays() const {
    int64_t const era = floor_div(months, 12 * 400);
    unsigned const moe = months - era * (12 * 400);
    unsigned const yoe = moe / 12;
    unsigned const moy = moe % 12;
    unsigned const doy = (153 * moy + 2) / 5 + clampDom(yoe, moy, dom);
    unsigned const doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (kEpochAdjustedDays + era * kDaysPer400Years + doe);
  }
};

}  // namespace

constexpr int64_t pow10[10]{1, 0, 0, 1000, 0, 0, 1000 * 1000, 0, 0, 1000 * 1000 * 1000};

extern "C" ALWAYS_INLINE int32_t DateAddSeconds(const int32_t time,
                                                const int64_t interval) {
  return time + interval / kSecsPerDay;
}

extern "C" ALWAYS_INLINE int64_t TimeAddSeconds(const int64_t time,
                                                const int64_t interval) {
  return time + interval;
}

extern "C" ALWAYS_INLINE int32_t DateAddMonths(const int32_t time,
                                               const int64_t interval) {
  return MonthDaySecond(time).addMonths(interval).getDays();
}

extern "C" ALWAYS_INLINE int32_t TimeAddMonths(const int64_t time,
                                               const int64_t interval) {
  return MonthDaySecond(time).addMonths(interval).unixtime();
}

extern "C" ALWAYS_INLINE int64_t TimeAddMonthsHighPrecision(const int64_t time,
                                                            const int64_t interval,
                                                            const int32_t dim) {
  const int64_t scale = pow10[dim];
  return TimeAddMonths(floor_div(time, scale), interval) * scale +
         unsigned_mod(time, scale);
}

extern "C" ALWAYS_INLINE int64_t TimeAddSecondsHighPrecision(const int64_t time,
                                                             const int64_t interval,
                                                             const int32_t dim) {
  const int64_t scale = pow10[dim];
  return TimeAddSeconds(floor_div(time, scale), interval) * scale +
         unsigned_mod(time, scale);
}
