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

#include "BatchDataGenerator.h"

namespace cider {
namespace transformer {
namespace test {
namespace util {

using ::facebook::velox::Date;
using ::facebook::velox::IntervalDayTime;
using ::facebook::velox::StringView;
using ::facebook::velox::Timestamp;

template <>
bool BatchDataGenerator::gen_value<bool>(std::mt19937& gen) {
  return folly::Random::rand32(0, 2, gen) == 0;
}

template <>
int8_t BatchDataGenerator::gen_value<int8_t>(std::mt19937& gen) {
  return static_cast<int8_t>(folly::Random::rand32(gen));
}

template <>
int16_t BatchDataGenerator::gen_value<int16_t>(std::mt19937& gen) {
  return static_cast<int16_t>(folly::Random::rand32(gen));
}

template <>
int32_t BatchDataGenerator::gen_value<int32_t>(std::mt19937& gen) {
  return static_cast<int32_t>(folly::Random::rand32(gen));
}

template <>
int64_t BatchDataGenerator::gen_value<int64_t>(std::mt19937& gen) {
  return folly::Random::rand64(gen);
}

template <>
float BatchDataGenerator::gen_value<float>(std::mt19937& gen) {
  return static_cast<float>(folly::Random::randDouble01(gen));
}

template <>
double BatchDataGenerator::gen_value<double>(std::mt19937& gen) {
  return folly::Random::randDouble01(gen);
}

template <>
Date BatchDataGenerator::gen_value<Date>(std::mt19937& gen) {
  return Date(folly::Random::rand32(gen));
}

template <>
Timestamp BatchDataGenerator::gen_value<Timestamp>(std::mt19937& gen) {
  constexpr int64_t TIME_OFFSET = 1420099200;
  return Timestamp(TIME_OFFSET + folly::Random::rand32(0, 60 * 60 * 24 * 365, gen),
                   folly::Random::rand32(0, 1'000'000, gen));
}

template <>
IntervalDayTime BatchDataGenerator::gen_value<IntervalDayTime>(std::mt19937& gen) {
  return IntervalDayTime(folly::Random::rand32(gen));
}

template <>
StringView BatchDataGenerator::gen_value<StringView>(std::mt19937& gen) {
  char buff[12] = {'\0'};
  int32_t len = folly::Random::rand32(0, 10, gen) + 1;
  for (int32_t i = 0; i < len; ++i) {
    buff[i] = 'a' + folly::Random::rand32(0, 26, gen);
  }
  buff[len] = '\0';
  return StringView{buff, len};  // StringView max support 12 byte inline value
}


} // namespace util
} // namespace test
} // namespace transformer
} // namespace cider
