/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) 2016-2022 ClickHouse, Inc.
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

#pragma once

#include <common/Stopwatch.h>

#include <algorithm>

namespace cider::hashtable {

/**
 * The purpose of this helper class is to provide a good value for prefetch look ahead
 * (how distant row we should prefetch on the given iteration) based on the latency of a
 * single iteration of the given cycle.
 *
 * Assumed usage pattern is the following:
 *
 * PrefetchingHelper prefetching; /// When object is created, it starts a watch to measure
 * iteration latency. size_t prefetch_look_ahead = prefetching.getInitialLookAheadValue();
 * /// Initially it provides you with some reasonable default value.
 *
 * for (size_t i = 0; i < end; ++i)
 * {
 *     if (i == prefetching.iterationsToMeasure()) /// When enough iterations passed, we
 * are able to make a fairly accurate estimation of a single iteration latency.
 *         prefetch_look_ahead = prefetching.calcPrefetchLookAhead(); /// Based on this
 * estimation we can choose a good value for prefetch_look_ahead.
 *
 *     ... main loop body ...
 * }
 *
 */
class PrefetchingHelper {
 public:
  size_t calcPrefetchLookAhead() {
    static constexpr auto assumed_load_latency_ns = 100;
    static constexpr auto just_coefficient = 4;
    const auto single_iteration_latency = std::max<double>(
        static_cast<double>(1.0L * watch.elapsedNanoseconds() / iterations_to_measure),
        1.0);
    return std::clamp<size_t>(
        static_cast<size_t>(
            ceil(just_coefficient * assumed_load_latency_ns / single_iteration_latency)),
        min_look_ahead_value,
        max_look_ahead_value);
  }

  static constexpr size_t getInitialLookAheadValue() { return min_look_ahead_value; }

  static constexpr size_t iterationsToMeasure() { return iterations_to_measure; }

 private:
  static constexpr size_t iterations_to_measure = 100;
  static constexpr size_t min_look_ahead_value = 4;
  static constexpr size_t max_look_ahead_value = 32;

  Stopwatch watch;
};

}  // namespace cider::hashtable
