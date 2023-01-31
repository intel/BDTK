/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
/*
 * @file    Logger.h
 * @description Use Boost.Log for logging data compatible with previous API.
 *
 * Usage:
 * - Initialize a LogOptions object. E.g.
 *   logger::LogOptions log_options(argv[0]);
 * - LogOptions can optionally be added to boost::program_options:
 *   help_desc.add(log_options.get_options());
 * - Initialize global logger once per application:
 *   logger::init(log_options);
 * - From anywhere in the program:
 *    - LOG(INFO) << "Nice query!";
 *    - LOG(DEBUG4) << "x = " << x;
 *    - CHECK(condition);
 *    - CHECK_LE(x, xmax);
 *   Newlines are automatically appended to log messages.
 */

#ifndef SHARED_LOGGER_H
#define SHARED_LOGGER_H

#include <glog/logging.h>

#include <memory>
#include <set>

#include <array>
#include <sstream>
#include <thread>

extern bool g_enable_debug_timer;

namespace logger {

// Severity, SeverityNames, and SeveritySymbols must be updated together.
enum Severity {
  DEBUG4 = 0,
  DEBUG3,
  DEBUG2,
  DEBUG1,
  INFO,
  WARNING,
  ERROR,
  FATAL,
  _NSEVERITIES  // number of severity levels
};

#define UNREACHABLE() LOG(ERROR) << "UNREACHABLE "
#define UNIMPLEMENTED() LOG(ERROR) << "UNIMPLEMENTED! "

class Duration;

class DebugTimer {
  Duration* duration_;

 public:
  DebugTimer(Severity, char const* file, int line, char const* name);
  ~DebugTimer();
  void stop();
  // json is returned only when called on the root DurationTree.
  std::string stopAndGetJson();
};

using QueryId = uint64_t;
QueryId query_id();

// ~QidScopeGuard resets the thread_local g_query_id to 0 if the current value = id_.
// In other words, only the QidScopeGuard instance which resulted from changing
// g_query_id from zero to non-zero is responsible for resetting it back to zero when it
// goes out of scope. All other instances have no effect.
class QidScopeGuard {
  QueryId id_;

 public:
  QidScopeGuard(QueryId const id) : id_{id} {}
  QidScopeGuard(QidScopeGuard const&) = delete;
  QidScopeGuard& operator=(QidScopeGuard const&) = delete;
  QidScopeGuard(QidScopeGuard&& that) : id_(that.id_) { that.id_ = 0; }
  QidScopeGuard& operator=(QidScopeGuard&& that) {
    id_ = that.id_;
    that.id_ = 0;
    return *this;
  }
  ~QidScopeGuard();
  QueryId id() const { return id_; }
};

// Set logger::g_query_id based on given parameter.
QidScopeGuard set_thread_local_query_id(QueryId const);

using ThreadId = uint64_t;

void debug_timer_new_thread(ThreadId parent_thread_id);

ThreadId thread_id();

// Typical usage: auto timer = DEBUG_TIMER(__func__);
#define DEBUG_TIMER(name) logger::DebugTimer(logger::INFO, __FILE__, __LINE__, name)

// This MUST NOT be called more than once per thread, otherwise a failed CHECK() occurs.
// Best practice is to call it from the point where the new thread is spawned.
// Beware of threads that are re-used.
#define DEBUG_TIMER_NEW_THREAD(parent_thread_id)        \
  do {                                                  \
    if (g_enable_debug_timer)                           \
      logger::debug_timer_new_thread(parent_thread_id); \
  } while (false)

}  // namespace logger

#endif  // SHARED_LOGGER_H
