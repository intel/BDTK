/*
 * Copyright (c) 2022 Intel Corporation.
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
#pragma once

#include "util/boost_stacktrace.hpp"

class OutOfMemory : public std::runtime_error {
 public:
  OutOfMemory(size_t num_bytes)
      : std::runtime_error(parse_error_str("OutOfMemory", num_bytes)) {
    VLOG(1) << "Failed to allocate " << num_bytes << " bytes";
    VLOG(1) << boost::stacktrace::stacktrace();
  };

  OutOfMemory(const std::string& err) : std::runtime_error(parse_error_str(err, 0)) {
    VLOG(1) << "Failed with OutOfMemory, condition " << err;
    VLOG(1) << boost::stacktrace::stacktrace();
  };

  OutOfMemory(const std::string& err, size_t num_bytes)
      : std::runtime_error(parse_error_str(err, num_bytes)) {
    VLOG(1) << "Failed to allocate " << num_bytes << " bytes with condition " << err;
    VLOG(1) << boost::stacktrace::stacktrace();
  };

 private:
  std::string parse_error_str(const std::string& err, const size_t num_bytes = 0) {
    if (num_bytes) {
      return err + ": Failed to allocate " + std::to_string(num_bytes) + " bytes";
    } else {
      return "Failed to allocate memory with condition " + err;
    }
  }
};

class FailedToCreateFirstSlab : public OutOfMemory {
 public:
  FailedToCreateFirstSlab(size_t num_bytes)
      : OutOfMemory("FailedToCreateFirstSlab", num_bytes) {}
};

class FailedToCreateSlab : public OutOfMemory {
 public:
  FailedToCreateSlab(size_t num_bytes) : OutOfMemory("FailedToCreateSlab", num_bytes) {}
};

class TooBigForSlab : public OutOfMemory {
 public:
  TooBigForSlab(size_t num_bytes) : OutOfMemory("TooBigForSlab", num_bytes) {}
};
