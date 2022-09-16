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

#ifndef CIDER_CIDEREXCEPTION_H
#define CIDER_CIDEREXCEPTION_H

#include <stdexcept>
#include <string>

// CIDER_THROW(CiderCompileException, "xxx");
#define CIDER_THROW(type, msg) \
  throw type("[" + std::string(__FILE__) + ":" + std::to_string(__LINE__) + "]: " + msg)

class CiderException : public std::exception {
 public:
  explicit CiderException(std::string type, const std::string& msg)
      : msg_("[" + type + "]" + msg) {}
  virtual ~CiderException() noexcept {};

  const char* what() const noexcept override { return msg_.c_str(); }

 private:
  const std::string msg_;
};

// please use CIDER_THROW(CiderRuntimeException, "xxx");
class CiderRuntimeException : public CiderException {
 public:
  explicit CiderRuntimeException(const std::string& msg)
      : CiderException("CiderRuntimeException", msg) {}
  virtual ~CiderRuntimeException() noexcept {};
};

// please use CIDER_THROW(CiderCompileException, "xxx");
class CiderCompileException : public CiderException {
 public:
  explicit CiderCompileException(const std::string& msg)
      : CiderException("CiderCompileException", msg) {}
  virtual ~CiderCompileException() noexcept {};
};

#endif  // CIDER_CIDEREXCEPTION_H
