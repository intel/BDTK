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

#ifndef CIDER_CIDEREXCEPTION_H
#define CIDER_CIDEREXCEPTION_H

#include <fmt/core.h>
#include <stdexcept>
#include <string>

// CIDER_THROW(CiderCompileException, "Exception message");
#define CIDER_THROW(type, msg) \
  throw type(fmt::format("[{}:{}]: {}", __FILE__, std::to_string(__LINE__), msg))

class CiderException : public std::exception {
 public:
  explicit CiderException(const std::string& msg) : msg_("[CiderException]" + msg) {}
  explicit CiderException(std::string type, const std::string& msg)
      : msg_("[" + type + "]" + msg) {}
  virtual ~CiderException() noexcept {}

  const char* what() const noexcept override { return msg_.c_str(); }

 private:
  const std::string msg_;
};

// please use CIDER_THROW(CiderRuntimeException, "Exception message");
class CiderRuntimeException : public CiderException {
 public:
  explicit CiderRuntimeException(const std::string& msg)
      : CiderException("CiderRuntimeException", msg) {}

  // for subclass inheritance
  explicit CiderRuntimeException(std::string type, const std::string& msg)
      : CiderException(type, msg) {}

  virtual ~CiderRuntimeException() noexcept {}
};

// please use CIDER_THROW(CiderCompileException, "Exception message");
class CiderCompileException : public CiderException {
 public:
  explicit CiderCompileException(const std::string& msg)
      : CiderException("CiderCompileException", msg) {}

  // for subclass inheritance
  explicit CiderCompileException(std::string type, const std::string& msg)
      : CiderException(type, msg) {}

  virtual ~CiderCompileException() noexcept {}
};

// please use CIDER_THROW(CiderPlanValidateException, "Exception message");
class CiderPlanValidateException : public CiderException {
 public:
  explicit CiderPlanValidateException(const std::string& msg)
      : CiderException("CiderPlanValidateException", msg) {}

  // for subclass inheritance
  explicit CiderPlanValidateException(std::string type, const std::string& msg)
      : CiderException(type, msg) {}

  virtual ~CiderPlanValidateException() noexcept {}
};

// please use CIDER_THROW(CiderOutOfMemoryException, "Exception message");
class CiderOutOfMemoryException : public CiderRuntimeException {
 public:
  explicit CiderOutOfMemoryException(const std::string& msg)
      : CiderRuntimeException("CiderOutOfMemoryException", msg) {}

  virtual ~CiderOutOfMemoryException() noexcept {}
};

// please use CIDER_THROW(CiderWatchdogException, "Exception message");
class CiderWatchdogException : public CiderCompileException {
 public:
  explicit CiderWatchdogException(const std::string& msg)
      : CiderCompileException("CiderWatchdogException", msg) {}

  virtual ~CiderWatchdogException() noexcept {}
};

// please use CIDER_THROW(CiderHashJoinException, "Exception message");
class CiderHashJoinException : public CiderCompileException {
 public:
  explicit CiderHashJoinException(const std::string& msg)
      : CiderCompileException("CiderHashJoinException", msg) {}

  // for subclass inheritance
  explicit CiderHashJoinException(std::string type, const std::string& msg)
      : CiderCompileException(type, msg) {}

  virtual ~CiderHashJoinException() noexcept {}
};

// please use CIDER_THROW(CiderOneToMoreHashException, "Exception message");
class CiderOneToMoreHashException : public CiderHashJoinException {
 public:
  explicit CiderOneToMoreHashException(const std::string& msg)
      : CiderHashJoinException("CiderOneToMoreHashException", msg) {}

  virtual ~CiderOneToMoreHashException() noexcept {}
};

// please use CIDER_THROW(CiderTooManyHashEntriesException, "Exception message");
class CiderTooManyHashEntriesException : public CiderHashJoinException {
 public:
  explicit CiderTooManyHashEntriesException(const std::string& msg)
      : CiderHashJoinException("CiderTooManyHashEntriesException", msg) {}

  virtual ~CiderTooManyHashEntriesException() noexcept {}
};

// please use CIDER_THROW(CiderUnsupportedException, "Exception message");
class CiderUnsupportedException : public CiderCompileException {
 public:
  explicit CiderUnsupportedException(const std::string& msg)
      : CiderCompileException("CiderUnsupportedException", msg) {}

  virtual ~CiderUnsupportedException() noexcept {}
};

// please use CIDER_THROW(CheckFatalException, "Exception message");
class CheckFatalException : public CiderCompileException {
 public:
  explicit CheckFatalException(const std::string& msg)
      : CiderCompileException("CheckFatalException", msg) {}

  virtual ~CheckFatalException() noexcept {}
};
#endif  // CIDER_CIDEREXCEPTION_H
