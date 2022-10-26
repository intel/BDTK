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
#ifndef JITLIB_BASE_VALUES_H
#define JITLIB_BASE_VALUES_H

#include <memory>

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace jitlib {
class JITValue;

using JITValuePointer = std::shared_ptr<JITValue>;

class JITValue {
 public:
  JITValue(JITTypeTag type_tag,
           const std::string& name = "value",
           JITBackendTag backend = JITBackendTag::LLVMJIT)
      : value_name_(name), type_tag_(type_tag), backend_tag_(backend) {}

  JITValue(const JITValue&) = delete;
  JITValue(JITValue&&) = delete;
  JITValue& operator=(JITValue&& rh) = delete;

  JITValue& operator=(JITValue& rh) { return assign(rh); }

  std::string getValueName() const { return value_name_; }

  JITTypeTag getValueTypeTag() const { return type_tag_; }

  JITBackendTag getValueBackendTag() const { return backend_tag_; }

  virtual JITValue& assign(JITValue& value) = 0;

 protected:
  std::string value_name_;
  JITTypeTag type_tag_;
  JITBackendTag backend_tag_;
};
};  // namespace jitlib

#endif  // JITLIB_BASE_VALUES_H
