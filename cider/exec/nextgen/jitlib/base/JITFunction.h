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
#ifndef JITLIB_BASE_JITFUNCTION_H
#define JITLIB_BASE_JITFUNCTION_H

#include <any>
#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/jitlib/base/JITValue.h"

namespace jitlib {
enum JITFunctionParamAttr : uint64_t {};

struct JITFunctionParam {
  std::string name{""};
  JITTypeTag type;
  uint64_t attribute;
};

struct JITFunctionDescriptor {
  static constexpr size_t DefaultParamsNum = 8;
  std::string function_name;
  JITFunctionParam ret_type;
  boost::container::small_vector<JITFunctionParam, DefaultParamsNum> params_type;
};

struct JITFunctionEmitDescriptor {
  static constexpr size_t DefaultParamsNum = 8;
  JITTypeTag ret_type;
  boost::container::small_vector<JITValue*, DefaultParamsNum> params_vector;
};

class JITFunction {
 public:
  JITFunction(const JITFunctionDescriptor& descriptor) : descriptor_(descriptor) {}

  const JITFunctionDescriptor* getFunctionDescriptor() const { return &descriptor_; }

  template <typename R, typename... Args>
  auto getFunctionPointer() {
    if constexpr (sizeof...(Args) > 0) {
      using func_type = R (*)(Args...);
      return reinterpret_cast<func_type>(getFunctionPointer());
    } else {
      using func_type = R (*)();
      return reinterpret_cast<func_type>(getFunctionPointer());
    }
  }

  virtual JITValuePointer createVariable(const std::string& name,
                                          JITTypeTag type_tag) = 0;

  virtual JITValuePointer createConstant(JITTypeTag type_tag, std::any value) = 0;

  virtual void createReturn() = 0;

  virtual void createReturn(JITValue& value) = 0;

  virtual JITValuePointer emitJITFunctionCall(
      JITFunction& function,
      const JITFunctionEmitDescriptor& descriptor) = 0;

  virtual void finish() = 0;

 protected:
  JITFunctionDescriptor descriptor_;

 private:
  virtual void* getFunctionPointer() = 0;
};

using JITFunctionPointer = std::shared_ptr<JITFunction>;
};  // namespace jitlib

#endif  // JITLIB_BASE_JITFUNCTION_H
