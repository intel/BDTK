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

#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace jitlib {
enum JITFunctionParamAttr : uint64_t {};

struct JITFunctionParam {
  const char* name{""};
  TypeTag type;
  uint64_t attribute;
};

struct JITFunctionDescriptor {
  static constexpr size_t DefaultParamsNum = 8;

  const char* function_name;
  JITFunctionParam ret_type;
  boost::container::small_vector<JITFunctionParam, DefaultParamsNum> params_type;
};

template <typename JITFunctionImpl>
class JITFunction {
 public:
  const JITFunctionDescriptor* getFunctionDescriptor() const { return &descriptor_; }

  void finish() { getImpl()->finishImpl(); }

 protected:
  JITFunction(const JITFunctionDescriptor& descriptor) : descriptor_(descriptor) {}

  JITFunctionImpl* getImpl() { return static_cast<JITFunctionImpl*>(this); }

  JITFunctionDescriptor descriptor_;
};

template <typename R, typename... Args>
inline auto castFunctionPointer(void* ptr) {
  using func_type = R (*)(Args...);
  return reinterpret_cast<func_type>(ptr);
}

};  // namespace jitlib

#endif // JITLIB_BASE_JITFUNCTION_H
