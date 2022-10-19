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
#ifndef JIT_Function_H
#define JIT_Function_H

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
};  // namespace jitlib

#endif