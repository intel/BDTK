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
#ifndef JITLIB_JIT_MODULE_H
#define JITLIB_JIT_MODULE_H

#include "exec/nextgen/jitlib/base/JITFunction.h"

namespace jitlib {
template <typename JITModuleImpl, typename JITFunctionImpl>
class JITModule {
 public:
  JITFunctionImpl createJITFunction(const JITFunctionDescriptor& descriptor) {
    return getImpl()->createJITFunctionImpl(descriptor);
  }

  void finish() { getImpl()->finishImpl(); }

  void* getFunctionPtr(JITFunctionImpl& function) {
    return getImpl()->getFunctionPtrImpl(function);
  }

 private:
  JITModuleImpl* getImpl() { return static_cast<JITModuleImpl*>(this); }
};
};  // namespace jitlib

#endif