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
#ifndef JITLIB_LLVM_JIT_CONTROL_FLOW_H
#define JITLIB_LLVM_JIT_CONTROL_FLOW_H

#include "exec/nextgen/jitlib/base/ControlFlow.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

namespace jitlib {
template <TypeTag Type>
class Ret<Type, LLVMJITFunction> {
 public:
  explicit Ret(llvm::IRBuilder<>& builder) { builder.CreateRetVoid(); }

  explicit Ret(llvm::IRBuilder<>& builder, Value<Type, LLVMJITFunction>& ret_value) {
    builder.CreateRet(ret_value.load());
  }
};
};  // namespace jitlib
#endif