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
#ifndef JITLIB_LLVMJIT_LLVMJITMODULE_H
#define JITLIB_LLVMJIT_LLVMJITMODULE_H

#include "exec/nextgen/jitlib/base/JITModule.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

namespace cider::jitlib {
class LLVMJITModule final : public JITModule {
 public:
  friend LLVMJITEngineBuilder;
  friend LLVMJITFunction;

 public:
  explicit LLVMJITModule(const std::string& name);

  JITFunctionPointer createJITFunction(const JITFunctionDescriptor& descriptor) override;

  void finish() override;

 protected:
  llvm::LLVMContext& getLLVMContext() { return *context_; }
  void* getFunctionPtrImpl(LLVMJITFunction& function);

 private:
  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::Module> module_;
  std::unique_ptr<LLVMJITEngine> engine_;
};
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITMODULE_H
