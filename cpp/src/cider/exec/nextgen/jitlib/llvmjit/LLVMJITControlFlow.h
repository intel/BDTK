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
#ifndef JITLIB_LLVMJIT_LLVMJITCONTROLFLOW_H
#define JITLIB_LLVMJIT_LLVMJITCONTROLFLOW_H

#include <llvm/IR/IRBuilder.h>

#include "exec/nextgen/jitlib/base/JITControlFlow.h"

namespace cider::jitlib {
class LLVMIfBuilder final : public IfBuilder {
 public:
  LLVMIfBuilder(llvm::Function& function, llvm::IRBuilder<>& builder)
      : func_(function), builder_(builder) {}

  // TBD: Refactor with template to avoid using std::function.
  void build() override;

 private:
  llvm::Function& func_;
  llvm::IRBuilder<>& builder_;
};

class LLVMLoopBuilder final : public LoopBuilder {
 public:
  LLVMLoopBuilder(llvm::Function& function, llvm::IRBuilder<>& builder)
      : func_(function), builder_(builder) {}

  void build() override;

  void loopContinue() override;

 private:
  llvm::Function& func_;
  llvm::IRBuilder<>& builder_;

  llvm::BasicBlock* update_block_{nullptr};
};
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITCONTROLFLOW_H
