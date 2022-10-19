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
#ifndef LLVM_JIT_FUNCTION_H
#define LLVM_JIT_FUNCTION_H

#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>

#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/base/Values.h"

namespace jitlib {
class LLVMJITModule;

class LLVMJITFunction final : public JITFunction<LLVMJITFunction> {
 public:
  template <TypeTag Type, typename FunctionImpl>
  friend class Value;

  template <TypeTag Type, typename FunctionImpl>
  friend class Ret;

  template <typename JITFunctionImpl>
  friend class JITFunction;

 public:
  explicit LLVMJITFunction(const JITFunctionDescriptor& descriptor,
                           LLVMJITModule& module,
                           llvm::Function& func);

 protected:
  void finishImpl();
  llvm::IRBuilder<>* getIRBuilder() { return ir_builder_.get(); }
  llvm::LLVMContext& getContext();

 private:
  LLVMJITModule& module_;
  llvm::Function& func_;
  std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
};
};  // namespace jitlib

#endif