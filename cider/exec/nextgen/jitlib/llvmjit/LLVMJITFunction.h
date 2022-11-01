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
#ifndef JITLIB_LLVMJIT_FUNCTION_H
#define JITLIB_LLVMJIT_FUNCTION_H

#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <any>

#include "exec/nextgen/jitlib/base/JITFunction.h"

namespace cider::jitlib {
class LLVMJITModule;

class LLVMJITFunction final : public JITFunction {
 public:
  explicit LLVMJITFunction(const JITFunctionDescriptor& descriptor,
                           LLVMJITModule& module,
                           llvm::Function& func);

  operator llvm::IRBuilder<>&() const { return *ir_builder_; }

  JITValuePointer createVariable(const std::string& name, JITTypeTag type_tag) override;

  JITValuePointer createConstant(JITTypeTag type_tag, std::any value) override;

  JITValuePointer getArgument(size_t index) override;

  void createReturn() override;

  void createReturn(JITValue& value) override;

  JITValuePointer emitJITFunctionCall(
      JITFunction& function,
      const JITFunctionEmitDescriptor& descriptor) override;

  void finish() override;

 protected:
  llvm::LLVMContext& getLLVMContext();

 private:
  void* getFunctionPointer() override;

  LLVMJITModule& module_;
  llvm::Function& func_;
  mutable std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
};
};  // namespace cider::jitlib

#endif