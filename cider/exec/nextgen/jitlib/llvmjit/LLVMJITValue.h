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
#ifndef JITLIB_LLVMJIT_LLVMJITVALUE_H
#define JITLIB_LLVMJIT_LLVMJITVALUE_H

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"

namespace cider::jitlib {
class LLVMIfBuilder;
class LLVMForBuilder;

class LLVMJITValue final : public JITValue {
  friend LLVMJITFunction;
  friend LLVMIfBuilder;
  friend LLVMForBuilder;

 public:
  explicit LLVMJITValue(JITTypeTag type_tag,
                        LLVMJITFunction& parent_function,
                        llvm::Value* value,
                        const std::string& name = "value",
                        JITBackendTag backend = JITBackendTag::LLVMJIT,
                        bool is_variable = false)
      : JITValue(type_tag, parent_function, name, backend)
      , parent_function_(parent_function)
      , llvm_value_(value)
      , is_variable_(is_variable) {}

 public:
  JITValue& assign(JITValue& value) override;

  JITValuePointer notOp() override;

  JITValuePointer add(JITValue& rh) override;
  JITValuePointer sub(JITValue& rh) override;
  JITValuePointer mul(JITValue& rh) override;
  JITValuePointer div(JITValue& rh) override;
  JITValuePointer mod(JITValue& rh) override;

 private:
  static llvm::IRBuilder<>& getFunctionBuilder(const LLVMJITFunction& function) {
    return static_cast<llvm::IRBuilder<>&>(function);
  }

  static void checkOprandsType(JITTypeTag lh, JITTypeTag rh, const char* op);

  llvm::Value* load();

  llvm::Value* store(LLVMJITValue& rh);

  LLVMJITFunction& parent_function_;
  llvm::Value* llvm_value_;
  bool is_variable_;
};
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITVALUE_H
