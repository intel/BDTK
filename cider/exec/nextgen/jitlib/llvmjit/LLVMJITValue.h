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

#include "exec/nextgen/jitlib/base/Values.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"

namespace jitlib {

template <TypeTag Type>
class Value<Type, LLVMJITFunction> final
    : public BasicValue<Value<Type, LLVMJITFunction>, Type, LLVMJITFunction> {
 public:
  using FunctionImpl = LLVMJITFunction;
  using NativeType = typename TypeTraits<Type>::NativeType;

 public:
  template <TypeTag, typename FunctionImpl>
  friend class Ret;
  template <typename ValueImpl, TypeTag, typename FunctionImpl>
  friend class BasicValue;

 public:
  explicit Value(LLVMJITFunction& function, llvm::Value* value, bool is_variable)
      : parent_function_(function), llvm_value_(value), is_variable_(is_variable) {}

 protected:
  static llvm::IRBuilder<>& getFunctionBuilder(const LLVMJITFunction& function) {
    return static_cast<llvm::IRBuilder<>&>(function);
  }

  static Value createVariableImpl(LLVMJITFunction& function,
                                  const char* name,
                                  NativeType init) {
    auto llvm_type = getLLVMType(Type, function.getLLVMContext());

    llvm::IRBuilder<>& builder = getFunctionBuilder(function);
    auto current_block = builder.GetInsertBlock();
    auto& local_var_block = current_block->getParent()->getEntryBlock();
    auto iter = local_var_block.end();
    builder.SetInsertPoint(&local_var_block, --iter);

    llvm::AllocaInst* variable_memory = builder.CreateAlloca(llvm_type);
    variable_memory->setName(name);
    variable_memory->setAlignment(TypeTraits<Type>::width);

    llvm::Value* init_value = getLLVMConstant(init, Type, function.getLLVMContext());
    builder.CreateStore(init_value, variable_memory, false);

    builder.SetInsertPoint(current_block);

    return Value(function, variable_memory, true);
  }

  llvm::Value* load() {
    if (is_variable_) {
      return getFunctionBuilder(parent_function_).CreateLoad(llvm_value_, false);
    } else {
      return llvm_value_;
    }
  }

  llvm::Value* store(Value& rh) {
    if (is_variable_) {
      return getFunctionBuilder(parent_function_)
          .CreateStore(rh.load(), llvm_value_, false);
    } else {
      // TODO (bigPYJ1151): Add Exception
    }
  }

 private:
  LLVMJITFunction& parent_function_;
  llvm::Value* llvm_value_;
  bool is_variable_;
};
};  // namespace jitlib

#endif // JITLIB_LLVMJIT_LLVMJITVALUE_H
