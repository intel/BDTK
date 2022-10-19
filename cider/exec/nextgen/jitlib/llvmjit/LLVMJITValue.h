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
#ifndef JITLIB_LLVM_JIT_VALUE_H
#define JITLIB_LLVM_JIT_VALUE_H

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Value.h>

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
  static Value createVariableImpl(LLVMJITFunction& function,
                                  const char* name,
                                  NativeType init) {
    auto llvm_type = getLLVMType(Type, function.getContext());

    llvm::AllocaInst* variable_memory = function.getIRBuilder()->CreateAlloca(llvm_type);
    variable_memory->setName(name);
    variable_memory->setAlignment(TypeTraits<Type>::width);

    llvm::Value* init_value = getLLVMConstant(init, Type, function.getContext());
    function.getIRBuilder()->CreateStore(init_value, variable_memory, false);

    return Value(function, variable_memory, true);
  }

  llvm::Value* load() {
    if (is_variable_) {
      return parent_function_.getIRBuilder()->CreateLoad(llvm_value_, false);
    } else {
      return llvm_value_;
    }
  }

  llvm::Value* store(Value& rh) {
    if (is_variable_) {
      return parent_function_.getIRBuilder()->CreateStore(rh.load(), llvm_value_, false);
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

#endif