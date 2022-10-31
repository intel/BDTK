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
#ifndef JITLIB_LLVMJIT_LLVMJITFUNCTION_H
#define JITLIB_LLVMJIT_LLVMJITFUNCTION_H

#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

#include <llvm/IR/Function.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_os_ostream.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITControlFlow.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"
#include "util/Logger.h"

namespace cider::jitlib {
LLVMJITFunction::LLVMJITFunction(const JITFunctionDescriptor& descriptor,
                                 LLVMJITModule& module,
                                 llvm::Function& func)
    : JITFunction(descriptor), module_(module), func_(func), ir_builder_(nullptr) {
  auto local_variable_block =
      llvm::BasicBlock::Create(getLLVMContext(), ".Local_Vars", &func_);
  auto entry_block = llvm::BasicBlock::Create(getLLVMContext(), ".Start", &func_);

  ir_builder_ = std::make_unique<llvm::IRBuilder<>>(local_variable_block);
  ir_builder_->CreateBr(entry_block);

  ir_builder_->SetInsertPoint(entry_block);
}

llvm::LLVMContext& LLVMJITFunction::getLLVMContext() {
  return module_.getLLVMContext();
}

void LLVMJITFunction::finish() {
  std::stringstream error_msg;
  llvm::raw_os_ostream error_os(error_msg);
  if (llvm::verifyFunction(func_, &error_os)) {
    error_os << "\n-----\n";
    func_.print(error_os);
    error_os << "\n-----\n";
    LOG(FATAL) << error_msg.str();
  }
}

void* LLVMJITFunction::getFunctionPointer() {
  return module_.getFunctionPtrImpl(*this);
}

JITValuePointer LLVMJITFunction::createVariable(const std::string& name,
                                                JITTypeTag type_tag) {
  auto llvm_type = getLLVMType(type_tag, getLLVMContext());

  auto current_block = ir_builder_->GetInsertBlock();
  auto& local_var_block = current_block->getParent()->getEntryBlock();
  auto iter = local_var_block.end();
  ir_builder_->SetInsertPoint(&local_var_block, --iter);

  llvm::AllocaInst* variable_memory = ir_builder_->CreateAlloca(llvm_type);
  variable_memory->setName(name);
  variable_memory->setAlignment(getJITTypeSize(type_tag));

  ir_builder_->SetInsertPoint(current_block);

  return std::make_unique<LLVMJITValue>(
      type_tag, *this, variable_memory, name, JITBackendTag::LLVMJIT, true);
}

void LLVMJITFunction::createReturn() {
  ir_builder_->CreateRetVoid();
}

void LLVMJITFunction::createReturn(JITValue& value) {
  if (LLVMJITValue* llvmjit_value = dynamic_cast<LLVMJITValue*>(&value); llvmjit_value) {
    ir_builder_->CreateRet(llvmjit_value->load());
  } else {
    UNREACHABLE();
  }
}

template <JITTypeTag type_tag,
          typename NativeType = typename JITTypeTraits<type_tag>::NativeType>
llvm::Value* createConstantImpl(llvm::LLVMContext& context, std::any value) {
  NativeType actual_value = std::any_cast<NativeType>(value);
  if constexpr (std::is_floating_point_v<NativeType>) {
    return getLLVMConstantFP(actual_value, type_tag, context);
  } else {
    return getLLVMConstantInt(actual_value, type_tag, context);
  }
}

JITValuePointer LLVMJITFunction::createConstant(JITTypeTag type_tag, std::any value) {
  llvm::Value* llvm_value = nullptr;
  switch (type_tag) {
    case JITTypeTag::BOOL:
      llvm_value = createConstantImpl<JITTypeTag::BOOL>(getLLVMContext(), value);
      break;
    case JITTypeTag::INT8:
      llvm_value = createConstantImpl<JITTypeTag::INT8>(getLLVMContext(), value);
      break;
    case JITTypeTag::INT16:
      llvm_value = createConstantImpl<JITTypeTag::INT16>(getLLVMContext(), value);
      break;
    case JITTypeTag::INT32:
      llvm_value = createConstantImpl<JITTypeTag::INT32>(getLLVMContext(), value);
      break;
    case JITTypeTag::INT64:
      llvm_value = createConstantImpl<JITTypeTag::INT64>(getLLVMContext(), value);
      break;
    case JITTypeTag::FLOAT:
      llvm_value = createConstantImpl<JITTypeTag::FLOAT>(getLLVMContext(), value);
      break;
    case JITTypeTag::DOUBLE:
      llvm_value = createConstantImpl<JITTypeTag::DOUBLE>(getLLVMContext(), value);
      break;
    default:
      LOG(FATAL) << "Invalid JITTypeTag in LLVMJITFunction::createConstant: "
                 << getJITTypeName(type_tag);
  }
  return std::make_unique<LLVMJITValue>(
      type_tag, *this, llvm_value, "", JITBackendTag::LLVMJIT, false);
}

JITValuePointer LLVMJITFunction::emitJITFunctionCall(
    JITFunction& function,
    const JITFunctionEmitDescriptor& descriptor) {
  if (LLVMJITFunction& llvmjit_function = dynamic_cast<LLVMJITFunction&>(function);
      &llvmjit_function.module_ == &module_) {
    llvm::SmallVector<llvm::Value*, JITFunctionEmitDescriptor::DefaultParamsNum> args;
    args.reserve(descriptor.params_vector.size());

    for (auto jit_value : descriptor.params_vector) {
      LLVMJITValue* llvmjit_value = static_cast<LLVMJITValue*>(jit_value);
      args.push_back(llvmjit_value->llvm_value_);
    }

    llvm::Value* ans = ir_builder_->CreateCall(&llvmjit_function.func_, args);
    return std::make_unique<LLVMJITValue>(
        descriptor.ret_type, *this, ans, "ret", JITBackendTag::LLVMJIT, false);
  } else {
    LOG(FATAL) << "Invalid target function in LLVMJITFunction::emitJITFunctionCall.";
    return nullptr;
  }
}

JITValuePointer LLVMJITFunction::getArgument(size_t index) {
  if (index > descriptor_.params_type.size()) {
    LOG(FATAL) << "Index out of range in LLVMJITFunction::getArgument.";
  }

  auto& param_type = descriptor_.params_type[index];
  llvm::Value* llvm_value = func_.arg_begin() + index;
  switch (param_type.type) {
    case JITTypeTag::POINTER:
    case JITTypeTag::INVALID:
    case JITTypeTag::TUPLE:
    case JITTypeTag::STRUCT:
      UNREACHABLE();
    default:
      return std::make_unique<LLVMJITValue>(param_type.type,
                                            *this,
                                            llvm_value,
                                            param_type.name,
                                            JITBackendTag::LLVMJIT,
                                            false);
  }
}

IfBuilderPointer LLVMJITFunction::getIfBuilder() {
  return std::make_unique<LLVMIfBuilder>(func_, *ir_builder_);
}

ForBuilderPointer LLVMJITFunction::getForBuilder() {
  return std::make_unique<LLVMForBuilder>(func_, *ir_builder_);
}
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITFUNCTION_H
