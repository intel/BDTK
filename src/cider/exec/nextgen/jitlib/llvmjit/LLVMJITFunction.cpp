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
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "exec/nextgen/jitlib/base/JITValueOperations.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITControlFlow.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"
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

JITValuePointer LLVMJITFunction::createLocalJITValueImpl(
    LocalJITValueBuilderEmitter emitter,
    void* builder) {
  auto current_block = ir_builder_->GetInsertBlock();
  auto current_insert_iter = ir_builder_->GetInsertPoint();

  auto& local_var_block = current_block->getParent()->getEntryBlock();
  auto local_var_inst_iter = local_var_block.end();
  ir_builder_->SetInsertPoint(&local_var_block, --local_var_inst_iter);

  JITValuePointer ret(emitter(builder));

  ir_builder_->SetInsertPoint(current_block, current_insert_iter);

  return ret;
}

JITValuePointer LLVMJITFunction::createVariableImpl(JITTypeTag type_tag,
                                                    const std::string& name,
                                                    JITValuePointer& init_val) {
  return createLocalJITValue([type_tag, &name, &init_val, this] {
    auto llvm_type = getLLVMType(type_tag, getLLVMContext());
    llvm::AllocaInst* variable_memory = ir_builder_->CreateAlloca(llvm_type);
    variable_memory->setName(name);
    variable_memory->setAlignment(getJITTypeSize(type_tag));

    auto value =
        makeJITValuePointer<LLVMJITValue>(type_tag, *this, variable_memory, name, true);
    *value = init_val;

    return value;
  });
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
llvm::Value* createConstantImpl(llvm::LLVMContext& context, const std::any& value) {
  NativeType actual_value = std::any_cast<NativeType>(value);
  if constexpr (std::is_floating_point_v<NativeType>) {
    return getLLVMConstantFP(actual_value, type_tag, context);
  } else {
    return getLLVMConstantInt(actual_value, type_tag, context);
  }
}

template <JITTypeTag type_tag,
          typename NativeType = typename JITTypeTraits<type_tag>::NativeType>
llvm::Value* createConstantImpl(llvm::LLVMContext& context,
                                llvm::IRBuilder<>* builder,
                                const std::any& value) {
  static_assert(std::is_same_v<NativeType, std::string>,
                "this function should only be used to create string literals");
  NativeType actual_value = std::any_cast<NativeType>(value);
  return getLLVMConstantGlobalStr(actual_value, builder, context);
}

JITValuePointer LLVMJITFunction::createLiteralImpl(JITTypeTag type_tag,
                                                   const std::any& value) {
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
    case JITTypeTag::POINTER:
      type_tag = JITTypeTag::INT64;
    case JITTypeTag::INT64:
      llvm_value = createConstantImpl<JITTypeTag::INT64>(getLLVMContext(), value);
      break;
    case JITTypeTag::FLOAT:
      llvm_value = createConstantImpl<JITTypeTag::FLOAT>(getLLVMContext(), value);
      break;
    case JITTypeTag::DOUBLE:
      llvm_value = createConstantImpl<JITTypeTag::DOUBLE>(getLLVMContext(), value);
      break;
    case JITTypeTag::VARCHAR:
      llvm_value = createConstantImpl<JITTypeTag::VARCHAR>(
          getLLVMContext(), ir_builder_.get(), value);
      break;
    default:
      LOG(FATAL) << "Invalid JITTypeTag in LLVMJITFunction::createLiteralImpl: "
                 << getJITTypeName(type_tag);
  }
  return makeJITValuePointer<LLVMJITValue>(type_tag, *this, llvm_value, "", false);
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
    return makeJITValuePointer<LLVMJITValue>(
        descriptor.ret_type, *this, ans, "ret", false);
  } else {
    LOG(FATAL) << "Invalid target function in LLVMJITFunction::emitJITFunctionCall.";
    return JITValuePointer(nullptr);
  }
}

// look up a runtime function based on the name
JITValuePointer LLVMJITFunction::emitRuntimeFunctionCall(
    const std::string& fname,
    const JITFunctionEmitDescriptor& descriptor) {
  auto func = module_.module_->getFunction(fname);
  if (!func) {
    LOG(FATAL) << "Function: " << fname << " does not exist.";
  }
  cloneFunctionRecursive(func);

  llvm::SmallVector<llvm::Value*, JITFunctionEmitDescriptor::DefaultParamsNum> args;
  args.reserve(descriptor.params_vector.size());
  for (auto jit_value : descriptor.params_vector) {
    LLVMJITValue* llvmjit_value = static_cast<LLVMJITValue*>(jit_value);
    args.push_back(llvmjit_value->load());
  }

  llvm::Value* ans = ir_builder_->CreateCall(func, args);
  return makeJITValuePointer<LLVMJITValue>(
      descriptor.ret_type, *this, ans, "ret", false, descriptor.ret_sub_type);
}

void LLVMJITFunction::cloneFunctionRecursive(llvm::Function* fn) {
  CHECK(fn);
  if (!fn->isDeclaration()) {
    return;
  }
  // Get the implementation from the runtime module.
  auto func_impl = module_.runtime_module_->getFunction(fn->getName());
  CHECK(func_impl) << fn->getName().str();
  if (func_impl->isDeclaration()) {
    return;
  }

  auto target_it = fn->arg_begin();
  for (auto arg_it = func_impl->arg_begin(); arg_it != func_impl->arg_end(); ++arg_it) {
    target_it->setName(arg_it->getName());
    module_.vmap_[&*arg_it] = &*target_it++;
  }

  llvm::SmallVector<llvm::ReturnInst*, JITFunctionEmitDescriptor::DefaultParamsNum>
      returns;  // Ignore returns cloned.
#if LLVM_VERSION_MAJOR > 12
  llvm::CloneFunctionInto(fn,
                          func_impl,
                          module_.vmap_,
                          llvm::CloneFunctionChangeType::DifferentModule,
                          Returns);
#else
  llvm::CloneFunctionInto(
      fn, func_impl, module_.vmap_, /*ModuleLevelChanges=*/true, returns);
#endif

  for (auto it = llvm::inst_begin(fn), e = llvm::inst_end(fn); it != e; ++it) {
    if (llvm::isa<llvm::CallInst>(*it)) {
      auto& call = llvm::cast<llvm::CallInst>(*it);
      cloneFunctionRecursive(call.getCalledFunction());
    }
  }
}

JITValuePointer LLVMJITFunction::getArgument(size_t index) {
  if (index > descriptor_.params_type.size()) {
    LOG(FATAL) << "Index out of range in LLVMJITFunction::getArgument.";
  }

  auto& param_type = descriptor_.params_type[index];
  llvm::Value* llvm_value = func_.arg_begin() + index;
  switch (param_type.type) {
    case JITTypeTag::INVALID:
    case JITTypeTag::TUPLE:
    case JITTypeTag::STRUCT:
      UNREACHABLE();
    default:
      return makeJITValuePointer<LLVMJITValue>(param_type.type,
                                               *this,
                                               llvm_value,
                                               param_type.name,
                                               false,
                                               param_type.sub_type);
  }
}

JITValuePointer LLVMJITFunction::packJITValuesImpl(
    const std::vector<JITValuePointer>& vals,
    const uint64_t alignment) {
  int64_t memory_count = 0;
  // record memory index address
  std::vector<int64_t> memory_index;
  for (auto val : vals) {
    memory_index.push_back(memory_count);
    memory_count += getJITTypeSize(val->getValueTypeTag());
    // memory align
    memory_count = (memory_count + alignment - 1) & ~(alignment - 1);
  }

  llvm::AllocaInst* allocated_memory = ir_builder_->CreateAlloca(
      llvm::Type::getInt8Ty(getLLVMContext()),
      getLLVMConstantInt(memory_count, JITTypeTag::INT64, getLLVMContext()));
  auto start_address = allocated_memory;
  auto start_val = makeJITValuePointer<LLVMJITValue>(JITTypeTag::POINTER,
                                                     *this,
                                                     allocated_memory,
                                                     "allocated_memory",
                                                     false,
                                                     JITTypeTag::INT8);

  // store JITValuePointer
  for (int i = 0; i < vals.size(); ++i) {
    auto offset = createLiteral(JITTypeTag::INT64, memory_index[i]);
    auto memory_val =
        (start_val + offset)->castPointerSubType(vals[i]->getValueTypeTag());
    **memory_val = *vals[i];
  }
  return makeJITValuePointer<LLVMJITValue>(JITTypeTag::POINTER,
                                           *this,
                                           start_address,
                                           "start_address",
                                           false,
                                           JITTypeTag::INT8);
}

IfBuilderPointer LLVMJITFunction::createIfBuilder() {
  return std::make_unique<LLVMIfBuilder>(func_, *ir_builder_);
}

LoopBuilderPointer LLVMJITFunction::createLoopBuilder() {
  return std::make_unique<LLVMLoopBuilder>(func_, *ir_builder_);
}
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITFUNCTION_H
