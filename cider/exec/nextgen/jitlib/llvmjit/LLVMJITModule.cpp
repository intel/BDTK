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
#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"

#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/IR/Module.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"
#include "util/Logger.h"
#include "util/filesystem/cider_path.h"

namespace cider::jitlib {

static llvm::MemoryBuffer* getRuntimeBuffer() {
  static std::once_flag has_set_buffer;
  static std::unique_ptr<llvm::MemoryBuffer> runtime_function_buffer;

  std::call_once(has_set_buffer, [&]() {
    auto root_path = cider::get_root_abs_path();
    auto template_path = root_path + "/function/RuntimeFunctions.bc";
    CHECK(boost::filesystem::exists(template_path));

    auto buffer_or_error = llvm::MemoryBuffer::getFile(template_path);
    CHECK(!buffer_or_error.getError()) << "bc_filename=" << template_path;
    runtime_function_buffer = std::move(buffer_or_error.get());
  });

  return runtime_function_buffer.get();
}

LLVMJITModule::LLVMJITModule(const std::string& name, bool should_copy_runtime_module)
    : context_(std::make_unique<llvm::LLVMContext>()), engine_(nullptr) {
  if (should_copy_runtime_module) {
    auto expected_res =
        llvm::parseBitcodeFile(getRuntimeBuffer()->getMemBufferRef(), *context_);
    if (!expected_res) {
      LOG(FATAL) << "LLVM IR ParseError: Something wrong when parsing bitcode.";
    } else {
      runtime_module_ = std::move(expected_res.get());
    }
    copyRuntimeModule();
  } else {
    module_ = std::make_unique<llvm::Module>(name, *context_);
  }
  CHECK(module_);
}

static llvm::FunctionType* getFunctionSignature(const JITFunctionDescriptor& descriptor,
                                                llvm::LLVMContext& context) {
  llvm::Type* ret_type = getLLVMType(descriptor.ret_type.type, context);

  llvm::SmallVector<llvm::Type*, JITFunctionDescriptor::DefaultParamsNum> arguments;
  arguments.reserve(descriptor.params_type.size());

  for (const JITFunctionParam& param_descriptor : descriptor.params_type) {
    llvm::Type* arg_type = getLLVMType(param_descriptor.type, context);
    if (arg_type) {
      arguments.push_back(arg_type);
    } else {
      LOG(ERROR) << "Invalid argument type in getFunctionSignature: "
                 << getJITTypeName(param_descriptor.type);
      return nullptr;
    }
  }

  return llvm::FunctionType::get(ret_type, arguments, false);
}

JITFunctionPointer LLVMJITModule::createJITFunction(
    const JITFunctionDescriptor& descriptor) {
  auto func_signature = getFunctionSignature(descriptor, *context_);
  llvm::Function* func = llvm::Function::Create(func_signature,
                                                llvm::GlobalValue::ExternalLinkage,
                                                descriptor.function_name,
                                                *module_);
  func->setCallingConv(llvm::CallingConv::C);

  auto arg_iter = func->arg_begin();
  for (size_t index = 0; index < descriptor.params_type.size(); ++index, ++arg_iter) {
    if (auto& name = descriptor.params_type[index].name; !name.empty()) {
      arg_iter->setName(name);
    }
  }

  // TODO (bigPYJ1151): Set Parameters Attributes.
  return std::make_shared<LLVMJITFunction>(descriptor, *this, *func);
}

void LLVMJITModule::finish() {
  // TODO (bigPYJ1151): Refactor Debug information.
  llvm::outs() << *module_;

  LLVMJITEngineBuilder builder(*this);
  engine_ = builder.build();
}

void* LLVMJITModule::getFunctionPtrImpl(LLVMJITFunction& function) {
  if (engine_) {
    auto descriptor = function.getFunctionDescriptor();
    return engine_->engine->getPointerToNamedFunction(descriptor->function_name);
  }
  return nullptr;
}

void LLVMJITModule::copyRuntimeModule() {
  module_ = llvm::CloneModule(*runtime_module_, vmap_, [](const llvm::GlobalValue* gv) {
    auto func = llvm::dyn_cast<llvm::Function>(gv);
    if (!func) {
      return true;
    };
    return false;
  });
}
};  // namespace cider::jitlib