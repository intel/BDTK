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
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"
#include "util/Logger.h"
#include "util/filesystem/cider_path.h"

namespace cider::jitlib {

static void dumpModuleIR(llvm::Module* module, const std::string& module_name) {
  const std::string fname = module_name + ".ll";

  std::error_code error_code;
  llvm::raw_fd_ostream file(fname, error_code, llvm::sys::fs::F_None);
  if (error_code) {
    LOG(ERROR) << "Could not open file to dump Module IR: " << fname;
  } else {
    llvm::legacy::PassManager pass_mgr;
    pass_mgr.add(llvm::createStripDeadPrototypesPass());
    pass_mgr.run(*module);

    file << *module;
  }
}

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

LLVMJITModule::LLVMJITModule(const std::string& name,
                             bool copy_runtime_module,
                             const CompilationOptions& co)
    : context_(std::make_unique<llvm::LLVMContext>()), engine_(nullptr), co_(co) {
  if (copy_runtime_module) {
    auto expected_res =
        llvm::parseBitcodeFile(getRuntimeBuffer()->getMemBufferRef(), *context_);
    if (!expected_res) {
      LOG(FATAL) << "LLVM IR ParseError: Something wrong when parsing bitcode.";
    } else {
      runtime_module_ = std::move(expected_res.get());
    }
    copyRuntimeModule();
    module_->setModuleIdentifier(name);
  } else {
    module_ = std::make_unique<llvm::Module>(name, *context_);
  }
  CHECK(module_);
}

static llvm::FunctionType* getFunctionSignature(const JITFunctionDescriptor& descriptor,
                                                llvm::LLVMContext& context) {
  auto get_llvm_type = [&context](const JITFunctionParam& param) -> llvm::Type* {
    return JITTypeTag::POINTER == param.type ? getLLVMPtrType(param.sub_type, context)
                                             : getLLVMType(param.type, context);
  };
  llvm::Type* ret_type = get_llvm_type(descriptor.ret_type);

  llvm::SmallVector<llvm::Type*, JITFunctionDescriptor::DefaultParamsNum> arguments;
  arguments.reserve(descriptor.params_type.size());

  for (const JITFunctionParam& param_descriptor : descriptor.params_type) {
    llvm::Type* arg_type = get_llvm_type(param_descriptor);
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
  if (co_.dump_ir) {
    dumpModuleIR(module_.get(), module_->getModuleIdentifier());
  }

  LLVMJITEngineBuilder builder(*this);

  // IR optimization
  optimizeIR(module_.get());
  if (co_.dump_ir) {
    dumpModuleIR(module_.get(), module_->getModuleIdentifier() + "_opt");
  }

  engine_ = builder.build();
}

void LLVMJITModule::optimizeIR(llvm::Module* module) {
  llvm::legacy::PassManager pass_manager;
  switch (co_.optimize_level) {
    case LLVMJITOptimizeLevel::RELEASE:
      // the always inliner legacy pass must always run first
      pass_manager.add(llvm::createAlwaysInlinerLegacyPass());

      pass_manager.add(llvm::createSROAPass());
      // mem ssa drops unused load and store instructions, e.g. passing variables directly
      // where possible
      pass_manager.add(llvm::createEarlyCSEPass(
          /*enable_mem_ssa=*/true));  // Catch trivial redundancies

      pass_manager.add(llvm::createJumpThreadingPass());  // Thread jumps.
      pass_manager.add(llvm::createCFGSimplificationPass());

      // remove load/stores in PHIs if instructions can be accessed directly post thread
      // jumps
      pass_manager.add(llvm::createNewGVNPass());

      pass_manager.add(llvm::createDeadStoreEliminationPass());
      pass_manager.add(llvm::createLICMPass());

      pass_manager.add(llvm::createInstructionCombiningPass());

      // module passes
      pass_manager.add(llvm::createPromoteMemoryToRegisterPass());
      pass_manager.add(llvm::createGlobalOptimizerPass());

      pass_manager.add(llvm::createCFGSimplificationPass());  // cleanup after everything

      pass_manager.run(*module);
      break;
    // TBD other optimize level to be added
    case LLVMJITOptimizeLevel::DEBUG:
      // DEBUG : default optimize level, will not do any optimization
      break;
    default:
      LOG(FATAL) << "Invalid optimize level.";
      break;
  }
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
    }
    return false;
  });
}
};  // namespace cider::jitlib
