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

#include <llvm/Analysis/CGSCCPassManager.h>
#include <llvm/Analysis/TargetLibraryInfo.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/PassManager.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/IPO/GlobalOpt.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar/LoopPassManager.h>
#include <llvm/Transforms/Scalar/LoopRotation.h>
#include <llvm/Transforms/Scalar/SROA.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Utils.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <llvm/Transforms/Utils/LoopSimplify.h>
#include <llvm/Transforms/Utils/Mem2Reg.h>
#include <llvm/Transforms/Vectorize/LoopVectorize.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITTargets.h"
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
    CHECK(std::filesystem::exists(template_path));

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

  owned_functions_.emplace_back(func);

  // TODO (bigPYJ1151): Set Parameters Attributes.
  return std::make_shared<LLVMJITFunction>(descriptor, *this, *func);
}

void LLVMJITModule::finish(const std::string& main_func) {
  llvm::TargetMachine* tm = buildTargetMachine(co_);
  module_->setTargetTriple(tm->getTargetTriple().str());
  module_->setDataLayout(tm->createDataLayout());

  llvm::Function* main_func_ptr = module_->getFunction(main_func);
  if (main_func_ptr) {
    main_func_ptr->addFnAttr("target-features", tm->getTargetFeatureString());
    main_func_ptr->addFnAttr("target-cpu", tm->getTargetCPU());
  } else {
    for (auto& func : module_->getFunctionList()) {
      func.addFnAttr("target-features", tm->getTargetFeatureString());
      func.addFnAttr("target-cpu", tm->getTargetCPU());
    }
  }

  if (co_.dump_ir) {
    dumpModuleIR(module_.get(), module_->getModuleIdentifier());
  }

  // IR optimization
  optimizeIR(tm);

  LLVMJITEngineBuilder builder(*this, tm);

  if (co_.dump_ir) {
    dumpModuleIR(module_.get(), module_->getModuleIdentifier() + "_opt");
  }

  engine_ = builder.build();
}

void LLVMJITModule::optimizeIR(llvm::TargetMachine* tm) {
  if (co_.optimize_ir) {
    llvm::TargetLibraryInfoImpl target_info(llvm::Triple(module_->getTargetTriple()));

    llvm::ModuleAnalysisManager module_analysis_mgr;
    llvm::LoopAnalysisManager loop_analysis_mgr;
    llvm::FunctionAnalysisManager function_analysis_mgr;
    llvm::CGSCCAnalysisManager cgscc_analysis_mgr;

    function_analysis_mgr.registerPass([tm]() { return tm->getTargetIRAnalysis(); });
    function_analysis_mgr.registerPass(
        [&target_info]() { return llvm::TargetLibraryAnalysis(target_info); });

    llvm::AAManager aam;
    aam.registerFunctionAnalysis<llvm::BasicAA>();
    function_analysis_mgr.registerPass([&aam]() { return std::move(aam); });

    llvm::PassBuilder pass_builder;
    pass_builder.registerModuleAnalyses(module_analysis_mgr);
    pass_builder.registerFunctionAnalyses(function_analysis_mgr);
    pass_builder.registerLoopAnalyses(loop_analysis_mgr);
    pass_builder.registerCGSCCAnalyses(cgscc_analysis_mgr);
    pass_builder.crossRegisterProxies(loop_analysis_mgr,
                                      function_analysis_mgr,
                                      cgscc_analysis_mgr,
                                      module_analysis_mgr);

    llvm::ModulePassManager module_pass_mgr;

    module_pass_mgr.addPass(
        llvm::AlwaysInlinerPass(false));  // Inline all functions labeled as always_inline

    llvm::FunctionPassManager function_pass_mgr1;
    function_pass_mgr1.addPass(llvm::SimplifyCFGPass());
    function_pass_mgr1.addPass(llvm::SROA());
    function_pass_mgr1.addPass(llvm::InstCombinePass());

    llvm::LoopPassManager loop_pass_mgr;
    loop_pass_mgr.addPass(llvm::LoopRotatePass());

    function_pass_mgr1.addPass(llvm::FunctionToLoopPassAdaptor(std::move(loop_pass_mgr)));

    function_pass_mgr1.addPass(llvm::SimplifyCFGPass());
    function_pass_mgr1.addPass(llvm::LoopSimplifyPass());

    if (co_.enable_vectorize) {
      function_pass_mgr1.addPass(llvm::LoopVectorizePass());
    }

    module_pass_mgr.addPass(
        llvm::ModuleToFunctionPassAdaptor(std::move(function_pass_mgr1)));

    module_pass_mgr.addPass(llvm::GlobalOptPass());

    module_pass_mgr.run(*module_, module_analysis_mgr);
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
