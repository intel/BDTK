/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/ExecutionEngine/JITEventListener.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"

namespace cider::jitlib {
LLVMJITEngine::~LLVMJITEngine() {
  LLVMDisposeExecutionEngine(llvm::wrap(engine));
}

LLVMJITEngineBuilder::LLVMJITEngineBuilder(LLVMJITModule& module, llvm::TargetMachine* tm)
    : module_(module), llvm_module_(module.module_.get()), tm_(tm) {}

void LLVMJITEngineBuilder::dumpASM(LLVMJITEngine& engine) {
  const std::string fname = llvm_module_->getModuleIdentifier() + ".s";

  std::error_code error_code;
  llvm::raw_fd_ostream file(fname, error_code, llvm::sys::fs::F_None);
  if (error_code) {
    LOG(ERROR) << "Could not open file to dump Module ASM: " << fname;
  } else {
    llvm::legacy::PassManager pass_mgr;
    llvm::TargetMachine* tm = engine.engine->getTargetMachine();

    tm->Options.MCOptions.AsmVerbose = true;
    pass_mgr.add(llvm::createTargetTransformInfoWrapperPass(tm->getTargetIRAnalysis()));
    tm->addPassesToEmitFile(
        pass_mgr, file, nullptr, llvm::TargetMachine::CGFT_AssemblyFile);

    pass_mgr.run(*llvm_module_);
    tm->Options.MCOptions.AsmVerbose = false;
  }
}

std::unique_ptr<LLVMJITEngine> LLVMJITEngineBuilder::build() {
  std::string error;
  llvm::EngineBuilder eb(std::move(module_.module_));

  eb.setMCPU(llvm::sys::getHostCPUName().str())
      .setEngineKind(llvm::EngineKind::JIT)
      .setErrorStr(&error);

  auto engine = std::make_unique<LLVMJITEngine>();
  engine->engine = eb.create(tm_.release());
  engine->engine->DisableLazyCompilation(false);
  engine->engine->setVerifyModules(false);

  engine->engine->RegisterJITEventListener(
      llvm::JITEventListener::createPerfJITEventListener());
  engine->engine->RegisterJITEventListener(
      llvm::JITEventListener::createIntelJITEventListener());

  DLOG(INFO) << "Enabled features: "
             << engine->engine->getTargetMachine()->getTargetFeatureString().str();

  engine->engine->finalizeObject();

  if (module_.getCompilationOptions().dump_ir) {
    dumpASM(*engine);
  }

  return engine;
}
};  // namespace cider::jitlib
