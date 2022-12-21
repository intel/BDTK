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
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/CodeGen.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"

namespace cider::jitlib {
LLVMJITEngine::~LLVMJITEngine() {
  LLVMDisposeExecutionEngine(llvm::wrap(engine));
}

namespace {
static const char* avx256_inst_sets[] = {"avx", "avx2"};
static const char* avx512_inst_sets[] = {"avx512ifma",
                                         "avx512bitalg",
                                         "avx512er",
                                         "avx512vnni",
                                         "avx512vpopcntdq",
                                         "avx512f",
                                         "avx512bw",
                                         "avx512vbmi2",
                                         "avx512vl",
                                         "avx512cd",
                                         "avx512vbmi",
                                         "avx512bf16",
                                         "avx512dq",
                                         "avx512pf"};

static const std::string process_triple = llvm::sys::getProcessTriple();  // NOLINT
static const std::string process_name = llvm::sys::getHostCPUName();      // NOLINT
static const llvm::Target* host_target = []() {
  // Initialize LLVM runtime env
  llvm::InitializeNativeTarget();
  llvm::InitializeAllTargetMCs();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  std::string error;
  auto* target = llvm::TargetRegistry::lookupTarget(process_triple, error);
  if (nullptr == target) {
    LOG(FATAL) << "Unable to initialize host target, process triple: " << process_triple
               << ", error: " << error << ".";
  }
  return target;
}();

static llvm::StringMap<bool> host_supported_features = []() {
  llvm::StringMap<bool> features;
  if (!llvm::sys::getHostCPUFeatures(features)) {
    LOG(FATAL) << "Unable to get host supported features.";
  }
  // TBD (bigPYJ1151): whether need to filter unused instruction sets.
  return features;
}();
}  // namespace

LLVMJITEngineBuilder::LLVMJITEngineBuilder(LLVMJITModule& module)
    : module_(module), llvm_module_(module.module_.get()) {}

static llvm::TargetOptions buildTargetOptions() {
  llvm::TargetOptions to;
  to.EnableFastISel = true;
  to.MCOptions.AsmVerbose = false;

  return to;
}

static llvm::SubtargetFeatures buildTargetFeatures(const CompilationOptions& co) {
  llvm::StringMap<bool> features_copy(host_supported_features);
  auto switch_inst_set = [&features_copy](auto&& sets, bool flag) {
    for (auto feature : sets) {
      if (auto iter = features_copy.find(feature); features_copy.end() != iter) {
        iter->second &= flag;
      }
    }
  };

  switch_inst_set(avx256_inst_sets, co.enable_avx2);
  switch_inst_set(avx512_inst_sets, co.enable_avx512);

  llvm::SubtargetFeatures features;
  for (auto&& entry : features_copy) {
    features.AddFeature(entry.getKey(), entry.getValue());
  }
  return features;
}

llvm::TargetMachine* LLVMJITEngineBuilder::buildTargetMachine() {
  return host_target->createTargetMachine(process_triple,
                                          process_name,
                                          buildTargetFeatures(module_.co_).getString(),
                                          buildTargetOptions(),
                                          llvm::None,
                                          llvm::None,
                                          module_.co_.aggressive_jit_compile
                                              ? llvm::CodeGenOpt::Aggressive
                                              : llvm::CodeGenOpt::Default,
                                          true);
}

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
  engine->engine = eb.create(buildTargetMachine());
  engine->engine->DisableLazyCompilation(false);
  engine->engine->setVerifyModules(false);

  LOG(INFO) << "Enabled features: "
            << engine->engine->getTargetMachine()->getTargetFeatureString().str();

  engine->engine->finalizeObject();

  if (module_.getCompilationOptions().dump_ir) {
    dumpASM(*engine);
  }

  return engine;
}
};  // namespace cider::jitlib
