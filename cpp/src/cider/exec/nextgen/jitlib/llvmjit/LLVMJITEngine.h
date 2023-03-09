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
#ifndef JITLIB_LLVMJIT_LLVMJITENGINE_H
#define JITLIB_LLVMJIT_LLVMJITENGINE_H

#include <llvm/ExecutionEngine/ExecutionEngine.h>

namespace cider::jitlib {
class LLVMJITModule;

// compilation config info
struct CompilationOptions {
  bool optimize_ir = true;
  bool aggressive_jit_compile = true;
  bool dump_ir = false;
  bool enable_vectorize = true;
  bool enable_avx2 = true;
  bool enable_avx512 = false;
};

struct LLVMJITEngine {
  llvm::ExecutionEngine* engine{nullptr};
  llvm::JITEventListener* perf_listener{nullptr};
  llvm::JITEventListener* intel_listener{nullptr};

  ~LLVMJITEngine();
};

class LLVMJITEngineBuilder {
 public:
  explicit LLVMJITEngineBuilder(LLVMJITModule& module, llvm::TargetMachine* tm);

  std::unique_ptr<LLVMJITEngine> build();

 private:
  void dumpASM(LLVMJITEngine& engine);

  LLVMJITModule& module_;
  llvm::Module* llvm_module_;
  std::unique_ptr<llvm::TargetMachine> tm_;
};
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITENGINE_H
