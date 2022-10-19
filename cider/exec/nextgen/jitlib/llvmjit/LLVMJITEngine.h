#ifndef LLVM_JIT_ENGINE_H
#define LLVM_JIT_ENGINE_H

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/LegacyPassManager.h>

namespace jitlib {
class LLVMJITModule;

struct LLVMJITEngine {
  llvm::ExecutionEngine* engine{nullptr};

  ~LLVMJITEngine();
};

class LLVMJITEngineBuilder {
 public:
  explicit LLVMJITEngineBuilder(LLVMJITModule& module);

  std::unique_ptr<LLVMJITEngine> build();

 private:
  static void initializationTargets();

  LLVMJITModule& module_;
};
};  // namespace jitlib

#endif