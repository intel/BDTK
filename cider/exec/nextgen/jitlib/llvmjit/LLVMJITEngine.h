#ifndef JITLIB_LLVMJIT_LLVMJITENGINE_H
#define JITLIB_LLVMJIT_LLVMJITENGINE_H

#include <llvm/ExecutionEngine/ExecutionEngine.h>

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

#endif  // JITLIB_LLVMJIT_LLVMJITENGINE_H
