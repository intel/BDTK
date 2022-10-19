#ifndef LLVM_JIT_MODULE_H
#define LLVM_JIT_MODULE_H

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/JITModule.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITEngine.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

namespace jitlib {
class LLVMJITModule final : public JITModule<LLVMJITModule, LLVMJITFunction> {
 public:
  friend LLVMJITEngineBuilder;
  friend LLVMJITFunction;
  template <typename JITModuleImpl, typename JITFunctionImpl>
  friend class JITModule;

 public:
  explicit LLVMJITModule(const std::string& name);

 protected:
  LLVMJITFunction createJITFunctionImpl(const JITFunctionDescriptor& descriptor);
  void* getFunctionPtrImpl(LLVMJITFunction& function);

  void finishImpl();

 private:
  llvm::LLVMContext& getContext() { return *context_; }

  std::unique_ptr<llvm::LLVMContext> context_;
  std::unique_ptr<llvm::Module> module_;
  std::unique_ptr<LLVMJITEngine> engine_;
};
};  // namespace jitlib

#endif