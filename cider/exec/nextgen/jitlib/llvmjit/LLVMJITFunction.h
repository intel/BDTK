#ifndef LLVM_JIT_FUNCTION_H
#define LLVM_JIT_FUNCTION_H

#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>

#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/base/Values.h"

namespace jitlib {
class LLVMJITModule;

class LLVMJITFunction final : public JITFunction<LLVMJITFunction> {
 public:
  template <TypeTag Type, typename FunctionImpl>
  friend class Value;

  template <TypeTag Type, typename FunctionImpl>
  friend class Ret;

  template <typename JITFunctionImpl>
  friend class JITFunction;

 public:
  explicit LLVMJITFunction(const JITFunctionDescriptor& descriptor,
                           LLVMJITModule& module,
                           llvm::Function& func);

 protected:
  void finishImpl();
  llvm::IRBuilder<>* getIRBuilder() { return ir_builder_.get(); }
  llvm::LLVMContext& getContext();

 private:
  LLVMJITModule& module_;
  llvm::Function& func_;
  std::unique_ptr<llvm::IRBuilder<>> ir_builder_;
};
};  // namespace jitlib

#endif