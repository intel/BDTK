#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_os_ostream.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"
#include "util/Logger.h"

namespace jitlib {
LLVMJITFunction::LLVMJITFunction(const JITFunctionDescriptor& descriptor,
                                 LLVMJITModule& module,
                                 llvm::Function& func)
    : JITFunction<LLVMJITFunction>(descriptor)
    , module_(module)
    , func_(func)
    , ir_builder_(nullptr) {
  auto local_variable_block =
      llvm::BasicBlock::Create(getContext(), ".Local_Vars", &func_);
  auto entry_block = llvm::BasicBlock::Create(getContext(), ".Entry", &func_);

  ir_builder_ = std::make_unique<llvm::IRBuilder<>>(local_variable_block);
  ir_builder_->CreateBr(entry_block);

  ir_builder_->SetInsertPoint(entry_block);
}

llvm::LLVMContext& LLVMJITFunction::getContext() {
  return module_.getContext();
}

void LLVMJITFunction::finishImpl() {
  std::stringstream error_msg;
  llvm::raw_os_ostream error_os(error_msg);
  if (llvm::verifyFunction(func_, &error_os)) {
    error_os << "\n-----\n";
    func_.print(error_os);
    error_os << "\n-----\n";
    LOG(FATAL) << error_msg.str();
  }
}
};  // namespace jitlib