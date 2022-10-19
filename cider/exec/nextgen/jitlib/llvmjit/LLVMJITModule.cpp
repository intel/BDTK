
#include "exec/nextgen/jitlib/llvmjit/LLVMJITModule.h"

#include <llvm/IR/CallingConv.h>
#include <llvm/IR/Function.h>

#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"
#include "util/Logger.h"

namespace jitlib {

LLVMJITModule::LLVMJITModule(const std::string& name)
    : context_(std::make_unique<llvm::LLVMContext>())
    , module_(std::make_unique<llvm::Module>(name, *context_))
    , engine_(nullptr) {
  CHECK(context_);
  CHECK(module_);
}

static llvm::FunctionType* getFunctionSignature(const JITFunctionDescriptor& descriptor,
                                                llvm::LLVMContext& context) {
  llvm::Type* ret_type = getLLVMType(descriptor.ret_type.type, context);

  llvm::SmallVector<llvm::Type*, 8> arguments;
  for (const JITFunctionParam& param_descriptor : descriptor.params_type) {
    llvm::Type* arg_type = getLLVMType(param_descriptor.type, context);
    if (arg_type) {
      arguments.push_back(arg_type);
    } else {
      // TODO (bigPYJ1151): Add Exceptions.
    }
  }

  return llvm::FunctionType::get(ret_type, arguments, false);
}

LLVMJITFunction LLVMJITModule::createJITFunctionImpl(
    const JITFunctionDescriptor& descriptor) {
  auto func_signature = getFunctionSignature(descriptor, *context_);
  llvm::Function* func = llvm::Function::Create(func_signature,
                                                llvm::GlobalValue::ExternalLinkage,
                                                descriptor.function_name,
                                                *module_);
  func->setCallingConv(llvm::CallingConv::C);

  auto arg_iter = func->arg_begin();
  for (size_t index = 0; index < descriptor.params_type.size(); ++index, ++arg_iter) {
    if (auto name = descriptor.params_type[index].name; name) {
      arg_iter->setName(name);
    }
  }

  // TODO (bigPYJ1151): Set Parameters Attributes.
  return LLVMJITFunction(descriptor, *this, *func);
}

void LLVMJITModule::finishImpl() {
  LLVMJITEngineBuilder builder(*this);
  engine_ = builder.build();
}

void* LLVMJITModule::getFunctionPtrImpl(LLVMJITFunction& function) {
  if (engine_) {
    auto descriptor = function.getFunctionDescriptor();
    return engine_->engine->getPointerToNamedFunction(descriptor->function_name);
    
  }
  return nullptr;
}

};  // namespace jitlib