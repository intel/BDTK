
#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"

#include <llvm/IR/Value.h>

#include "util/Logger.h"

namespace jitlib {

JITValue& LLVMJITValue::assign(JITValue& value) {
  if (!is_variable_) {
    LOG(ERROR) << "JITValue " << getValueName()
               << "is not a variable in LLVMJITValue::assign.";
  }
  store(static_cast<LLVMJITValue&>(value));
  return *this;
}

JITValuePointer LLVMJITValue::add(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "add");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case INT8:
    case INT16:
    case INT32:
    case INT64:
      ans = getFunctionBuilder(parent_function_)
                .CreateAdd(load(), llvm_rh.load());
      break;
    case FLOAT:
    case DOUBLE:
      ans = getFunctionBuilder(parent_function_)
                .CreateFAdd(load(), llvm_rh.load());
      break;
    default:
      LOG(ERROR) << "Invalid JITValue type for add operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "add", JITBackendTag::LLVMJIT, false);
}

void LLVMJITValue::checkOprandsType(JITTypeTag lh, JITTypeTag rh, const char* op) {
  if (lh != rh) {
    LOG(ERROR) << "Oprands type doesn't match in LLVMJITValue operator " << op
               << " lh=" << getJITTypeName(lh) << ", rh=" << getJITTypeName(rh) << ".";
  }
}

llvm::Value* LLVMJITValue::load() {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_).CreateLoad(llvm_value_, false);
  } else {
    return llvm_value_;
  }
}

llvm::Value* LLVMJITValue::store(LLVMJITValue& rh) {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_)
        .CreateStore(rh.load(), llvm_value_, false);
  }
  return nullptr;
}

};  // namespace jitlib