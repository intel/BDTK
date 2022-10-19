#ifndef LLVM_JIT_UTILS_H
#define LLVM_JIT_UTILS_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"

#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Value.h>
#include <cstdint>

namespace jitlib {
inline llvm::Type* getLLVMType(TypeTag tag, llvm::LLVMContext& ctx) {
  switch (tag) {
    case VOID:
      return llvm::Type::getVoidTy(ctx);
    case INT8:
      return llvm::Type::getInt8Ty(ctx);
    case INT16:
      return llvm::Type::getInt16Ty(ctx);
    case INT32:
      return llvm::Type::getInt32Ty(ctx);
    case INT64:
      return llvm::Type::getInt64Ty(ctx);
    default:
      return nullptr;
  }
}

inline llvm::Value* getLLVMConstant(uint64_t value, TypeTag tag, llvm::LLVMContext& ctx) {
  llvm::Type* type = getLLVMType(tag, ctx);
  switch (tag) {
    case INT8:
    case INT16:
    case INT32:
    case INT64:
      return llvm::ConstantInt::get(type, value, true);
    default:
      return nullptr;
  }
}

};  // namespace jitlib

#endif
