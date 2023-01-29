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
#ifndef JITLIB_LLVMJIT_LLVMJITUTILS_H
#define JITLIB_LLVMJIT_LLVMJITUTILS_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"

#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>

namespace cider::jitlib {
inline llvm::Type* getLLVMType(JITTypeTag tag, llvm::LLVMContext& ctx) {
  switch (tag) {
    case JITTypeTag::VOID:
      return llvm::Type::getVoidTy(ctx);
    case JITTypeTag::BOOL:
      return llvm::Type::getInt1Ty(ctx);
    case JITTypeTag::INT8:
      return llvm::Type::getInt8Ty(ctx);
    case JITTypeTag::INT16:
      return llvm::Type::getInt16Ty(ctx);
    case JITTypeTag::INT32:
      return llvm::Type::getInt32Ty(ctx);
    case JITTypeTag::INT64:
      return llvm::Type::getInt64Ty(ctx);
    case JITTypeTag::FLOAT:
      return llvm::Type::getFloatTy(ctx);
    case JITTypeTag::DOUBLE:
      return llvm::Type::getDoubleTy(ctx);
    default:
      return nullptr;
  }
}

inline llvm::PointerType* getLLVMPtrType(JITTypeTag sub_tag, llvm::LLVMContext& ctx) {
  switch (sub_tag) {
    case JITTypeTag::BOOL:
      return llvm::Type::getInt1PtrTy(ctx);
    case JITTypeTag::INT8:
      return llvm::Type::getInt8PtrTy(ctx);
    case JITTypeTag::INT16:
      return llvm::Type::getInt16PtrTy(ctx);
    case JITTypeTag::INT32:
      return llvm::Type::getInt32PtrTy(ctx);
    case JITTypeTag::INT64:
      return llvm::Type::getInt64PtrTy(ctx);
    case JITTypeTag::FLOAT:
      return llvm::Type::getFloatPtrTy(ctx);
    case JITTypeTag::DOUBLE:
      return llvm::Type::getDoublePtrTy(ctx);
    default:
      return nullptr;
  }
}

inline llvm::Value* getLLVMConstantInt(uint64_t value,
                                       JITTypeTag tag,
                                       llvm::LLVMContext& ctx) {
  llvm::Type* type = getLLVMType(tag, ctx);
  switch (tag) {
    case JITTypeTag::BOOL:
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      return llvm::ConstantInt::get(type, value, true);
    default:
      return nullptr;
  }
}

inline llvm::Value* getLLVMConstantFP(double value,
                                      JITTypeTag tag,
                                      llvm::LLVMContext& ctx) {
  llvm::Type* type = getLLVMType(tag, ctx);
  switch (tag) {
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      return llvm::ConstantFP::get(type, value);
    default:
      return nullptr;
  }
}

inline llvm::Value* getLLVMConstantGlobalStr(const std::string& value,
                                             llvm::IRBuilder<>* builder,
                                             llvm::LLVMContext& ctx) {
  llvm::Value* str_lv = builder->CreateGlobalString(
      value, "str_const_" + std::to_string(std::hash<std::string>()(value)));
  str_lv = builder->CreateBitCast(str_lv, getLLVMPtrType(JITTypeTag::INT8, ctx));
  return str_lv;
}
};  // namespace cider::jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITUTILS_H
