/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

namespace jitlib {
inline llvm::Type* getLLVMType(JITTypeTag tag, llvm::LLVMContext& ctx) {
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

inline llvm::Value* getLLVMConstant(uint64_t value,
                                    JITTypeTag tag,
                                    llvm::LLVMContext& ctx) {
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

#endif  // JITLIB_LLVMJIT_LLVMJITUTILS_H
