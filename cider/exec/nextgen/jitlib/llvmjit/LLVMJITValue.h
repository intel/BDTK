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

#ifndef JITLIB_LLVMJIT_LLVMJITVALUE_H
#define JITLIB_LLVMJIT_LLVMJITVALUE_H

#include "exec/nextgen/jitlib/base/JITValues.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITFunction.h"
#include "exec/nextgen/jitlib/llvmjit/LLVMJITUtils.h"

namespace jitlib {

class LLVMJITValue final : public JITValue {
  friend LLVMJITFunction;

 public:
  explicit LLVMJITValue(JITTypeTag type_tag,
                        LLVMJITFunction& function,
                        llvm::Value* value,
                        const std::string& name = "value",
                        JITBackendTag backend = LLVMJIT,
                        bool is_variable = false)
      : JITValue(type_tag, name, backend)
      , parent_function_(function)
      , llvm_value_(value)
      , is_variable_(is_variable) {}

  JITValue& assign(JITValue& value) override {
    if (LLVMJIT == value.getValueBackendTag()) {
      store(static_cast<LLVMJITValue&>(value));
    }
    return *this;
  }

 private:
  static llvm::IRBuilder<>& getFunctionBuilder(const LLVMJITFunction& function) {
    return static_cast<llvm::IRBuilder<>&>(function);
  }

  llvm::Value* load() {
    if (is_variable_) {
      return getFunctionBuilder(parent_function_).CreateLoad(llvm_value_, false);
    } else {
      return llvm_value_;
    }
  }

  llvm::Value* store(LLVMJITValue& rh) {
    if (is_variable_) {
      return getFunctionBuilder(parent_function_)
          .CreateStore(rh.load(), llvm_value_, false);
    }
    return nullptr;
  }

  LLVMJITFunction& parent_function_;
  llvm::Value* llvm_value_;
  bool is_variable_;
};
};  // namespace jitlib

#endif  // JITLIB_LLVMJIT_LLVMJITVALUE_H
