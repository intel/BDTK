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
#ifndef JITLIB_BASE_JITFUNCTION_H
#define JITLIB_BASE_JITFUNCTION_H

#include <any>
#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/JITControlFlow.h"
#include "exec/nextgen/jitlib/base/JITValue.h"

namespace cider::jitlib {
enum JITFunctionParamAttr : uint64_t {};

struct JITFunctionParam {
  std::string name{""};
  JITTypeTag type;
  JITTypeTag sub_type{JITTypeTag::INVALID};
  uint64_t attribute;
};

struct JITFunctionDescriptor {
  static constexpr size_t DefaultParamsNum = 8;
  std::string function_name;
  JITFunctionParam ret_type;
  boost::container::small_vector<JITFunctionParam, DefaultParamsNum> params_type;
};

struct JITFunctionEmitDescriptor {
  static constexpr size_t DefaultParamsNum = 8;
  JITTypeTag ret_type;
  JITTypeTag ret_sub_type = JITTypeTag::INVALID;
  boost::container::small_vector<JITValue*, DefaultParamsNum> params_vector;
};

class JITFunction {
 public:
  JITFunction(const JITFunctionDescriptor& descriptor) : descriptor_(descriptor) {}

  const JITFunctionDescriptor* getFunctionDescriptor() const { return &descriptor_; }

  template <typename R, typename... Args>
  auto getFunctionPointer() {
    if constexpr (sizeof...(Args) > 0) {
      using func_type = R (*)(Args...);
      return reinterpret_cast<func_type>(getFunctionPointer());
    } else {
      using func_type = R (*)();
      return reinterpret_cast<func_type>(getFunctionPointer());
    }
  }

  template <typename T = int32_t>
  JITValuePointer createVariable(JITTypeTag type_tag,
                                 const std::string& name = "var",
                                 T&& init_val = 0) {
    if constexpr (std::is_same_v<std::decay_t<T>, JITValuePointer> ||
                  std::is_same_v<std::decay_t<T>, JITValue>) {
      return createVariableImpl(type_tag, name, init_val);
    } else {
      auto init_jit_value = createLiteral(type_tag, init_val);
      return createVariableImpl(type_tag, name, init_jit_value);
    }
  }

  [[deprecated("Use createLiteral.")]] JITValuePointer createConstant(
      JITTypeTag type_tag,
      const std::any& value) {
    return createLiteralImpl(type_tag, value);
  }

  template <typename T>
  JITValuePointer createLiteral(JITTypeTag type_tag, T value) {
    return createLiteralImpl(type_tag, castLiteral(type_tag, value));
  }

  using LocalJITValueBuilderEmitter = JITValuePointer(void*);

  template <
      typename T,
      typename std::enable_if_t<std::is_invocable_r_v<JITValuePointer, T>, bool> = true>
  JITValuePointer createLocalJITValue(T&& builder) {
    auto builder_wrapper = [](void* builder_ptr) -> JITValuePointer {
      auto actual_builder = reinterpret_cast<T*>(builder_ptr);
      return (*actual_builder)();
    };

    return createLocalJITValueImpl(builder_wrapper, (void*)&builder);
  }

  virtual JITValuePointer getArgument(size_t index) = 0;

  virtual IfBuilderPointer createIfBuilder() = 0;

  virtual LoopBuilderPointer createLoopBuilder() = 0;

  virtual void createReturn() = 0;

  virtual void createReturn(JITValue& value) = 0;

  virtual JITValuePointer emitJITFunctionCall(
      JITFunction& function,
      const JITFunctionEmitDescriptor& descriptor) = 0;

  virtual JITValuePointer emitRuntimeFunctionCall(
      const std::string& fname,
      const JITFunctionEmitDescriptor& descriptor) = 0;

  virtual void finish() = 0;

 protected:
  JITFunctionDescriptor descriptor_;

 private:
  virtual void* getFunctionPointer() = 0;

  virtual JITValuePointer createLiteralImpl(JITTypeTag type_tag,
                                            const std::any& value) = 0;

  virtual JITValuePointer createLocalJITValueImpl(LocalJITValueBuilderEmitter emitter,
                                                  void* builder) = 0;

  virtual JITValuePointer createVariableImpl(JITTypeTag type_tag,
                                             const std::string& name,
                                             JITValuePointer& init_val) = 0;
};

using JITFunctionPointer = std::shared_ptr<JITFunction>;
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITFUNCTION_H
