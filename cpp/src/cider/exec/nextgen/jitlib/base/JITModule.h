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
#ifndef JITLIB_BASE_JITMODULE_H
#define JITLIB_BASE_JITMODULE_H

#include "exec/nextgen/jitlib/base/JITFunction.h"

namespace cider::jitlib {
class JITFunctionBuilder;
class JITModule {
  friend JITFunctionBuilder;

 public:
  virtual void finish(const std::string& main_func = "") = 0;

 protected:
  virtual JITFunctionPointer createJITFunction(
      const JITFunctionDescriptor& descriptor) = 0;
};

using JITModulePointer = std::shared_ptr<JITModule>;

class JITFunctionBuilder {
 public:
  JITFunctionBuilder() : module_(nullptr) {}

  JITFunctionBuilder& setFuncName(const std::string& name) {
    name_ = name;
    return *this;
  }

  JITFunctionBuilder& registerModule(JITModule& module) {
    module_ = &module;
    return *this;
  }

  JITFunctionBuilder& addParameter(JITTypeTag type,
                                   const std::string& name = "",
                                   JITTypeTag sub_type = JITTypeTag::INVALID) {
    parameter_list_.push_back(JITFunctionParam{name, type, sub_type});
    return *this;
  }

  JITFunctionBuilder& addReturn(JITTypeTag type,
                                const std::string& name = "",
                                JITTypeTag sub_type = JITTypeTag::INVALID) {
    return_ = JITFunctionParam{name, type, sub_type};
    return *this;
  }

  template <typename FunctionType>
  JITFunctionBuilder& addProcedureBuilder(FunctionType builder) {
    builder_ = builder;
    return *this;
  }

  JITFunctionPointer build() {
    if (!module_) {
      LOG(FATAL) << "Create JITFunction ERROR: lack of module.";
    }
    if (name_ == "") {
      LOG(FATAL) << "Create JITFunction ERROR: lack of function name";
    }
    if (return_.type == JITTypeTag::INVALID) {
      LOG(FATAL) << "Create JITFunction ERROR: return type invalid";
    }
    if (!builder_) {
      LOG(FATAL) << "Create JITFunction ERROR: lack of procedure builder.";
    }

    JITFunctionPointer function = module_->createJITFunction(JITFunctionDescriptor{
        .function_name = name_, .ret_type = return_, .params_type = parameter_list_});
    builder_(function);
    function->finish();
    return function;
  }

 private:
  std::string name_;
  JITModule* module_;
  static constexpr size_t DefaultParamsNum = 8;
  boost::container::small_vector<JITFunctionParam, DefaultParamsNum> parameter_list_;
  JITFunctionParam return_;
  std::function<void(const JITFunctionPointer&)> builder_;
};

};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITMODULE_H
