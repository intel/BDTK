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
#include "exec/nextgen/context/CodegenContext.h"

#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/JITLib.h"

namespace cider::exec::nextgen::context {
using namespace cider::jitlib;

JITValuePointer CodegenContext::registerBatch(const SQLTypeInfo& type,
                                              const std::string& name,
                                              bool arrow_array_output) {
  int64_t id = acquireContextID();
  JITValuePointer ret = jit_func_->createLocalJITValue([this, id, arrow_array_output]() {
    auto index = this->jit_func_->createConstant(JITTypeTag::INT64, id);
    auto pointer = this->jit_func_->emitRuntimeFunctionCall(
        "get_query_context_ptr",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .ret_sub_type = JITTypeTag::INT8,
            .params_vector = {this->jit_func_->getArgument(0).get(), index.get()}});
    if (arrow_array_output) {
      return this->jit_func_->emitRuntimeFunctionCall(
          "get_arrow_array_ptr",
          JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                    .ret_sub_type = JITTypeTag::INT8,
                                    .params_vector = {pointer.get()}});
    }
    return pointer;
  });
  ret->setName(name);

  batch_descriptors_.emplace_back(std::make_shared<BatchDescriptor>(id, name, type), ret);

  return ret;
}

RuntimeCtxPtr CodegenContext::generateRuntimeCTX(
    const CiderAllocatorPtr& allocator) const {
  auto runtime_ctx = std::make_unique<RuntimeContext>(getNextContextID());

  for (auto& batch_desc : batch_descriptors_) {
    runtime_ctx->addBatch(batch_desc.first);
  }

  runtime_ctx->instantiate(allocator);
  return runtime_ctx;
}
}  // namespace cider::exec::nextgen::context