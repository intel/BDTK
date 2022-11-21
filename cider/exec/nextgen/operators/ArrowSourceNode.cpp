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
#include "exec/nextgen/operators/ArrowSourceNode.h"
#include "exec/module/batch/ArrowABI.h"

#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::operators {
TranslatorPtr SourceNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<SourceTranslator>(shared_from_this(), succ);
}

void ArrowSourceTranslator::consume(Context& context) {
  codegen(context);
}

void SourceTranslator::codegen(Context& context) {
  auto func = context.query_func_;
  auto&& [output_type, exprs] = op_node_->getOutputExprs();
  // get ArrowArray pointer
  auto arrow_pointer = func->getArgument(0);
  for (int index = 0; index < exprs.size(); ++index) {
    auto jit_index = func->createConstant(JITTypeTag::INT64, (int64_t)index);
    // extract ArrowArray null buffer
    auto null_data = func->emitRuntimeFunctionCall(
        "extract_arrow_array_null",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .ret_sub_type = JITTypeTag::VOID,
            .params_vector = {{arrow_pointer.get(), jit_index.get()}}});

    // extract ArrowArray data buffer
    auto data = func->emitRuntimeFunctionCall(
        "extract_arrow_array_data",
        JITFunctionEmitDescriptor{
            .ret_type = JITTypeTag::POINTER,
            .ret_sub_type = JITTypeTag::VOID,
            .params_vector = {{arrow_pointer.get(), jit_index.get()}}});

    exprs[index]->set_null_datas(null_data);
    exprs[index]->set_datas(data);
  }
  successor_->consume(context);
}

}  // namespace cider::exec::nextgen::operators