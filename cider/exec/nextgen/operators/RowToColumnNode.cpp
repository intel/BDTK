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

#include "exec/nextgen/operators/RowToColumnNode.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/RuntimeContext.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace cider::exec::nextgen::operators {

TranslatorPtr RowToColumnNode::toTranslator(const TranslatorPtr& succ) {
  return createOpTranslator<RowToColumnTranslator>(shared_from_this());
}

void RowToColumnTranslator::consume(Context& context) {
  codegen(context);
}

void RowToColumnTranslator::codegen(Context& context) {
  auto func = context.query_func_;
  auto&& inputs = node_.getOutputExprs();

  int len = context.expr_outs_.size();
  // construct Batch SQLTypeInfo
  std::vector<SQLTypeInfo> vec;
  for (int i = 0; i < len; ++i) {
    vec.emplace_back(SQLTypeInfo(inputs[i]->get_type_info()));
  }

  for (int64_t idx = 0; idx < len; ++idx) {
    auto output_batch = context.codegen_context_.registerBatch(
        SQLTypeInfo(kSTRUCT, false, vec), "output");

    auto index = func->createVariable(JITTypeTag::INT64, "idx");
    index = func->createConstant(JITTypeTag::INT64, idx);
    auto array_child = func->emitRuntimeFunctionCall(
        "extract_arrow_array_child",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .ret_sub_type = JITTypeTag::INT8,
                                  .params_vector = {output_batch.get(), index.get()}});

    // TODO(qiuyang) : null vector handle have not supported
    // auto null_idx = func->createConstant(JITTypeTag::INT64, 0l);
    // allocate null buffer space
    // func->emitRuntimeFunctionCall(
    //     "allocate_arrow_buffer",
    //     JITFunctionEmitDescriptor{.params_vector = {array_child.get(),
    //     null_idx.get()}});
    // extract null buffer
    // auto null_buffer = func->emitRuntimeFunctionCall(
    //     "extract_arrow_array_buffer",
    //     JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
    //                               .ret_sub_type = JITTypeTag::INT32,
    //                               .params_vector = {array_child.get(),
    //                               null_idx.get()}});

    auto data_idx = func->createConstant(JITTypeTag::INT64, 1l);
    // allocate data buffer space
    func->emitRuntimeFunctionCall(
        "allocate_arrow_buffer",
        JITFunctionEmitDescriptor{.params_vector = {array_child.get(), data_idx.get()}});
    // extract data buffer
    auto array_buffer = func->emitRuntimeFunctionCall(
        "extract_arrow_array_buffer",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::POINTER,
                                  .ret_sub_type = JITTypeTag::INT32,
                                  .params_vector = {array_child.get(), data_idx.get()}});

    // null_buffer[*context.cur_line_idx_] = context.expr_outs_[idx]->getNull();
    array_buffer[*context.cur_line_idx_] = context.expr_outs_[idx]->getValue();
  }
}

}  // namespace cider::exec::nextgen::operators
