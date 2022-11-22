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

#include "exec/nextgen/operators/ColumnToRowNode.h"

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/operators/expr.h"

namespace cider::exec::nextgen::operators {
void ColumnToRowTranslator::consume(Context& context) {
  codegen(context);
}

void ColumnToRowTranslator::codegen(Context& context) {
  auto func = context.query_func_;
  auto inputs = node_.getExprs();
  // for row loop
  auto arrow_pointer = func->getArgument(0);
  auto index = func->createVariable(JITTypeTag::INT64, "index");
  index = func->createConstant(JITTypeTag::INT64, 0l);
  auto len = func->createVariable(JITTypeTag::INT64, "len");
  // len means rows length
  len = func->emitRuntimeFunctionCall(
      "extract_arrow_array_len",
      JITFunctionEmitDescriptor{.ret_type = JITTypeTag::INT64,
                                .params_vector = {arrow_pointer.get()}});

  func->createLoopBuilder()
      ->condition([&index, &len]() { return index < len; })
      ->loop([&]() {
        col2RowConvert(inputs, func, index);
        // context record row index
        context.index_ = index.get();
        successor_->consume(context);
      })
      ->update([&index]() { index = index + 1l; })
      ->build();
}

void ColumnToRowTranslator::col2RowConvert(ExprPtrVector inputs,
                                           JITFunction* func,
                                           JITValuePointer index) {
  ExprGenerator gen(func);
  // for column loop
  for (int idx = 0; idx < inputs.size(); ++idx) {
    std::vector<JITValuePointer> vec;
    JITValue* column_null_data = inputs[idx]->get_null_datas()[0].get();
    JITValue* column_data = inputs[idx]->get_datas()[0].get();
    JITTypeTag tag = gen.getJITTag(inputs[idx]->get_type_info().get_type());
    // data buffer decoder
    auto data_pointer = column_data->castPointerSubType(tag);
    auto row_data = data_pointer[index];
    // null buffer decoder
    auto row_null_data = func->emitRuntimeFunctionCall(
        "check_bit_vector_clear",
        JITFunctionEmitDescriptor{.ret_type = JITTypeTag::BOOL,
                                  .params_vector = {{column_null_data, index.get()}}});

    vec.emplace_back(row_data);
    vec.emplace_back(row_null_data);
    inputs[idx]->set_expr_value(vec);
  }
}

}  // namespace cider::exec::nextgen::operators
