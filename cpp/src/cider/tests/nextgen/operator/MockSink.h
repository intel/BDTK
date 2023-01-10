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

#ifndef CIDER_EXEC_NEXTGEN_OPERATOR_MOCK_SINK_H
#define CIDER_EXEC_NEXTGEN_OPERATOR_MOCK_SINK_H

#include "exec/nextgen/Context.h"
#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen::operators {
class MockSinkTranslator : public Translator {
 public:
  explicit MockSinkTranslator(size_t start_pos) : start_pos_(start_pos) {}

  void consume(Context& context) override { codegen(context); };

 private:
  void codegen(Context& context) {
    auto& func_ = context.query_func_;
    for (auto& out : context.expr_outs_) {
      auto var = func_->getArgument(start_pos_++);
      **var = out->getValue();
    }
  }

  size_t start_pos_;
};

}  // namespace cider::exec::nextgen::operators
#endif
