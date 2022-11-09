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

#include "exec/nextgen/translator/project.h"
#include "exec/nextgen/jitlib/base/JITValue.h"
#include "exec/nextgen/translator/dummy.h"
#include "exec/nextgen/translator/expr.h"

namespace cider::exec::nextgen::translator {
void ProjectTranslator::consume(Context& context) {
  codegen(context);
}

void ProjectTranslator::codegen(Context& context) {
  ExprGenerator gen(context.query_func_);
  for (const auto& expr : node_.exprs_) {
    gen.codegen(expr.get());
  }
}

}  // namespace cider::exec::nextgen::translator
