/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "CiderExprEvaluator.h"

CiderExprEvaluator::CiderExprEvaluator(
    std::vector<::substrait::Expression*> exprs,
    std::vector<::substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
        funcs_info,
    ::substrait::NamedStruct* schema,
    std::shared_ptr<CiderAllocator> allocator,
    const generator::ExprType& expr_type)
    : allocator_(allocator) {
  // Set up exec option and compilation option
  auto exec_option = CiderExecutionOption::defaults();
  auto compile_option = CiderCompilationOption::defaults();

  auto ciderCompileModule = CiderCompileModule::Make(allocator_);
  auto result = ciderCompileModule->compile(
      exprs, *schema, funcs_info, expr_type, compile_option, exec_option);
  runner_ = std::make_shared<CiderRuntimeModule>(result, compile_option, exec_option);
}

CiderBatch CiderExprEvaluator::eval(const CiderBatch& in_batch) {
  runner_->processNextBatch(in_batch);
  auto [_, out_batch] = runner_->fetchResults();
  return std::move(*out_batch);
}
