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

#include "CiderRunner.h"
#include <google/protobuf/util/json_util.h>

CiderCompiler::CiderCompiler() {
  cider_compile_module_ = CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());
}

std::shared_ptr<CiderCompilationResult> CiderCompiler::compile(
    const std::string& create_ddl,
    const std::string& sql,
    CiderCompilationOption cco,
    CiderExecutionOption ceo) {
  std::string json = RunIsthmus::processSql(sql, create_ddl);
  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);

  return cider_compile_module_->compile(plan, cco, ceo);
}

std::shared_ptr<CiderRunner> CiderRunner::createCiderRunner(
    std::shared_ptr<CiderCompilationResult> res) {
  if (res->getQueryType() == QueryType::kStatefulNonGroupBy ||
      res->getQueryType() == QueryType::kStatefulGroupBy) {
    return std::make_shared<CiderStatefulRunner>(res);
  } else if (res->getQueryType() == QueryType::kStateless) {
    return std::make_shared<CiderStatelessRunner>(res);
  }
}

CiderStatelessRunner::CiderStatelessRunner(std::shared_ptr<CiderCompilationResult> res) {
  com_res_ = res;
  cider_runtime_module_ = std::make_shared<CiderRuntimeModule>(com_res_);
}

std::shared_ptr<CiderBatch> CiderStatelessRunner::finish() {
  finished_ = true;  // or move to constructor;
  return nullptr;
}

std::shared_ptr<CiderBatch> CiderStatelessRunner::processNextBatch(
    const std::shared_ptr<CiderBatch>& input_batch) {
  cider_runtime_module_->processNextBatch(*input_batch);

  auto [_, output_batch] = cider_runtime_module_->fetchResults();
  return std::move(output_batch);
}

CiderStatefulRunner::CiderStatefulRunner(std::shared_ptr<CiderCompilationResult> res) {
  com_res_ = res;
  cider_runtime_module_ = std::make_shared<CiderRuntimeModule>(com_res_);
}

std::shared_ptr<CiderBatch> CiderStatefulRunner::finish() {
  if (isFinished()) {
    return nullptr;
  }
  auto [state, output_batch] = cider_runtime_module_->fetchResults();
  if (state == CiderRuntimeModule::ReturnCode::kNoMoreOutput) {
    finished_ = true;
  }
  return std::move(output_batch);
}
std::shared_ptr<CiderBatch> CiderStatefulRunner::processNextBatch(
    const std::shared_ptr<CiderBatch>& input_batch) {
  cider_runtime_module_->processNextBatch(*input_batch);

  return nullptr;
}
