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
#include "CiderBenchmarkRunner.h"

void CiderBenchmarkRunner::compile(const std::string& sql) {
  auto plan = genSubstraitPlan(sql);
  compile_option.use_cider_data_format = true;
  compile_res_ = ciderCompileModule_->compile(plan, compile_option, exe_option);
  cider_runtime_module_ =
      std::make_shared<CiderRuntimeModule>(compile_res_, compile_option, exe_option);
  output_schema_ = compile_res_->getOutputCiderTableSchema();
}

CiderBatch CiderBenchmarkRunner::runNextBatch(
    const std::shared_ptr<CiderBatch>& input_batch) {
  cider_runtime_module_->processNextBatch(*input_batch);
  if (cider_runtime_module_->isGroupBy()) {
    auto iterator = cider_runtime_module_->getGroupByAggHashTableIteratorAt(0);
    auto runtime_state = iterator->getRuntimeState();
    CHECK_EQ(runtime_state.getRowIndexNeedSpillVec().size(), 0);
    return std::move(handleRes(1024, cider_runtime_module_, compile_res_).front());
  } else {
    auto [_, output_batch] = cider_runtime_module_->fetchResults();
    if (!output_batch->schema()) {
      output_batch->set_schema(output_schema_);
    }
    return std::move(*output_batch);
  }
}
