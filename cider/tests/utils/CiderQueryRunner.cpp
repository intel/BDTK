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

#include "substrait/type.pb.h"

#include "CiderBatchBuilder.h"
#include "CiderQueryRunner.h"
#include "Utils.h"
#include "cider/CiderRuntimeModule.h"
#include "exec/module/CiderCompilationResultImpl.h"
#include "exec/template/CountDistinct.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"
#include "util/measure.h"

#include <google/protobuf/util/json_util.h>

#define DEBUG_SUBSTRAIT_OUTPUT()                             \
  if (print_substrait_) {                                    \
    std::cout << "substrait json is: " << json << std::endl; \
  }

#define DEBUG_LLVM_IR()                                               \
  if (print_IR_) {                                                    \
    std::cout << "LLVM IR is: " << compile_res->getIR() << std::endl; \
  }

#define COMPILE_AND_GEN_RUNTIME_MODULE()                                             \
  compile_option.needs_error_check = true;                                           \
  auto compile_res = ciderCompileModule_->compile(plan, compile_option, exe_option); \
  auto cider_runtime_module =                                                        \
      std::make_shared<CiderRuntimeModule>(compile_res, compile_option, exe_option); \
  auto output_schema =                                                               \
      std::make_shared<CiderTableSchema>(compile_res->getOutputCiderTableSchema());  \
  DEBUG_LLVM_IR();

std::string getSubstraitPlanFilesPath() {
  const std::string absolute_path = __FILE__;
  auto const pos = absolute_path.find_last_of('/');
  return absolute_path.substr(0, pos) + "/../substrait_plan_files/";
}

bool isJsonFile(const std::string& file_or_sql) {
  auto const pos = file_or_sql.find_last_of('.');
  return std::string::npos != pos &&
         ".json" == file_or_sql.substr(pos, file_or_sql.size() - pos);
}

std::string getFileContent(const std::string& file_name) {
  std::ifstream file(getSubstraitPlanFilesPath() + file_name);
  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string content = buffer.str();
  return content;
}

::substrait::Plan CiderQueryRunner::genSubstraitPlan(const std::string& file_or_sql) {
  INJECT_TIMER(GenSubstraitPlan);
  std::string json;
  if (isJsonFile(file_or_sql)) {
    json = getFileContent(file_or_sql);
  } else {
    json = RunIsthmus::processSql(file_or_sql, create_ddl_);
  }

  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);
  DEBUG_SUBSTRAIT_OUTPUT();
  return std::move(plan);
}

CiderBatch CiderQueryRunner::runQueryOneBatch(
    const std::string& file_or_sql,
    const std::shared_ptr<CiderBatch>& input_batch) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: compile and gen runtime module
  COMPILE_AND_GEN_RUNTIME_MODULE();

  // Step 3: run on this batch
  cider_runtime_module->processNextBatch(*input_batch);

  // Step 4: handle agg and fetch data and sort data
  CiderBatch result_batch;
  if (cider_runtime_module->isGroupBy()) {
    auto iterator = cider_runtime_module->getGroupByAggHashTableIteratorAt(0);
    auto runtime_state = iterator->getRuntimeState();
    CHECK_EQ(runtime_state.getRowIndexNeedSpillVec().size(), 0);
    result_batch = std::move(handleRes(1024, cider_runtime_module, compile_res).front());
  } else if (compile_res->impl_->query_mem_desc_->hasCountDistinct() &&
             compile_res->impl_->query_mem_desc_->getQueryDescriptionType() ==
                 QueryDescriptionType::NonGroupedAggregate) {
    auto [_, output_batch] = cider_runtime_module->fetchResults();
    result_batch = updateCountDistinctRes(std::move(output_batch), compile_res);
  } else {
    auto [_, output_batch] = cider_runtime_module->fetchResults();
    if (!output_batch->schema()) {
      output_batch->set_schema(output_schema);
    }
    result_batch = std::move(*output_batch);
  }
  const SortInfo& sort_info = cider_runtime_module->getSortInfo();
  if (sort_info.order_entries.size() > 0 && !result_batch.getIsSorted()) {
    result_batch.sort(sort_info);
  }
  return std::move(result_batch);
}

std::vector<CiderBatch> CiderQueryRunner::runQueryMultiBatches(
    const std::string& file_or_sql,
    std::vector<std::shared_ptr<CiderBatch>>& input_batches) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: compile and gen runtime module
  COMPILE_AND_GEN_RUNTIME_MODULE();

  // Step 3 & Step 4: run on these batches and fetch data and sort data
  std::vector<CiderBatch> res_vec;
  CiderBatch output_batch;
  const auto& query_mem_desc = compile_res->impl_->query_mem_desc_;

  bool is_non_groupby_agg =
      compile_res->impl_->query_mem_desc_->getQueryDescriptionType() ==
      QueryDescriptionType::NonGroupedAggregate;

  if (cider_runtime_module->isGroupBy()) {
    for (auto it = input_batches.begin(); it != input_batches.end(); it++) {
      cider_runtime_module->processNextBatch(**it);
    }
    auto iterator = cider_runtime_module->getGroupByAggHashTableIteratorAt(0);
    CHECK_EQ(iterator->getRuntimeState().getRowIndexNeedSpillVec().size(), 0);
    res_vec = handleRes(1024, cider_runtime_module, compile_res);
  } else if (is_non_groupby_agg) {
    for (auto it = input_batches.begin(); it != input_batches.end(); it++) {
      cider_runtime_module->processNextBatch(**it);
    }
    auto [_, output_batch] = cider_runtime_module->fetchResults();
    if (!output_batch->schema()) {
      output_batch->set_schema(output_schema);
    }
    res_vec.emplace_back(std::move(*output_batch));
  } else {
    for (auto it = input_batches.begin(); it != input_batches.end(); it++) {
      cider_runtime_module->processNextBatch(**it);
      auto [_, output_batch] = cider_runtime_module->fetchResults();
      if (!output_batch->schema()) {
        output_batch->set_schema(output_schema);
      }
      res_vec.emplace_back(std::move(*output_batch));
    }
  }
  const SortInfo& sort_info = cider_runtime_module->getSortInfo();
  for (auto& result_batch : res_vec) {
    if (sort_info.order_entries.size() > 0 && !result_batch.getIsSorted()) {
      result_batch.sort(sort_info);
    }
  }
  return res_vec;
}

CiderBatch CiderQueryRunner::runJoinQueryOneBatch(const std::string& file_or_sql,
                                                  const CiderBatch& left_batch,
                                                  CiderBatch& right_batch) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: feed build table and compile and gen runtime module
  ciderCompileModule_->feedBuildTable(std::move(right_batch));
  auto compile_res = ciderCompileModule_->compile(plan);
  auto cider_runtime_module = std::make_shared<CiderRuntimeModule>(compile_res);
  DEBUG_LLVM_IR();

  auto output_schema =
      std::make_shared<CiderTableSchema>(compile_res->getOutputCiderTableSchema());

  // Step 3: run on this batch and fetch data and sort data
  cider_runtime_module->processNextBatch(left_batch);
  auto [_, output_batch] = cider_runtime_module->fetchResults();
  if (!output_batch->schema()) {
    output_batch->set_schema(output_schema);
  }
  const SortInfo& sort_info = cider_runtime_module->getSortInfo();
  if (sort_info.order_entries.size() > 0 && !output_batch->getIsSorted()) {
    output_batch->sort(sort_info);
  }
  return std::move(*output_batch);
}

std::vector<CiderBatch> CiderQueryRunner::runQueryForCountDistinct(
    const std::string& file_or_sql,
    const std::vector<std::shared_ptr<CiderBatch>> input_batches) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: compile and gen runtime module
  COMPILE_AND_GEN_RUNTIME_MODULE();

  // Step 3 & Step 4: run on this batch and fetch data and sort data
  CHECK(compile_res->impl_->query_mem_desc_->hasCountDistinct());
  // Check result for each batch process
  std::vector<CiderBatch> res_batches;
  for (int i = 0; i < input_batches.size(); i++) {
    cider_runtime_module->processNextBatch(*input_batches[i]);
    auto [_, output_batch] = cider_runtime_module->fetchResults();
    auto res_batch = updateCountDistinctRes(std::move(output_batch), compile_res);
    const SortInfo& sort_info = cider_runtime_module->getSortInfo();
    if (sort_info.order_entries.size() > 0 && !res_batch.getIsSorted()) {
      res_batch.sort(sort_info);
    }
    res_batches.push_back(std::move(res_batch));
  }
  return res_batches;
}

std::vector<CiderBatch> CiderQueryRunner::handleRes(
    const int max_output_row_num,
    std::shared_ptr<CiderRuntimeModule> cider_runtime_module,
    std::shared_ptr<CiderCompilationResult> compile_res) {
  int column_num = compile_res->getOutputCiderTableSchema().getColumnCount();
  auto schema = compile_res->getOutputCiderTableSchema();
  std::vector<CiderBatch> res;
  auto has_more_output = CiderRuntimeModule::ReturnCode::kMoreOutput;
  while (has_more_output == CiderRuntimeModule::ReturnCode::kMoreOutput) {
    std::vector<const int8_t*> out_col_buffers(column_num);
    for (size_t i = 0; i < column_num; ++i) {
      size_t type_bytes = schema.GetColumnTypeSize(i);
      out_col_buffers[i] = new int8_t[type_bytes * max_output_row_num];
    }
    auto schema_ptr = std::make_shared<CiderTableSchema>(schema);
    std::unique_ptr<CiderBatch> out_batch = nullptr;
    std::tie(has_more_output, out_batch) =
        cider_runtime_module->fetchResults(max_output_row_num);
    if (!out_batch->schema()) {
      out_batch->set_schema(schema_ptr);
    }
    if (compile_res->impl_->query_mem_desc_->hasCountDistinct()) {
      res.emplace_back(updateCountDistinctRes(std::move(out_batch), compile_res));
    } else {
      res.emplace_back(std::move(*out_batch));
    }
  }
  return res;
}

// get actual count number of distinct values
CiderBatch CiderQueryRunner::updateCountDistinctRes(
    std::unique_ptr<CiderBatch> output_batch,
    std::shared_ptr<CiderCompilationResult> compile_res) {
  const int8_t** outBuffers = output_batch->table();
  for (int i = 0; i < output_batch->column_num(); i++) {
    if (compile_res->impl_->query_mem_desc_->getCountDistinctDescriptor(i).impl_type_ !=
        CountDistinctImplType::Invalid) {
      int64_t* result =
          const_cast<int64_t*>(reinterpret_cast<const int64_t*>(outBuffers[i]));
      for (int j = 0; j < output_batch->row_num(); j++) {
        result[j] = reinterpret_cast<CiderBatch*>(result[j])->row_num();
      }
    }
  }
  return std::move(*output_batch);
}
