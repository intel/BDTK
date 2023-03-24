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

#include "tests/utils/CiderNextgenQueryRunner.h"

#include <google/protobuf/util/json_util.h>
#include <fstream>

#include "exec/module/batch/ArrowABI.h"
#include "exec/nextgen/context/Batch.h"
#include "tests/utils/Utils.h"
#include "util/measure.h"

namespace cider::test::util {

namespace {

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
  if (!file.good()) {
    CIDER_THROW(CiderException, "Substrait JSON file " + file_name + " does not exist.");
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string content = buffer.str();
  return content;
}

}  // namespace

::substrait::Plan CiderNextgenQueryRunner::genSubstraitPlan(
    const std::string& file_or_sql) {
  std::string json;
  if (isJsonFile(file_or_sql)) {
    json = getFileContent(file_or_sql);
  } else {
    json = RunIsthmus::processSql(file_or_sql, create_ddl_);
  }

  ::substrait::Plan plan;
  google::protobuf::util::JsonStringToMessage(json, &plan);
  return plan;
}

void CiderNextgenQueryRunner::runQueryOneBatch(const std::string& file_or_sql,
                                               const ArrowArray& input_array,
                                               const ArrowSchema& input_schema,
                                               ArrowArray& output_array,
                                               ArrowSchema& output_schema,
                                               const CodegenOptions& codegen_options) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: compile and gen runtime module
  processor_ = exec::processor::BatchProcessor::Make(plan, context_, codegen_options);

  // Step 3: run on this batch
  processor_->processNextBatch(&input_array, &input_schema);

  // Step 4: notify no more input to process
  processor_->finish();

  // Step 5: fetch data
  processor_->getResult(output_array, output_schema);

  return;
}

void CiderNextgenQueryRunner::runJoinQueryOneBatch(
    const std::string& file_or_sql,
    const struct ArrowArray& input_array,
    const struct ArrowSchema& input_schema,
    struct ArrowArray& build_array,
    struct ArrowSchema& build_schema,
    struct ArrowArray& output_array,
    struct ArrowSchema& output_schema,
    const CodegenOptions& codegen_options) {
  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  // Step 2: compile and gen runtime module
  processor_ = exec::processor::BatchProcessor::Make(plan, context_, codegen_options);

  auto table_build_context = std::make_shared<exec::processor::JoinHashTableBuildContext>(
      context_->getAllocator());
  auto join_hash_builder = makeJoinHashTableBuilder(
      plan.relations(0).root().input().join(), table_build_context);

  exec::nextgen::context::Batch build_batch(build_schema, build_array);
  join_hash_builder->appendBatch(
      std::make_shared<exec::nextgen::context::Batch>(build_batch));
  auto hm = join_hash_builder->build();
  processor_->feedHashBuildTable(std::move(hm));

  // Step 3: run on this batch
  processor_->processNextBatch(&input_array, &input_schema);

  // Step 4: notify no more input to process
  processor_->finish();

  // Step 5: fetch data
  processor_->getResult(output_array, output_schema);

  return;
}

}  // namespace cider::test::util
