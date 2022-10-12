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

#ifndef CIDER_CIDERQUERYRUNNER_H
#define CIDER_CIDERQUERYRUNNER_H

#include "cider/CiderBatch.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"

#include <memory>
#include <string>
#include <vector>

class CiderQueryRunner {
 public:
  CiderQueryRunner() {
    ciderCompileModule_ =
        CiderCompileModule::Make(std::make_shared<CiderDefaultAllocator>());
    exe_option = CiderExecutionOption::defaults();
    compile_option = CiderCompilationOption::defaults();
    compile_option.max_groups_buffer_entry_guess = 16384;
  }

  void prepare(const std::string& create_ddl) { create_ddl_ = create_ddl; }

  CiderBatch runQueryOneBatch(const std::string& file_or_sql,
                              const std::shared_ptr<CiderBatch>& input_batch,
                              bool is_arrow_format = false);

  std::vector<CiderBatch> runQueryMultiBatches(
      const std::string& file_or_sql,
      std::vector<std::shared_ptr<CiderBatch>>& input_batches);

  CiderBatch runJoinQueryOneBatch(const std::string& file_or_sql,
                                  const CiderBatch& left_batch,
                                  CiderBatch& right_batch);

  std::vector<CiderBatch> runQueryForCountDistinct(
      const std::string& file_or_sql,
      const std::vector<std::shared_ptr<CiderBatch>> input_batches);

  ::substrait::Plan genSubstraitPlan(const std::string& file_or_sql);

  CiderBatch runMoreBatch(const CiderBatch& left_batch);

 protected:
  std::string create_ddl_;
  std::shared_ptr<CiderCompileModule> ciderCompileModule_;
  CiderExecutionOption exe_option;
  CiderCompilationOption compile_option;

  std::vector<CiderBatch> handleRes(
      const int max_output_row_num,
      std::shared_ptr<CiderRuntimeModule> cider_runtime_module,
      std::shared_ptr<CiderCompilationResult> compile_res);

 private:
  // for debug
  bool print_substrait_ = false;
  bool print_IR_ = false;

  CiderBatch updateCountDistinctRes(std::unique_ptr<CiderBatch> output_batch,
                                    std::shared_ptr<CiderCompilationResult> compile_res);
};
#endif  // CIDER_CIDERQUERYRUNNER_H
