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

#ifndef MODULARSQL_CIDERBENCHMARKRUNNER_H
#define MODULARSQL_CIDERBENCHMARKRUNNER_H

#include "CiderQueryRunner.h"
#include "cider/CiderBatch.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"

#include <memory>
#include <string>
#include <vector>

class CiderBenchmarkRunner : public CiderQueryRunner {
 public:
  CiderBenchmarkRunner() {
    ciderCompileModule_ = CiderCompileModule::Make();
    exe_option = CiderExecutionOption::defaults();
    compile_option = CiderCompilationOption::defaults();
    compile_option.max_groups_buffer_entry_guess = 16384;
  }

  void prepare(const std::string& create_ddl) { create_ddl_ = create_ddl; }

  void compile(const std::string& sql);

  CiderBatch runNextBatch(const std::shared_ptr<CiderBatch>& input_batch);

 private:
  std::shared_ptr<CiderRuntimeModule> cider_runtime_module_;
  std::shared_ptr<CiderTableSchema> output_schema_;
  std::shared_ptr<CiderCompilationResult> compile_res_;
};
#endif  // MODULARSQL_CIDERBENCHMARKRUNNER_H
