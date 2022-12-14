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

#ifndef MODULARSQL_CIDERRUNNER_H
#define MODULARSQL_CIDERRUNNER_H

#include "Utils.h"
#include "cider/CiderBatch.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"

#include <memory>

class CiderCompiler {
 public:
  CiderCompiler();
  ~CiderCompiler() {}

  std::shared_ptr<CiderCompilationResult> compile(
      const std::string& create_ddl,
      const std::string& file_or_sql,
      CiderCompilationOption cco = CiderCompilationOption::defaults(),
      CiderExecutionOption ceo = CiderExecutionOption::defaults());

 private:
  std::shared_ptr<CiderCompileModule> cider_compile_module_;
};

class CiderRunner {
 public:
  CiderRunner() {}
  virtual ~CiderRunner() {}
  static std::shared_ptr<CiderRunner> createCiderRunner(
      std::shared_ptr<CiderCompilationResult> res);
  virtual std::shared_ptr<CiderBatch> processNextBatch(
      const std::shared_ptr<CiderBatch>& input_batch) = 0;
  virtual std::shared_ptr<CiderBatch> finish() = 0;
  virtual bool isFinished() { return finished_; }

 protected:
  std::shared_ptr<CiderCompilationResult> com_res_;
  std::shared_ptr<CiderRuntimeModule> cider_runtime_module_;
  bool finished_ = true;

 private:
};

class CiderStatefulRunner : public CiderRunner {
 public:
  ~CiderStatefulRunner() {}
  explicit CiderStatefulRunner(std::shared_ptr<CiderCompilationResult> res);
  std::shared_ptr<CiderBatch> finish() override;  // do nothing, return a nullptr;
  std::shared_ptr<CiderBatch> processNextBatch(
      const std::shared_ptr<CiderBatch>& input_batch) override;
};

class CiderStatelessRunner : public CiderRunner {
 public:
  ~CiderStatelessRunner() {}
  explicit CiderStatelessRunner(std::shared_ptr<CiderCompilationResult> res);
  std::shared_ptr<CiderBatch> finish() override;
  std::shared_ptr<CiderBatch> processNextBatch(
      const std::shared_ptr<CiderBatch>& input_batch) override;
};

#endif  // MODULARSQL_CIDERRUNNER_H
