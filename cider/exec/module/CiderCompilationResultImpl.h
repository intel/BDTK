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

#ifndef CIDER_CIDERCOMPILATIONRESULTIMPL_H
#define CIDER_CIDERCOMPILATIONRESULTIMPL_H

#include "cider/CiderCompileModule.h"
#include "exec/template/Execute.h"

class CiderCompilationResult::Impl {
 public:
  Impl() {}
  ~Impl() {}

  std::string getIR() { return compilation_result_.llvm_ir; }

  std::vector<int8_t> getHoistLiteral() const { return hoist_buf; }

  void* func() {
    auto cpu_generated_code = std::dynamic_pointer_cast<CpuCompilationContext>(
        compilation_result_.generated_code);
    return cpu_generated_code->func();
  }

  std::shared_ptr<CiderTableSchema> getOutputCiderTableSchema() const {
    return outputSchema_;
  }

  QueryType getQueryType() const {
    switch (query_mem_desc_->getQueryDescriptionType()) {
      case QueryDescriptionType::GroupByPerfectHash:
      case QueryDescriptionType::GroupByBaselineHash:
        return QueryType::kStatefulGroupBy;
      case QueryDescriptionType::NonGroupedAggregate:
        return QueryType::kStatefulNonGroupBy;
      case QueryDescriptionType::Projection:
        return QueryType::kStateless;
      default:
        return QueryType::kInvalid;
    }
  }

  CompilationResult compilation_result_;
  std::unique_ptr<QueryMemoryDescriptor> query_mem_desc_;
  std::shared_ptr<RelAlgExecutionUnit> rel_alg_exe_unit_;
  bool hoist_literals_;
  std::vector<int8_t> hoist_buf;
  std::shared_ptr<CiderTableSchema> outputSchema_;
  CiderBatch build_table_;
  std::shared_ptr<StringDictionaryProxy> ciderStringDictionaryProxy_;
};

#endif
