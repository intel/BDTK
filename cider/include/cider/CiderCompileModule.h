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

#ifndef CIDER_CIDERCOMPILEMODULE_H
#define CIDER_CIDERCOMPILEMODULE_H

#include <map>
#include <string>

#include "CiderInterface.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "substrait/plan.pb.h"

class AggregatedColRange;

enum class QueryType {
  kStateless = 0,
  kStatefulGroupBy,
  kStatefulNonGroupBy,
  kInvalid = -1
};

class CiderCompilationResult {
 public:
  CiderCompilationResult();
  ~CiderCompilationResult();

  std::string getIR() const;

  std::vector<int8_t> getHoistLiteral() const;

  void* func() const;

  std::shared_ptr<CiderTableSchema> getOutputCiderTableSchema() const;

  QueryType getQueryType() const;

  class Impl;
  std::unique_ptr<Impl> impl_;
};

class CiderCompileModule {
 public:
  CiderCompileModule(std::shared_ptr<CiderAllocator> allocator);
  ~CiderCompileModule();

  static std::shared_ptr<CiderCompileModule> Make(
      std::shared_ptr<CiderAllocator> allocator);

  std::shared_ptr<CiderCompilationResult> compile(
      const substrait::Plan& plan,
      CiderCompilationOption cco = CiderCompilationOption::defaults(),
      CiderExecutionOption ceo = CiderExecutionOption::defaults());

  // compile API for expression
  std::shared_ptr<CiderCompilationResult> compile(
      const std::vector<substrait::Expression*> expr,
      const substrait::NamedStruct& schema,
      const std::vector<
          substrait::extensions::SimpleExtensionDeclaration_ExtensionFunction*>
          func_infos,
      const generator::ExprType& expr_type,
      CiderCompilationOption cco = CiderCompilationOption::defaults(),
      CiderExecutionOption ceo = CiderExecutionOption::defaults());

  // For test only
  std::shared_ptr<CiderCompilationResult> compile(
      void* ra_exe_unit,
      void* query_infos,
      CiderCompilationOption cco = CiderCompilationOption::defaults(),
      CiderExecutionOption ceo = CiderExecutionOption::defaults());

  // For test only (with table schema)
  std::shared_ptr<CiderCompilationResult> compile(
      void* ra_exe_unit,
      void* query_infos,
      std::shared_ptr<CiderTableSchema> schema,
      CiderCompilationOption cco = CiderCompilationOption::defaults(),
      CiderExecutionOption ceo = CiderExecutionOption::defaults());

  // User of CiderCompileModule needs to feed ALL build table data by calling this API
  // before compile join query. We use a right reference here to steal CiderBatch from
  // caller side
  void feedBuildTable(CiderBatch&& build_table);

  class Impl;
  std::unique_ptr<Impl> impl_;
};

#endif  // CIDER_CIDERCOMPILEMODULE_H
