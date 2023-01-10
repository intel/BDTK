/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#ifndef QUERYENGINE_QUERYEXECUTIONCONTEXT_H
#define QUERYENGINE_QUERYEXECUTIONCONTEXT_H

#include "CompilationOptions.h"
#include "ResultSet.h"

#include "QueryMemoryInitializer.h"

#include <boost/core/noncopyable.hpp>
#include <vector>

class CpuCompilationContext;

struct RelAlgExecutionUnit;
class QueryMemoryDescriptor;
class Executor;

class QueryExecutionContext : boost::noncopyable {
 public:
  // TODO(alex): remove device_type
  QueryExecutionContext(const RelAlgExecutionUnit& ra_exe_unit,
                        const QueryMemoryDescriptor&,
                        const Executor* executor,
                        const ExecutorDispatchMode dispatch_mode,
                        const int device_id,
                        const int64_t num_rows,
                        const std::vector<std::vector<const int8_t*>>& col_buffers,
                        const std::vector<std::vector<uint64_t>>& frag_offsets,
                        std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                        const bool output_columnar,
                        const size_t thread_idx);

  int64_t getAggInitValForIndex(const size_t index) const;

 private:
  // TODO(adb): convert to shared_ptr
  QueryMemoryDescriptor query_mem_desc_;
  const Executor* executor_;
  const ExecutorDispatchMode dispatch_mode_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  const bool output_columnar_;
  std::unique_ptr<QueryMemoryInitializer> query_buffers_;
  mutable std::unique_ptr<ResultSet> estimator_result_set_;

  friend class Executor;
};

#endif  // QUERYENGINE_QUERYEXECUTIONCONTEXT_H
