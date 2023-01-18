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

#include "QueryExecutionContext.h"
#include "AggregateUtils.h"
#include "Execute.h"
#include "InPlaceSort.h"
#include "QueryMemoryInitializer.h"
#include "RelAlgExecutionUnit.h"
#include "ResultSet.h"
#include "SpeculativeTopN.h"
#include "StreamingTopN.h"
#include "exec/template/common/descriptors/QueryMemoryDescriptor.h"
#include "util/likely.h"

extern bool g_enable_non_kernel_time_query_interrupt;
extern bool g_enable_dynamic_watchdog;
extern unsigned g_dynamic_watchdog_time_limit;

QueryExecutionContext::QueryExecutionContext(
    const RelAlgExecutionUnit& ra_exe_unit,
    const QueryMemoryDescriptor& query_mem_desc,
    const Executor* executor,
    const ExecutorDispatchMode dispatch_mode,
    const int device_id,
    const int64_t num_rows,
    const std::vector<std::vector<const int8_t*>>& col_buffers,
    const std::vector<std::vector<uint64_t>>& frag_offsets,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    const bool output_columnar,
    const size_t thread_idx)
    : query_mem_desc_(query_mem_desc)
    , executor_(executor)
    , dispatch_mode_(dispatch_mode)
    , row_set_mem_owner_(row_set_mem_owner)
    , output_columnar_(output_columnar) {
  CHECK(executor);

  query_buffers_ = std::make_unique<QueryMemoryInitializer>(ra_exe_unit,
                                                            query_mem_desc,
                                                            device_id,
                                                            dispatch_mode,
                                                            output_columnar,
                                                            num_rows,
                                                            col_buffers,
                                                            frag_offsets,
                                                            row_set_mem_owner,
                                                            thread_idx,
                                                            executor);
}

int64_t QueryExecutionContext::getAggInitValForIndex(const size_t index) const {
  CHECK(query_buffers_);
  return query_buffers_->getAggInitValForIndex(index);
}