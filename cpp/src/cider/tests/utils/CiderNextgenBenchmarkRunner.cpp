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

#include "CiderNextgenBenchmarkRunner.h"
#include "util/measure.h"
extern bool g_enable_debug_timer;

namespace cider::test::util {

void CiderNextgenBenchmarkRunner::runQueryOneBatch(
    const std::string& file_or_sql,
    const struct ArrowArray& input_array,
    const struct ArrowSchema& input_schema,
    struct ArrowArray& output_array,
    struct ArrowSchema& output_schema,
    const cider::exec::nextgen::context::CodegenOptions& codegen_options) {
  g_enable_debug_timer = true;

  // Step 1: construct substrait plan
  auto plan = genSubstraitPlan(file_or_sql);

  {
    INJECT_TIMER(create);
    // Step 2: compile and gen runtime module
    processor_ = makeBatchProcessor(plan, context_);
  }

  {
    INJECT_TIMER(run);
    // Step 3: run on this batch
    processor_->processNextBatch(&input_array, &input_schema);

    // Step 4: fetch data
    processor_->getResult(output_array, output_schema);
  }
  return;
}
}  // namespace cider::test::util
