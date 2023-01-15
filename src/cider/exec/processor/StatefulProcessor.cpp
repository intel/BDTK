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

#include "exec/processor/StatefulProcessor.h"

#include <utility>

namespace cider::exec::processor {

StatefulProcessor::StatefulProcessor(const plan::SubstraitPlanPtr& plan,
                                     const BatchProcessorContextPtr& context)
    : DefaultBatchProcessor(plan, context) {
  is_groupby_ = plan->isGroupingAggregateRel();
}

void StatefulProcessor::getResult(struct ArrowArray& array, struct ArrowSchema& schema) {
  if (!no_more_batch_ || !has_result_) {
    array.length = 0;
    return;
  }

  state_ = BatchProcessorState::kFinished;

  if (!is_groupby_) {
    has_result_ = false;
    auto output_batch = runtime_context_->getNonGroupByAggOutputBatch();
    output_batch->move(schema, array);
  }

  return;
}

}  // namespace cider::exec::processor
