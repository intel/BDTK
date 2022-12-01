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

#include "DefaultBatchProcessor.h"
#include "StatefulProcessor.h"
#include "StatelessProcessor.h"

namespace cider::processor {

DefaultBatchProcessor::DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                                             const BatchProcessorContextPtr& context)
    : plan_(plan), context_(context) {
  auto allocator = context->getAllocator();
  const auto substraitPlan = plan_->getPlan();
  // TODO: create instance of ciderCompileModule_ ?
}

void DefaultBatchProcessor::processNextBatch(std::shared_ptr<CiderBatch> batch) {
  this->inputBatch_ = std::move(batch);
  // TODO: processBatch through nextGen API
}

BatchProcessorState DefaultBatchProcessor::getState() {
  return BatchProcessorState::kRunning;
}

void DefaultBatchProcessor::finish() {
  noMoreBatch_ = true;
}

void DefaultBatchProcessor::setState(BatchProcessorState state) {
  this->state_ = state;
}

bool DefaultBatchProcessor::isFinished() {
  return BatchProcessorState::kFinished == state_;
}

std::shared_ptr<BatchProcessor> makeBatchProcessor(
    const ::substrait::Plan& plan,
    const BatchProcessorContextPtr& context) {
  auto substraitPlan = std::make_shared<plan::SubstraitPlan>(plan);
  if (substraitPlan->hasAggregateRel()) {
    return std::make_shared<StatefulProcessor>(substraitPlan, context);
  } else {
    return std::make_shared<StatelessProcessor>(substraitPlan, context);
  }
}

}  // namespace cider::processor
