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

#include "StatefulProcessor.h"
#include "StatelessProcessor.h"
#include "include/cider/processor/BatchProcessor.h"

namespace cider::exec::processor {

namespace substrait_plan {

bool hasAggregateRel(const ::substrait::Plan* plan) {
  for (auto& rel : plan->relations()) {
    if (rel.has_root() && rel.root().has_input()) {
      return rel.root().input().has_aggregate();
    }
  }
  return false;
}

bool hasJoinRel(const ::substrait::Plan* plan) {
  for (auto& rel : plan->relations()) {
    if (rel.has_root() && rel.root().has_input()) {
      return rel.root().input().has_join();
    }
  }
  return false;
}

}  // namespace substrait_plan

BatchProcessor::BatchProcessor(const ::substrait::Plan* plan,
                               const ProcessorContextPtr& context)
    : plan_(plan), context_(context) {
  // TODO: construct ciderCompileModule and runtime module
}

std::shared_ptr<BatchProcessor> BatchProcessor::makeProcessor(
    const ::substrait::Plan* plan,
    const ProcessorContextPtr& context) {
  bool isStatefulPipeline = substrait_plan::isStatefulPipeline(plan);
  if (isStatefulPipeline) {
    return std::make_shared<StatefulProcessor>(plan, context);
  } else {
    return std::make_shared<StatelessProcessor>(plan, context);
  }
}

void BatchProcessor::processNextBatch(std::shared_ptr<CiderBatch> batch) {
  this->inputBatch_ = std::move(batch);
}

std::shared_ptr<CiderBatch> BatchProcessor::getResult() {
  return std::move(this->inputBatch_);
}

ProcessorState BatchProcessor::getState() {
  return ProcessorState::kRunning;
}

}  // namespace cider::exec::processor
