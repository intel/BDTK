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

#include "exec/processor/DefaultBatchProcessor.h"

#include <memory>

#include "cider/CiderException.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/processor/StatefulProcessor.h"
#include "exec/processor/StatelessProcessor.h"

namespace cider::exec::processor {

DefaultBatchProcessor::DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                                             const BatchProcessorContextPtr& context)
    : plan_(plan), context_(context) {
  auto allocator = context->allocator();
  if (plan_->hasJoinRel()) {
    // TODO: currently we can't distinguish the joinRel is either a hashJoin rel
    // or a mergeJoin rel, just hard-code as HashJoinHandler for now and will refactor to
    // initialize joinHandler accordingly once the
    joinHandler_ = std::make_shared<HashProbeHandler>(shared_from_this());
    this->state_ = BatchProcessorState::kWaiting;
  }

  auto translator =
      std::make_shared<generator::SubstraitToRelAlgExecutionUnit>(plan_->getPlan());
  RelAlgExecutionUnit ra_exe_unit = translator->createRelAlgExecutionUnit();
  jitlib::CompilationOptions co;
  co.dump_ir = true;
  codegen_context_ = nextgen::compile(ra_exe_unit, co);
  runtime_context_ = codegen_context_->generateRuntimeCTX(allocator);
  query_func_ = reinterpret_cast<nextgen::QueryFunc>(
      codegen_context_->getJITFunction()->getFunctionPointer<void, int8_t*, int8_t*>());
}

void DefaultBatchProcessor::processNextBatch(const struct ArrowArray* array,
                                             const struct ArrowSchema* schema) {
  if (BatchProcessorState::kRunning != state_) {
    CIDER_THROW(CiderRuntimeException,
                "DefaultBatchProcessor::processNextBatch can only be called if state is "
                "kRunning.");
  }
  if (joinHandler_) {
    // this->inputBatch_ = joinHandler_->onProcessBatch(batch);
  } else {
    input_arrow_array_ = array;
  }

  query_func_((int8_t*)runtime_context_.get(), (int8_t*)array);
}

BatchProcessorState DefaultBatchProcessor::getState() {
  if (joinHandler_) {
    joinHandler_->onState(state_);
  }
  return state_;
}

void DefaultBatchProcessor::finish() {
  no_more_batch_ = true;
  if (joinHandler_) {
    joinHandler_->onFinish();
  }
}

void DefaultBatchProcessor::feedHashBuildTable(
    const std::shared_ptr<JoinHashTable>& hashTable) {
  // switch state from waiting to running once hashTable is ready
  this->state_ = BatchProcessorState::kRunning;
  // TODO: feed the hashTable into nextGen context
}

std::unique_ptr<BatchProcessor> makeBatchProcessor(
    const ::substrait::Plan& plan,
    const BatchProcessorContextPtr& context) {
  auto substraitPlan = std::make_shared<plan::SubstraitPlan>(plan);
  if (substraitPlan->hasAggregateRel()) {
    return std::make_unique<StatefulProcessor>(substraitPlan, context);
  } else {
    return std::make_unique<StatelessProcessor>(substraitPlan, context);
  }
}

}  // namespace cider::exec::processor
