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

#include "CiderPipelineOperator.h"
#include "Allocator.h"
#include "CiderCrossJoinBuild.h"
#include "CiderHashJoinBuild.h"
#include "exec/plan/substrait/SubstraitPlan.h"
#include "velox/exec/Task.h"
#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#include "velox/vector/arrow/Abi.h"
#endif
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

bool CiderPipelineOperator::needsInput() const {
  return !finished_;
}

void CiderPipelineOperator::addInput(RowVectorPtr input) {
  for (size_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->mutableRawNulls();
  }
  this->input_ = std::move(input);
  ArrowArray inputArrowArray;
  exportToArrow(input_, inputArrowArray);

  batchProcessor_->processNextBatch(&inputArrowArray);
}

facebook::velox::exec::BlockingReason CiderPipelineOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  auto batchProcessState = batchProcessor_->getState();
  if (cider::exec::processor::BatchProcessorState::kWaiting == batchProcessState &&
      ciderPlanNode_->isKindOf(CiderPlanNodeKind::kJoin)) {
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  if (cider::exec::processor::BatchProcessorState::kFinished == batchProcessState) {
    finished_ = true;
  }

  if (future_.valid()) {
    VELOX_CHECK(!isFinished());
    *future = std::move(future_);
  }
  return exec::BlockingReason::kNotBlocked;
}

bool CiderPipelineOperator::isFinished() {
  return finished_;
}

facebook::velox::RowVectorPtr CiderPipelineOperator::getOutput() {
  struct ArrowArray array;
  struct ArrowSchema schema;

  batchProcessor_->getResult(array, schema);
  if (array.length) {
    VectorPtr baseVec = importFromArrowAsOwner(schema, array, operatorCtx_->pool());
    return std::reinterpret_pointer_cast<RowVector>(baseVec);
  }
  return nullptr;
}

void CiderPipelineOperator::noMoreInput() {
  batchProcessor_->finish();
}

CiderPipelineOperator::CiderPipelineOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const CiderPlanNode>& ciderPlanNode)
    : Operator(driverCtx,
               ciderPlanNode->outputType(),
               operatorId,
               ciderPlanNode->id(),
               "CiderOp")
    , ciderPlanNode_(ciderPlanNode)
    , allocator_(std::make_shared<PoolAllocator>(operatorCtx_->pool())) {
  auto substraitPlan = ciderPlanNode->getSubstraitPlan();
  auto planUtil = std::make_shared<cider::exec::plan::SubstraitPlan>(substraitPlan);
  auto context =
      std::make_shared<cider::exec::processor::BatchProcessorContext>(allocator_);

  if (planUtil->hasCrossRel()) {
    cider::exec::processor::CrossBuildTableSupplier crossBuildTableSupplier = [&]() {
      auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
          operatorCtx_->driverCtx()->splitGroupId, planNodeId());
      auto ciderJoinBridge = std::dynamic_pointer_cast<CiderCrossJoinBridge>(joinBridge);
      return *ciderJoinBridge->hasDataOrFuture(&future_);
    };
    context->setCrossJoinBuildTableSupplier(crossBuildTableSupplier);
  } else if (planUtil->hasJoinRel()) {
    cider::exec::processor::HashBuildTableSupplier buildTableSupplier = [&]() {
      auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
          operatorCtx_->driverCtx()->splitGroupId, planNodeId());
      auto ciderJoinBridge = std::dynamic_pointer_cast<CiderHashJoinBridge>(joinBridge);
      return ciderJoinBridge->hashBuildResultOrFuture(&future_);
    };
    context->setHashBuildTableSupplier(buildTableSupplier);
  }

  batchProcessor_ = cider::exec::processor::BatchProcessor::Make(substraitPlan, context);
}

}  // namespace facebook::velox::plugin
