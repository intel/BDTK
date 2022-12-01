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

#include "CiderPipelineOperator.h"
#include "Allocator.h"
#include "CiderHashJoinBuild.h"
#include "velox/exec/Task.h"
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

bool CiderPipelineOperator::needsInput() const {
  return !finished_;
}

void CiderPipelineOperator::addInput(RowVectorPtr input) {
  for (size_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->mutableRawNulls();
  }
  ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
  exportToArrow(input_, *inputArrowArray);
  ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
  exportToArrow(input_, *inputArrowSchema);

  auto allocator = std::make_shared<PoolAllocator>(operatorCtx_->pool());
  auto inBatch =
      CiderBatchUtils::createCiderBatch(allocator, inputArrowSchema, inputArrowArray);
  batchProcessor_->processNextBatch(std::move(inBatch));
}

facebook::velox::exec::BlockingReason CiderPipelineOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  auto batchProcessState = batchProcessor_->getState();
  if (cider::processor::BatchProcessorState::kWaiting == batchProcessState) {
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  if (cider::processor::BatchProcessorState::kFinished == batchProcessState) {
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
  auto output_ = batchProcessor_->getResult();
  if (output_) {
    ArrowSchema schema;
    ArrowArray array;
    output_->move(schema, array);
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
               "CiderOp") {
  auto context = std::make_shared<cider::processor::BatchProcessorContext>(
      std::make_shared<PoolAllocator>(operatorCtx_->pool()));

  cider::processor::HashBuildTableSupplier buildTableSupplier = [&]() {
    auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
    auto ciderJoinBridge = std::dynamic_pointer_cast<CiderHashJoinBridge>(joinBridge);
    return ciderJoinBridge->hashBuildResultOrFuture(&future_);
  };
  context->setHashBuildTableSupplier(buildTableSupplier);

  batchProcessor_ =
      cider::processor::makeBatchProcessor(ciderPlanNode->getSubstraitPlan(), context);
}

}  // namespace facebook::velox::plugin
