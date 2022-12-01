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
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

bool CiderPipelineOperator::needsInput() const {
  return !noMoreInput_ && !input_;
}

void CiderPipelineOperator::addInput(RowVectorPtr input) {
  this->input_ = std::move(input);
  if (is_using_arrow_format_) {
    for (size_t i = 0; i < input_->childrenSize(); i++) {
      input_->childAt(i)->mutableRawNulls();
    }
    ArrowArray* inputArrowArray = CiderBatchUtils::allocateArrowArray();
    exportToArrow(input_, *inputArrowArray);
    ArrowSchema* inputArrowSchema = CiderBatchUtils::allocateArrowSchema();
    exportToArrow(input_, *inputArrowSchema);

    auto allocator = std::make_shared<PoolAllocator>(operatorCtx_->pool());
    auto inBatch =
        CiderBatchUtils::createCiderBatch(allocator, inputArrowSchema, inputArrowArray);
    batchProcessor_->processNextBatch(std::move(inBatch));
  } else {
    auto inBatch = dataConvertor_->convertToCider(
        input_, input_->size(), &convertorInternalCounter, operatorCtx_->pool());
    batchProcessor_->processNextBatch(std::make_shared<CiderBatch>(inBatch));
  }
}

facebook::velox::exec::BlockingReason CiderPipelineOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  auto batchProcessState = batchProcessor_->getState();
  if (cider::processor::BatchProcessorState::kWaitForJoinBuild == batchProcessState) {
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  return exec::BlockingReason::kNotBlocked;
}

bool CiderPipelineOperator::isFinished() {
  return finished_;
}

facebook::velox::RowVectorPtr CiderPipelineOperator::getOutput() {
  // TODO: need to refactor when integrate with StatefulProcessor
  if (!input_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }
  input_ = nullptr;

  // TODO: will be changed After refactor with arrow format
  auto output_ = batchProcessor_->getResult();

  if (is_using_arrow_format_) {
    ArrowSchema schema;
    ArrowArray array;
    output_->move(schema, array);
    VectorPtr baseVec = importFromArrowAsOwner(schema, array, operatorCtx_->pool());
    return std::reinterpret_pointer_cast<RowVector>(baseVec);
  }
  return dataConvertor_->convertToRowVector(
      *output_, *outputSchema_, operatorCtx_->pool());
}

CiderPipelineOperator::CiderPipelineOperator(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const CiderPlanNode>& ciderPlanNode)
    : CiderOperator(operatorId, driverCtx, ciderPlanNode) {
  auto context = std::make_shared<cider::processor::BatchProcessorContext>();
  batchProcessor_ =
      cider::processor::BatchProcessor::make(ciderPlanNode->getSubstraitPlan(), context);
}

}  // namespace facebook::velox::plugin
