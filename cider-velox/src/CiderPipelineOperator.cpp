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
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

bool CiderPipelineOperator::needsInput() const {
  return !noMoreInput_ && !input_;
}

void CiderPipelineOperator::addInput(facebook::velox::RowVectorPtr input) {
  this->input_ = std::move(input);
  auto inBatch = dataConvertor_->convertToCider(
      input_, input_->size(), &convertorInternalCounter, operatorCtx_->pool());
  ciderRuntimeModule_->processNextBatch(inBatch);
}

facebook::velox::exec::BlockingReason CiderPipelineOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  return CiderOperator::isBlocked(future);
}

bool CiderPipelineOperator::isFinished() {
  return finished_;
}

facebook::velox::RowVectorPtr CiderPipelineOperator::getOutput() {
  if (!input_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }
  input_ = nullptr;

  // TODO: will be changed After refactor with arrow format
  // In order to preserve the lifecycle of unique_ptr<>
  CiderRuntimeModule::ReturnCode ret;
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
  const auto& substraitPlan = ciderPlanNode->getSubstraitPlan();
  batchProcessor_ = cider::processor::BatchProcessor::make(substraitPlan, context);
}

}  // namespace facebook::velox::plugin
