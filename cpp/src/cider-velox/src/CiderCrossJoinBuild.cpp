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

#include "CiderCrossJoinBuild.h"
#include "Allocator.h"
#include "velox/exec/Task.h"

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#define CIDER_BATCH_PROCESSOR_CONTEXT_H
#include "velox/vector/arrow/Abi.h"
#endif

#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

void CiderCrossJoinBridge::setData(std::shared_ptr<Batch> data) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!data_.has_value(), "setData may be called only once");
    this->data_ = std::move(data);

    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<std::shared_ptr<Batch>> CiderCrossJoinBridge::hasDataOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!cancelled_, "Getting data after the build side is aborted");
  if (data_.has_value()) {
    return data_;
  }
  promises_.emplace_back("CiderCrossJoinBridge::getCrossBuildData");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

CiderCrossJoinBuild::CiderCrossJoinBuild(int32_t operatorId,
                                         exec::DriverCtx* driverCtx,
                                         std::shared_ptr<const CiderPlanNode> joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "CiderCrossJoinBuild") {
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  joinBridge_ = std::dynamic_pointer_cast<CiderCrossJoinBridge>(joinBridge);
}

void CiderCrossJoinBuild::addInput(RowVectorPtr input) {
  if (input->size() > 0) {
    // Load lazy vectors before storing.
    for (auto& child : input->children()) {
      child->loadedVector();
    }
    data_.emplace_back(std::move(input));
  }
}

void CiderCrossJoinBuild::noMoreInput() {
  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // The last Driver to hit CrossJoinBuild::finish gathers the data from
  // all build Drivers and hands it over to the probe side. At this
  // point all build Drivers are continued and will free their
  // state. allPeersFinished is true only for the last Driver of the
  // build pipeline.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }

  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* build = dynamic_cast<CiderCrossJoinBuild*>(op);
    VELOX_CHECK(build);
    data_.insert(data_.begin(), build->data_.begin(), build->data_.end());
  }

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier and finish.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }

  // Convert data_ to Batch.
  auto rowVectorPtr = RowVector::createEmpty(data_.front()->type(), operatorCtx_->pool());
  for (auto batch : data_) {
    rowVectorPtr->append(batch.get());
  }

  ArrowArray inputArrowArray;
  exportToArrow(rowVectorPtr, inputArrowArray);
  ArrowSchema inputArrowSchema;
  exportToArrow(rowVectorPtr, inputArrowSchema);

  Batch inBatch(inputArrowSchema, inputArrowArray);
  joinBridge_->setData(std::make_shared<Batch>(std::move(inBatch)));
}

exec::BlockingReason CiderCrossJoinBuild::isBlocked(
    facebook::velox::ContinueFuture* future) {
  if (!future_.valid()) {
    return exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return exec::BlockingReason::kWaitForJoinBuild;
}

bool CiderCrossJoinBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

}  // namespace facebook::velox::plugin
