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

#include "CiderHashJoinBuild.h"
#include "Allocator.h"
#include "velox/exec/Task.h"

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#include "velox/vector/arrow/Abi.h"
#endif

#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::plugin {

void CiderHashJoinBridge::setHashTable(std::unique_ptr<CiderHashJoinTable> table) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(table, "setHashTable may be called only once");
    this->buildResult_ = CiderHashBuildResult(std::move(table));
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<CiderHashBuildResult> CiderHashJoinBridge::hashBuildResultOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!cancelled_, "Getting data after the build side is aborted");
  if (buildResult_.has_value()) {
    return buildResult_;
  }
  promises_.emplace_back("CiderHashJoinBridge::getHashBuildResult");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

CiderHashJoinBuild::CiderHashJoinBuild(int32_t operatorId,
                                       exec::DriverCtx* driverCtx,
                                       std::shared_ptr<const CiderPlanNode> joinNode)
    : Operator(driverCtx, nullptr, operatorId, joinNode->id(), "CiderHashJoinBuild")
    , allocator_(std::make_shared<PoolAllocator>(operatorCtx_->pool())) {
  const auto& joinRel = joinNode->getSubstraitPlan().relations(0).root().input().join();
  auto context = std::make_shared<CiderJoinHashTableBuildContext>(allocator_);
  joinHashTableBuilder_ =
      cider::exec::processor::makeJoinHashTableBuilder(joinRel, context);
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  joinBridge_ = std::dynamic_pointer_cast<CiderHashJoinBridge>(joinBridge);
}

void CiderHashJoinBuild::addInput(RowVectorPtr input) {
  for (size_t i = 0; i < input->childrenSize(); i++) {
    input->childAt(i)->mutableRawNulls();
  }
  ArrowArray inputArrowArray;
  exportToArrow(input_, inputArrowArray);
  ArrowSchema inputArrowSchema;
  exportToArrow(input_, inputArrowSchema);

  cider::exec::nextgen::context::Batch inBatch(inputArrowSchema, inputArrowArray);
  joinHashTableBuilder_->appendBatch(
      std::make_shared<cider::exec::nextgen::context::Batch>(inBatch));
}

void CiderHashJoinBuild::noMoreInput() {
  if (noMoreInput_) {
    return;
  }
  Operator::noMoreInput();
  noMoreInputInternal();
}

exec::BlockingReason CiderHashJoinBuild::isBlocked(ContinueFuture* future) {
  if (!future_.valid()) {
    return exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return exec::BlockingReason::kWaitForJoinBuild;
}

bool CiderHashJoinBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

void CiderHashJoinBuild::noMoreInputInternal() {
  if (!finishHashBuild()) {
    return;
  }
  postHashBuildProcess();
}

bool CiderHashJoinBuild::finishHashBuild() {
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // The last Driver to hit HashBuild::finish gathers the data from
  // all build Drivers and hands it over to the probe side. At this
  // point all build Drivers are continued and will free their
  // state. allPeersFinished is true only for the last Driver of the
  // build pipeline.
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return false;
  }

  std::vector<std::unique_ptr<CiderHashJoinTable>> otherTables;
  otherTables.reserve(peers.size());
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    CiderHashJoinBuild* build = dynamic_cast<CiderHashJoinBuild*>(op);
    VELOX_CHECK(build);
    otherTables.push_back(std::move(build->joinHashTableBuilder_->build()));
  }
  // merge other tables into one table
  auto joinTable = joinHashTableBuilder_->build();
  joinTable->merge_other_hashtables(otherTables);

  joinBridge_->setHashTable(std::move(joinTable));

  // Realize the promises so that the other Drivers (which were not
  // the last to finish) can continue from the barrier and finish.
  peers.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }
  return true;
}

void CiderHashJoinBuild::postHashBuildProcess() {
  // Release the unused memory reservation since we have finished the table
  // build.
  // FIXME!!
  // operatorCtx_->mappedMemory()->tracker()->release();
}

}  // namespace facebook::velox::plugin
