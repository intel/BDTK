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

#pragma once

#include "CiderPlanNode.h"
#include "cider/processor/BatchProcessorContext.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::plugin {

// Hands over all batches from a multi-threads build pipeline to a
// multi-threads probe pipeline.
class CiderCrossJoinBridge : public exec::JoinBridge {
 public:
  void setData(std::shared_ptr<Batch> data);

  std::optional<std::shared_ptr<Batch>> hasDataOrFuture(ContinueFuture* future);

 private:
  std::optional<std::shared_ptr<Batch>> data_;
};

class CiderCrossJoinBuild : public exec::Operator {
 public:
  CiderCrossJoinBuild(int32_t operatorId,
                      exec::DriverCtx* driverCtx,
                      std::shared_ptr<const CiderPlanNode> joinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override { return nullptr; }

  bool needsInput() const override { return !noMoreInput_; }

  void noMoreInput() override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override {
    data_.clear();
    Operator::close();
  }

 private:
  std::vector<VectorPtr> data_;

  // Future for synchronizing with other Drivers of the same pipeline. All build
  // Drivers must be completed before making data available for the probe side.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  std::shared_ptr<CiderCrossJoinBridge> joinBridge_;
};

}  // namespace facebook::velox::plugin
