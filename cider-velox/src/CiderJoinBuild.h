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

#pragma once

#include "CiderPlanNode.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::plugin {

// Hands over all batches from a multi-threaded build pipeline to a
// multi-threaded probe pipeline.
class CiderJoinBridge : public exec::JoinBridge {
 public:
  void setData(std::vector<RowVectorPtr> data);

  std::optional<std::vector<RowVectorPtr>> dataOrFuture(ContinueFuture* future);

 private:
  std::optional<std::vector<RowVectorPtr>> data_;
};

class CiderJoinBuild : public exec::Operator {
 public:
  CiderJoinBuild(int32_t operatorId,
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
  // Future for synchronizing with other Drivers of the same pipeline. All build
  // Drivers must be completed before making the hash table.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  std::vector<RowVectorPtr> data_;
};

}  // namespace facebook::velox::plugin
