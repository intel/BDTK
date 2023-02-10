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

#include "CiderOperator.h"
#include "cider/processor/BatchProcessor.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::plugin {

class CiderPipelineOperator : public exec::Operator {
 public:
  CiderPipelineOperator(int32_t operatorId,
                        exec::DriverCtx* driverCtx,
                        const std::shared_ptr<const CiderPlanNode>& ciderPlanNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override;

 private:
  cider::exec::processor::BatchProcessorPtr batchProcessor_;

  bool finished_{false};

  const std::shared_ptr<const CiderPlanNode> ciderPlanNode_;

  // Future for synchronizing with other Drivers of the same pipeline.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  const std::shared_ptr<CiderAllocator> allocator_;
};

}  // namespace facebook::velox::plugin
