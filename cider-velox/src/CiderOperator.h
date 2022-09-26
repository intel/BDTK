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

#include <memory>

#include "velox/exec/Operator.h"

#include "CiderPlanNode.h"
#include "DataConvertor.h"
#include "cider/CiderInterface.h"
#include "cider/CiderRuntimeModule.h"

namespace facebook::velox::plugin {

class CiderOperator : public exec::Operator {
 public:
  static std::unique_ptr<CiderOperator> Make(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const CiderPlanNode>& ciderPlanNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override;

 protected:
  CiderOperator(int32_t operatorId,
                exec::DriverCtx* driverCtx,
                const std::shared_ptr<const CiderPlanNode>& ciderPlanNode);

  const std::shared_ptr<const CiderPlanNode> planNode_;
  std::shared_ptr<CiderRuntimeModule> ciderRuntimeModule_;
  std::shared_ptr<CiderCompileModule> ciderCompileModule_;
  std::shared_ptr<CiderTableSchema> outputSchema_;
  std::shared_ptr<DataConvertor> dataConvertor_;
  std::chrono::microseconds convertorInternalCounter;

  // these properties are used for join with w/o agg case
  std::optional<std::vector<RowVectorPtr>> buildData_;
  bool buildSideEmpty_{false};
  bool buildTableFed_{false};
  bool finished_{false};
  bool is_using_arrow_format_;

  // TODO: will be changed After refactor with arrow format
  // In order to preserve the lifecycle of unique_ptr<>
  std::unique_ptr<CiderBatch> output_;
};

}  // namespace facebook::velox::plugin
