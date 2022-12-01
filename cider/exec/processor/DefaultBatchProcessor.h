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

#ifndef CIDER_DEFAULT_BATCH_PROCESSOR_H
#define CIDER_DEFAULT_BATCH_PROCESSOR_H

#include "JoinHandler.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/module/batch/ArrowABI.h"
#include "exec/plan/substrait/SubstraitPlan.h"

namespace cider::processor {

class DefaultBatchProcessor : public BatchProcessor,
                              std::enable_shared_from_this<DefaultBatchProcessor> {
 public:
  virtual ~DefaultBatchProcessor() = default;

  BatchProcessorContextPtr context() override { return context_; }

  void processNextBatch(std::shared_ptr<CiderBatch> batch) override;

  void finish() override;

  BatchProcessorState getState() override;

  void feedHashBuildTable(const std::shared_ptr<JoinHashTable>& hashTable) override;

 protected:
  DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                        const BatchProcessorContextPtr& context);

  const plan::SubstraitPlanPtr plan_;

  const BatchProcessorContextPtr context_;

  BatchProcessorState state_{BatchProcessorState::kRunning};

  ArrowArray* inputBatch_;

  bool noMoreBatch_{false};

  JoinHandlerPtr joinHandler_;
};

}  // namespace cider::processor

#endif  // CIDER_DEFAULT_BATCH_PROCESSOR_H
