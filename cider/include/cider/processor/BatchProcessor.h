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

#include "BatchProcessorContext.h"
#include "cider/CiderBatch.h"
#include "substrait/plan.pb.h"

namespace cider::processor {

enum BatchProcessorState {
  kRunning,
  kWaiting,
  kFinished,
};

class BatchProcessor {
 public:
  ~BatchProcessor() = default;

  BatchProcessorContextPtr getContext() const { return context_; }

  virtual void processNextBatch(std::shared_ptr<CiderBatch> batch);

  virtual std::shared_ptr<CiderBatch> getResult();

  virtual void finish(){};

  virtual BatchProcessorState getState();

  virtual bool isFinished() { return kFinished == state_; }

  static std::shared_ptr<BatchProcessor> make(const substrait::Plan& plan,
      const BatchProcessorContextPtr& context);

 protected:
  BatchProcessor(const ::substrait::Plan* plan, const BatchProcessorContextPtr& context);

  const ::substrait::Plan* plan_;

  const BatchProcessorContextPtr context_;

  BatchProcessorState state_{kRunning};

  std::shared_ptr<CiderBatch> inputBatch_;
};

using BatchProcessorPtr = std::shared_ptr<BatchProcessor>;

}  // namespace cider::processor
