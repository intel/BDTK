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

#ifndef CIDER_JOINHANDLER_H
#define CIDER_JOINHANDLER_H

#include <memory>
#include "cider/CiderBatch.h"
#include "cider/processor/BatchProcessor.h"

namespace cider::exec::processor {

class JoinHandler {
 public:
  virtual ~JoinHandler() = default;

  virtual std::shared_ptr<CiderBatch> onProcessBatch(
      std::shared_ptr<CiderBatch> batch) = 0;

  virtual void onState(BatchProcessorState state) = 0;

  virtual void onFinish() {}
};

using JoinHandlerPtr = std::shared_ptr<JoinHandler>;

class HashProbeHandler : public JoinHandler {
 public:
  explicit HashProbeHandler(const BatchProcessorPtr& batchProcessor)
      : batchProcessor_(batchProcessor) {}

  std::shared_ptr<CiderBatch> onProcessBatch(std::shared_ptr<CiderBatch> batch) override;

  void onState(BatchProcessorState state) override;

 private:
  BatchProcessorPtr batchProcessor_;
};

}  // namespace cider::exec::processor

#endif  // CIDER_JOINHANDLER_H
