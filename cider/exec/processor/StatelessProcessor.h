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

#ifndef CIDER_STATELESS_PROCESSOR_H
#define CIDER_STATELESS_PROCESSOR_H

#include "cider/processor/BatchProcessor.h"

namespace cider::processor {

class StatelessProcessor : public BatchProcessor {
 public:
  StatelessProcessor(const ::substrait::Plan* plan,
                     const BatchProcessorContextPtr& context);

  void processNextBatch(std::shared_ptr<CiderBatch> batch) override;

  std::shared_ptr<CiderBatch> getResult() override;

  BatchProcessorState getState() override;
};

}  // namespace cider::processor

#endif  // CIDER_STATELESS_PROCESSOR_H
