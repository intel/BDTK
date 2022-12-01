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

#ifndef CIDER_BATCH_PROCESSOR_CONTEXT_H
#define CIDER_BATCH_PROCESSOR_CONTEXT_H

#include <memory>
#include "cider/CiderAllocator.h"

namespace cider::processor {

class BatchProcessorContext {
 public:
  BatchProcessorContext(const std::shared_ptr<CiderAllocator>& allocator)
      : allocator_(allocator){};

  std::shared_ptr<CiderAllocator> getAllocator() { return allocator_; }

 private:
  const std::shared_ptr<CiderAllocator> allocator_;
};

using BatchProcessorContextPtr = std::shared_ptr<BatchProcessorContext>;

}  // namespace cider::processor

#endif  // CIDER_BATCH_PROCESSOR_CONTEXT_H
