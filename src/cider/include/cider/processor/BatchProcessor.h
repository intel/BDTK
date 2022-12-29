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

#ifndef CIDER_BATCH_PROCESSOR_H
#define CIDER_BATCH_PROCESSOR_H

#include <memory>

#include "cider/processor/BatchProcessorContext.h"
#include "cider/processor/JoinHashTableBuilder.h"
#include "substrait/plan.pb.h"

struct ArrowArray;
struct ArrowSchema;

namespace cider::exec::processor {

enum class BatchProcessorState {
  kRunning,
  kWaiting,
  kFinished,
};

class BatchProcessor : public std::enable_shared_from_this<BatchProcessor> {
 public:
  enum class Type {
    kStateless,
    kStateful,
  };

  virtual const BatchProcessorContextPtr& getContext() const = 0;
  /// Adds an input batch to the batchProcessor.  This method will only be called if
  /// getState return kRunning.
  virtual void processNextBatch(const struct ArrowArray* array,
                                const struct ArrowSchema* schema = nullptr) = 0;

  /// Gets an output batch from the batchProcessor.  return null If no output data.
  virtual void getResult(struct ArrowArray& array, struct ArrowSchema& schema) = 0;

  /// Notifies the batchProcessor that no more batch will be added and the
  /// batchProcessor should finish processing and flush results.
  virtual void finish() = 0;

  virtual BatchProcessorState getState() = 0;

  virtual Type getProcessorType() const = 0;

  virtual void feedHashBuildTable(const std::shared_ptr<JoinHashTable>& hasTable) = 0;
};

using BatchProcessorPtr = std::shared_ptr<BatchProcessor>;

/// Factory method to create an instance of batchProcessor
std::unique_ptr<BatchProcessor> makeBatchProcessor(
    const ::substrait::Plan& plan,
    const BatchProcessorContextPtr& context);

inline std::ostream& operator<<(std::ostream& stream, const BatchProcessor::Type& type) {
  switch (type) {
    case BatchProcessor::Type::kStateless:
      stream << "Stateless";
      break;
    case BatchProcessor::Type::kStateful:
      stream << "Stateful";
      break;
    default:
      stream << "Unknown Processor Type";
  }
  return stream;
}

}  // namespace cider::exec::processor

#endif  // CIDER_BATCH_PROCESSOR_H
