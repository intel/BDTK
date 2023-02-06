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

#include "cider/processor/ExpressionEvaluator.h"
#include "exec/processor/StatelessProcessor.h"

namespace cider::exec::processor {

ExprEvaluatorContext::ExprEvaluatorContext(std::shared_ptr<CiderAllocator> allocator)
    : allocator_(allocator) {}

ExpressionEvaluator::ExpressionEvaluator(
    const substrait::ExtendedExpression& extendedExpression,
    const std::shared_ptr<ExprEvaluatorContext>& context) {
  const auto batchProcessorContext =
      std::make_shared<BatchProcessorContext>(context->getAllocator());
  auto batchProcessor =
      std::make_shared<StatelessProcessor>(extendedExpression, batchProcessorContext);
}

std::unique_ptr<Batch> ExpressionEvaluator::evaluate(
    const std::shared_ptr<Batch>& inBatch) {
  batchProcessor_->processNextBatch(inBatch->getArray(), inBatch->getSchema());
  struct ArrowArray array;
  struct ArrowSchema schema;
  batchProcessor_->getResult(array, schema);
  return std::make_unique<Batch>(schema, array);
}

ExpressionEvaluator::~ExpressionEvaluator() {
  batchProcessor_->finish();
}

}  // namespace cider::exec::processor
