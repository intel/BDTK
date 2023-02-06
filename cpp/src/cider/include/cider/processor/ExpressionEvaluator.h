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

#ifndef CIDER_EXPRESSION_EVALUATOR_H
#define CIDER_EXPRESSION_EVALUATOR_H

#include <memory>
#include "cider/processor/BatchProcessor.h"
#include "exec/nextgen/context/Batch.h"
#include "substrait/extended_expression.pb.h"

namespace cider::exec::processor {
using Batch = cider::exec::nextgen::context::Batch;

class ExprEvaluatorContext {
 public:
  explicit ExprEvaluatorContext(std::shared_ptr<CiderAllocator> allocator);

  std::shared_ptr<CiderAllocator>& getAllocator() { return allocator_; }

 private:
  std::shared_ptr<CiderAllocator> allocator_;
};

class ExpressionEvaluator {
 public:
  ExpressionEvaluator(const substrait::ExtendedExpression& extendedExpression,
                      const std::shared_ptr<ExprEvaluatorContext>& context);

  ~ExpressionEvaluator();

  std::unique_ptr<Batch> evaluate(const std::shared_ptr<Batch>& inBatch);

 private:
  std::unique_ptr<BatchProcessor> batchProcessor_;
};

}  // namespace cider::exec::processor

#endif  // CIDER_EXPRESSION_EVALUATOR_H
