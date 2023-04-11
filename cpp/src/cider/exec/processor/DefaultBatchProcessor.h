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

#ifndef CIDER_DEFAULT_BATCH_PROCESSOR_H
#define CIDER_DEFAULT_BATCH_PROCESSOR_H

#include "cider/processor/BatchProcessor.h"
#include "exec/nextgen/Nextgen.h"
#include "exec/plan/substrait/SubstraitPlan.h"
#include "exec/processor/JoinHandler.h"
#include "substrait/extended_expression.pb.h"

namespace cider::exec::processor {

class DefaultBatchProcessor : public BatchProcessor {
 public:
  DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                        const BatchProcessorContextPtr& context,
                        const CodegenOptions& codegen_options = {});

  DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                        const BatchProcessorContextPtr& context,
                        const CodegenCtxPtr& codegen_ctx);

  DefaultBatchProcessor(const substrait::ExtendedExpression& extendedExpression,
                        const BatchProcessorContextPtr& context,
                        const CodegenOptions& codegen_options = {});

  ~DefaultBatchProcessor() override = default;

  const BatchProcessorContextPtr& getContext() const override { return context_; }

  void processNextBatch(const struct ArrowArray* array,
                        const struct ArrowSchema* schema = nullptr) override;

  void finish() override;

  BatchProcessorState getState() override;

  void feedHashBuildTable(const std::shared_ptr<JoinHashTable>& hashTable) override;

  void feedCrossBuildData(std::shared_ptr<Batch>& crossData) override;

 protected:
  BatchProcessorContextPtr context_;

  BatchProcessorState state_{BatchProcessorState::kRunning};

  const struct ArrowArray* input_arrow_array_{nullptr};
  const struct ArrowSchema* input_arrow_schema_{nullptr};

  struct ArrowSchema* output_arrow_schema_{nullptr};

  bool no_more_batch_{false};

  bool has_result_{false};

  bool need_spill_{false};

  JoinHandlerPtr joinHandler_;

  nextgen::context::CodegenCtxPtr codegen_context_;
  nextgen::context::RuntimeCtxPtr runtime_context_;
  nextgen::QueryFunc query_func_;

 private:
  void initJoinHandler(const plan::SubstraitPlanPtr& plan);
};

}  // namespace cider::exec::processor

#endif  // CIDER_DEFAULT_BATCH_PROCESSOR_H
