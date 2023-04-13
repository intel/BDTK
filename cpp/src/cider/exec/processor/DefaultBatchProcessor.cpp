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

#include <memory>

#include "cider/CiderException.h"
#include "cider/processor/BatchProcessor.h"
#include "exec/nextgen/context/CodegenContext.h"
#include "exec/plan/parser/SubstraitToRelAlgExecutionUnit.h"
#include "exec/processor/DefaultBatchProcessor.h"
#include "exec/processor/StatefulProcessor.h"
#include "exec/processor/StatelessProcessor.h"

namespace cider::exec::processor {

std::string getErrorMessageFromErrCode(const cider::jitlib::ERROR_CODE error_code) {
  switch (error_code) {
    case cider::jitlib::ERROR_CODE::ERR_DIV_BY_ZERO:
      return "Division by zero";
    case cider::jitlib::ERROR_CODE::ERR_OVERFLOW_OR_UNDERFLOW:
      return "Overflow or underflow";
    case cider::jitlib::ERROR_CODE::ERR_OUT_OF_TIME:
      return "Query execution has exceeded the time limit";
    case cider::jitlib::ERROR_CODE::ERR_INTERRUPTED:
      return "Query execution has been interrupted";
    case cider::jitlib::ERROR_CODE::ERR_SINGLE_VALUE_FOUND_MULTIPLE_VALUES:
      return "Multiple distinct values encountered";
    case cider::jitlib::ERROR_CODE::ERR_WIDTH_BUCKET_INVALID_ARGUMENT:
      return "Arguments of WIDTH_BUCKET function does not satisfy the condition";
    default:
      return "Cider Runtime Other error: code " + std::to_string(error_code);
  }
}

DefaultBatchProcessor::DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                                             const BatchProcessorContextPtr& context,
                                             const CodegenOptions& codegen_options)
    : context_(context) {
  initJoinHandler(plan);
  auto translator = generator::SubstraitToRelAlgExecutionUnit(plan->getPlan());
  RelAlgExecutionUnit ra_exe_unit = translator.createRelAlgExecutionUnit();
  codegen_context_ = nextgen::compile(ra_exe_unit, codegen_options);
  runtime_context_ = codegen_context_->generateRuntimeCTX(context_->getAllocator());
  query_func_ =
      codegen_context_->getJITFunction()->getFunctionPointer<int32_t, int8_t*, int8_t*>();
}

// This API use the precompiled codegen_ctx.
DefaultBatchProcessor::DefaultBatchProcessor(const plan::SubstraitPlanPtr& plan,
                                             const BatchProcessorContextPtr& context,
                                             const CodegenCtxPtr& codegen_ctx)
    : context_(context) {
  initJoinHandler(plan);
  // Copy CodegenContext cause JoinHandler will modify it.
  codegen_context_ = std::make_unique<CodegenContext>(*codegen_ctx);
  runtime_context_ = codegen_context_->generateRuntimeCTX(context_->getAllocator());
  query_func_ = reinterpret_cast<nextgen::QueryFunc>(
      codegen_context_->getJITFunction()
          ->getFunctionPointer<int32_t, int8_t*, int8_t*>());
}

// This API compile from substrait plan everytime.
DefaultBatchProcessor::DefaultBatchProcessor(
    const substrait::ExtendedExpression& extendedExpression,
    const BatchProcessorContextPtr& context,
    const CodegenOptions& codegen_options)
    : context_(context) {
  auto translator = std::make_shared<generator::SubstraitToRelAlgExecutionUnit>();
  auto executionUnit = translator->createRelAlgExecutionUnit(&extendedExpression);
  codegen_context_ = cider::exec::nextgen::compile(executionUnit, codegen_options);
  runtime_context_ = codegen_context_->generateRuntimeCTX(context_->getAllocator());
  query_func_ = reinterpret_cast<nextgen::QueryFunc>(
      codegen_context_->getJITFunction()
          ->getFunctionPointer<int32_t, int8_t*, int8_t*>());
}

void DefaultBatchProcessor::processNextBatch(const ArrowArray* array,
                                             const ArrowSchema* schema) {
  if (BatchProcessorState::kRunning != state_) {
    CIDER_THROW(CiderRuntimeException,
                "DefaultBatchProcessor::processNextBatch can only be called if state is "
                "kRunning.");
  }
  if (joinHandler_) {
    // this->inputBatch_ = joinHandler_->onProcessBatch(batch);
  } else {
    input_arrow_array_ = array;
    input_arrow_schema_ = schema;
  }

  runtime_context_->resetBatch(context_->getAllocator(), array, schema);
  int ret = query_func_((int8_t*)runtime_context_.get(), (int8_t*)array);
  if (ret != 0) {
    CIDER_THROW(CiderRuntimeException,
                getErrorMessageFromErrCode(static_cast<cider::jitlib::ERROR_CODE>(ret)));
  }
  runtime_context_->destroyStringHeap();
  has_result_ = true;

  if (!need_spill_) {
    if (input_arrow_array_->release) {
      input_arrow_array_->release(const_cast<struct ArrowArray*>(input_arrow_array_));
    }
    input_arrow_array_ = nullptr;

    if (input_arrow_schema_ && input_arrow_schema_->release) {
      input_arrow_schema_->release(const_cast<struct ArrowSchema*>(input_arrow_schema_));
      input_arrow_schema_ = nullptr;
    }
  }
}

BatchProcessorState DefaultBatchProcessor::getState() {
  if (joinHandler_) {
    joinHandler_->onState(state_);
  }
  return state_;
}

void DefaultBatchProcessor::finish() {
  no_more_batch_ = true;
  if (joinHandler_) {
    joinHandler_->onFinish();
  }
}

void DefaultBatchProcessor::feedHashBuildTable(
    const std::shared_ptr<JoinHashTable>& hashTable) {
  // switch state from waiting to running once hashTable is ready
  this->state_ = BatchProcessorState::kRunning;
  this->codegen_context_->setHashTable(hashTable);
  this->runtime_context_->instantiate(context_->getAllocator());
}

void DefaultBatchProcessor::feedCrossBuildData(std::shared_ptr<Batch>& crossData) {
  // switch state from waiting to running once cross build data is ready
  this->state_ = BatchProcessorState::kRunning;
  // TODO: feed cross build data into nextGen context
  codegen_context_->setBuildTable(crossData);
}

void DefaultBatchProcessor::initJoinHandler(const plan::SubstraitPlanPtr& plan) {
  if (plan->hasJoinRel()) {
    // TODO: currently we can't distinguish the joinRel is either a hashJoin rel
    // or a mergeJoin rel, just hard-code as HashJoinHandler for now and will refactor to
    // initialize joinHandler accordingly once the
    joinHandler_ = std::make_shared<HashProbeHandler>(shared_from_this());
    state_ = BatchProcessorState::kWaiting;
  } else if (plan->hasCrossRel()) {
    joinHandler_ = std::make_shared<CrossProbeHandler>(shared_from_this());
    state_ = BatchProcessorState::kWaiting;
  }
}

std::unique_ptr<BatchProcessor> BatchProcessor::Make(
    const substrait::Plan& plan,
    const BatchProcessorContextPtr& context,
    const CodegenOptions& codegen_options) {
  auto substraitPlan = std::make_shared<plan::SubstraitPlan>(plan);
  if (substraitPlan->hasAggregateRel()) {
    return std::make_unique<StatefulProcessor>(substraitPlan, context, codegen_options);
  } else {
    return std::make_unique<StatelessProcessor>(substraitPlan, context, codegen_options);
  }
}

std::unique_ptr<BatchProcessor> BatchProcessor::Make(
    const plan::SubstraitPlanPtr& plan,
    const BatchProcessorContextPtr& context,
    const CodegenCtxPtr& codegen_ctx) {
  if (plan->hasAggregateRel()) {
    return std::make_unique<StatefulProcessor>(plan, context, codegen_ctx);
  } else {
    return std::make_unique<StatelessProcessor>(plan, context, codegen_ctx);
  }
}
}  // namespace cider::exec::processor
