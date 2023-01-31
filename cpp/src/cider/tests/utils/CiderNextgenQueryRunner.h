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

#ifndef CIDER_TESTS_UTILS_NEXTGEN_QUERY_RUNNER_H_
#define CIDER_TESTS_UTILS_NEXTGEN_QUERY_RUNNER_H_

#include <memory>
#include <string>

#include "cider/CiderAllocator.h"
#include "cider/processor/BatchProcessor.h"
#include "substrait/plan.pb.h"

namespace cider::test::util {

class CiderNextgenQueryRunner {
 public:
  virtual ~CiderNextgenQueryRunner() = default;

  CiderNextgenQueryRunner() {
    allocator_ = std::make_shared<CiderDefaultAllocator>();
    context_ =
        std::make_shared<cider::exec::processor::BatchProcessorContext>(allocator_);
  }

  void prepare(const std::string& create_ddl) { create_ddl_ = create_ddl; }

  ::substrait::Plan genSubstraitPlan(const std::string& file_or_sql);

  const std::shared_ptr<CiderAllocator>& getAllocator() const { return allocator_; }

  virtual void runQueryOneBatch(
      const std::string& file_or_sql,
      const struct ArrowArray& input_array,
      const struct ArrowSchema& input_schema,
      struct ArrowArray& output_array,
      struct ArrowSchema& output_schema,
      const cider::exec::nextgen::context::CodegenOptions& codegen_options = {});

 protected:
  std::shared_ptr<CiderAllocator> allocator_;
  exec::processor::BatchProcessorContextPtr context_;
  std::unique_ptr<exec::processor::BatchProcessor> processor_;
  std::string create_ddl_;
};

using CiderNextgenQueryRunnerPtr = std::shared_ptr<CiderNextgenQueryRunner>;

}  // namespace cider::test::util

#endif  // CIDER_TESTS_UTILS_NEXTGEN_QUERY_RUNNER_H_
