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

#include "CiderStatelessOperator.h"
#include "cider/CiderCompileModule.h"
#include "cider/CiderRuntimeModule.h"

namespace facebook::velox::plugin {

RowVectorPtr CiderStatelessOperator::getOutput() {
  if (!input_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }
  input_ = nullptr;

  // TODO: will be changed After refactor with arrow format
  // In order to preserve the lifecycle of unique_ptr<>
  CiderRuntimeModule::ReturnCode ret;
  std::tie(ret, output_) = ciderRuntimeModule_->fetchResults();
  if (UNLIKELY(output_->row_num() == 0)) {
    return nullptr;
  } else {
    return dataConvertor_->convertToRowVector(
        *output_, *outputSchema_, operatorCtx_->pool());
  }
}

}  // namespace facebook::velox::plugin
