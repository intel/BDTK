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

#include "JoinHandler.h"

namespace cider::exec::processor {

void HashProbeHandler::onState(BatchProcessorState state) {
  if (BatchProcessorState::kWaiting == state) {
    const auto& hashBuildTableSupplier =
        batchProcessor_->getContext()->getHashBuildTableSupplier();
    if (hashBuildTableSupplier) {
      auto hashBuildResult = hashBuildTableSupplier();
      if (hashBuildResult.has_value()) {
        batchProcessor_->feedHashBuildTable(hashBuildResult.value().table);
      }
    }
  }
}

void CrossProbeHandler::onState(cider::exec::processor::BatchProcessorState state) {
  if (BatchProcessorState::kWaiting == state) {
    const auto& crossBuildTableSupplier =
        batchProcessor_->getContext()->getCrossJoinBuildTableSupplier();
    if (crossBuildTableSupplier) {
      auto crossBuildData = crossBuildTableSupplier();
      if (crossBuildData.has_value()) {
        batchProcessor_->feedCrossBuildData(crossBuildData.value());
      }
    }
  }
}
}  // namespace cider::exec::processor
