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

#include "SequencePlanPattern.h"

namespace facebook::velox::plugin::plantransformer {
std::pair<bool, VeloxNodeAddrPlanSection> SequencePlanPattern::matchFromSrc(
    BranchSrcToTargetIterator& branchIte) const {
  while (branchIte.hasNext()) {
    auto nodeAddr = branchIte.next();
    bool isAccept = stateMachine_->accept(nodeAddr);
    if (!isAccept) {
      stateMachine_->reset();
      return std::pair<bool, VeloxNodeAddrPlanSection>(false, VeloxNodeAddrPlanSection());
    } else {
      if (stateMachine_->finish()) {
        break;
      }
    }
  }
  if (!stateMachine_->finish()) {
    bool acceptSequeceEnd = stateMachine_->accept(StateMachine::endOfSequence());
    if (!acceptSequeceEnd) {
      stateMachine_->reset();
      return std::pair<bool, VeloxNodeAddrPlanSection>(false, VeloxNodeAddrPlanSection());
    }
  }
  if (stateMachine_->finish()) {
    VeloxPlanNodeAddrList matchedNodes = stateMachine_->matchResult();
    VeloxNodeAddrPlanSection planSection{matchedNodes[matchedNodes.size() - 1],
                                         matchedNodes[0]};
    stateMachine_->reset();
    return std::pair<bool, VeloxNodeAddrPlanSection>(true, planSection);
  } else {
    stateMachine_->reset();
    return std::pair<bool, VeloxNodeAddrPlanSection>(false, VeloxNodeAddrPlanSection());
  }
}
}  // namespace facebook::velox::plugin::plantransformer
