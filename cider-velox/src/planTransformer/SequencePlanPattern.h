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

#pragma once

#include "PlanPattern.h"

namespace facebook::velox::plugin::plantransformer {
class State;
using StatePtr = std::shared_ptr<State>;
class State {
 public:
  State() {}
  virtual StatePtr accept(VeloxPlanNodeAddr nodeAddr) { return nullptr; }
  virtual bool isFinal() { return false; }
};

class StateMachine {
 public:
  virtual bool accept(VeloxPlanNodeAddr nodePtr) = 0;
  bool finish() { return getCurState()->isFinal(); }
  virtual void setInitState() = 0;
  void reset() {
    setInitState();
    clearMatchResult();
  }
  void setCurState(StatePtr state) { curStatePtr_ = state; }
  StatePtr getCurState() { return curStatePtr_; }
  virtual VeloxPlanNodeAddrList matchResult() {
    if (finish()) {
      return matchResult_;
    } else {
      return VeloxPlanNodeAddrList{};
    }
  }
  virtual void addToMatchResult(VeloxPlanNodeAddr nodeAddr) {
    matchResult_.emplace_back(nodeAddr);
  }
  virtual void clearMatchResult() { matchResult_ = {}; }
  // define a special type of node as end of the sequence.
  static VeloxPlanNodeAddr endOfSequence() { return VeloxPlanNodeAddr::invalid(); }

 private:
  StatePtr curStatePtr_;
  VeloxPlanNodeAddrList matchResult_;
};

class SequencePlanPattern : public PlanPattern {
 public:
  std::pair<bool, VeloxNodeAddrPlanSection> matchFromSrc(
      BranchSrcToTargetIterator branchIte) const;
  void setStateMachine(std::shared_ptr<StateMachine> stateMachine) {
    stateMachine_ = stateMachine;
  }

 private:
  std::shared_ptr<StateMachine> stateMachine_ = nullptr;
};

}  // namespace facebook::velox::plugin::plantransformer
