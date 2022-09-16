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

#include "CiderPlanPatterns.h"

namespace facebook::velox::plugin::plantransformer {
using namespace facebook::velox::core;

StatePtr CompoundStateMachine::Initial::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Filter>();
  } else if (auto projNode = std::dynamic_pointer_cast<const ProjectNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Project>();
  } else {
    return std::make_shared<CompoundStateMachine::NotAccept>();
  }
}

StatePtr CompoundStateMachine::Filter::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto projNode = std::dynamic_pointer_cast<const ProjectNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Project>();
  } else {
    return std::make_shared<CompoundStateMachine::NotAccept>();
  }
}

StatePtr CompoundStateMachine::Project::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto aggNode = std::dynamic_pointer_cast<const AggregationNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Aggregate>();
  } else {
    return std::make_shared<CompoundStateMachine::AcceptPrev>();
  }
}

bool CompoundStateMachine::accept(VeloxPlanNodeAddr nodeAddr) {
  StatePtr curState = getCurState();
  if (curState != nullptr) {
    curState = curState->accept(nodeAddr);
    setCurState(curState);
    if (auto notAcceptState = std::dynamic_pointer_cast<NotAccept>(curState)) {
      return false;
    } else if (auto AcceptPrevState = std::dynamic_pointer_cast<AcceptPrev>(curState)) {
      return true;
    } else {
      addToMatchResult(nodeAddr);
      return true;
    }
  } else {
    return false;
  }
}

StatePtr LeftDeepJoinStateMachine::Initial::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto joinNode = std::dynamic_pointer_cast<const AbstractJoinNode>(nodePtr)) {
    // Only accept one join node for now. change to return
    // std::make_shared<LeftJoin>() once velox-plugin is ready te accept multi
    // joins.
    return std::make_shared<OneJoin>();
    // return std::make_shared<LeftJoin>();
  } else {
    return std::make_shared<NotAccept>();
  }
}

StatePtr LeftDeepJoinStateMachine::LeftJoin::accept(VeloxPlanNodeAddr nodeAddr) {
  // encounter the sequence end
  if (StateMachine::endOfSequence().equal(nodeAddr)) {
    return std::make_shared<EndWithLeftJoin>();
  }
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto joinNode = std::dynamic_pointer_cast<const AbstractJoinNode>(nodePtr)) {
    return std::make_shared<LeftJoin>();
  } else if (auto crossJoinNode =
                 std::dynamic_pointer_cast<const CrossJoinNode>(nodePtr)) {
    return std::make_shared<LeftJoin>();
  } else {
    StatePtr init = std::make_shared<CompoundStateMachine::Initial>();
    return init->accept(nodeAddr);
  }
}

bool LeftDeepJoinStateMachine::accept(VeloxPlanNodeAddr nodeAddr) {
  StatePtr curState = getCurState();
  if (curState != nullptr) {
    curState = curState->accept(nodeAddr);
    setCurState(curState);
    if (auto notAcceptState = std::dynamic_pointer_cast<NotAccept>(curState)) {
      return false;
    } else if (auto notAcceptState =
                   std::dynamic_pointer_cast<CompoundStateMachine::NotAccept>(curState)) {
      return true;
    } else if (auto acceptPrev =
                   std::dynamic_pointer_cast<CompoundStateMachine::AcceptPrev>(
                       curState)) {
      return true;
    } else if (auto endWithLeftJoin =
                   std::dynamic_pointer_cast<EndWithLeftJoin>(curState)) {
      return true;
    } else {
      addToMatchResult(nodeAddr);
      return true;
    }
  } else {
    return false;
  }
}

VeloxPlanNodeAddrList LeftDeepJoinStateMachine::matchResult() {
  if (finish()) {
    if (auto compoundNotAcceptState =
            std::dynamic_pointer_cast<CompoundStateMachine::NotAccept>(getCurState())) {
      compoundMatchResult_ = {};
    }
    VeloxPlanNodeAddrList joinMatchResult = StateMachine::matchResult();
    joinMatchResult.insert(
        joinMatchResult.end(), compoundMatchResult_.begin(), compoundMatchResult_.end());
    return joinMatchResult;
  } else {
    return VeloxPlanNodeAddrList{};
  }
}

void LeftDeepJoinStateMachine::addToMatchResult(VeloxPlanNodeAddr nodeAddr) {
  if (inCompoundState()) {
    compoundMatchResult_.emplace_back(nodeAddr);
  } else {
    StateMachine::addToMatchResult(nodeAddr);
  }
}

void LeftDeepJoinStateMachine::clearMatchResult() {
  compoundMatchResult_ = {};
  StateMachine::clearMatchResult();
}

bool LeftDeepJoinStateMachine::inCompoundState() {
  if (auto compoundState =
          std::dynamic_pointer_cast<CompoundStateMachine::CompoundState>(getCurState())) {
    return true;
  }
  return false;
}

StatePtr LeftDeepJoinStateMachine::OneJoin::accept(VeloxPlanNodeAddr nodeAddr) {
  // encounter the sequence end
  if (StateMachine::endOfSequence().equal(nodeAddr)) {
    return std::make_shared<EndWithLeftJoin>();
  } else {
    StatePtr init = std::make_shared<CompoundStateMachine::Initial>();
    return init->accept(nodeAddr);
  }
}

StatePtr FilterStateMachine::Initial::accept(VeloxPlanNodeAddr nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(nodePtr)) {
    return std::make_shared<FilterStateMachine::Filter>();
  } else {
    return std::make_shared<FilterStateMachine::NotAccept>();
  }
}

bool FilterStateMachine::accept(VeloxPlanNodeAddr nodeAddr) {
  StatePtr curState = getCurState();
  if (curState != nullptr) {
    curState = curState->accept(nodeAddr);
    setCurState(curState);
    if (auto notAcceptState = std::dynamic_pointer_cast<NotAccept>(curState)) {
      return false;
    } else {
      addToMatchResult(nodeAddr);
      return true;
    }
  } else {
    return false;
  }
}
}  // namespace facebook::velox::plugin::plantransformer
