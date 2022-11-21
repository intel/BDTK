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

namespace {

using VeloxPlanNodePtr = std::shared_ptr<const facebook::velox::core::PlanNode>;

template <typename T>
bool isPartialNode(VeloxPlanNodePtr nodePtr) {
  if (auto node = std::dynamic_pointer_cast<const T>(nodePtr)) {
    return node->isPartial();
  } else {
    return false;
  }
}
}  // namespace

namespace facebook::velox::plugin::plantransformer {
using namespace facebook::velox::core;

StatePtr CompoundStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Filter>();
  } else if (auto projNode = std::dynamic_pointer_cast<const ProjectNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Project>();
  } else {
    return std::make_shared<CompoundStateMachine::NotAccept>();
  }
}

StatePtr CompoundStateMachine::Filter::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto projNode = std::dynamic_pointer_cast<const ProjectNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Project>();
  } else {
    return std::make_shared<CompoundStateMachine::NotAccept>();
  }
}

StatePtr CompoundStateMachine::Project::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto aggNode = std::dynamic_pointer_cast<const AggregationNode>(nodePtr)) {
    return std::make_shared<CompoundStateMachine::Aggregate>();
  } else if (isPartialNode<TopNNode>(nodePtr)) {
    // (filter) + project + partial topN pattern.
    // TODO: (Jie) enable this after TopN Node supported by cider-velox and cider.
    // Now we just return  AcceptPrev to workaround.
    // return std::make_shared<CompoundStateMachine::TopN>();
    return std::make_shared<CompoundStateMachine::AcceptPrev>();
  } else {
    return std::make_shared<CompoundStateMachine::AcceptPrev>();
  }
}

bool CompoundStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

StatePtr LeftDeepJoinStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

StatePtr LeftDeepJoinStateMachine::LeftJoin::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

bool LeftDeepJoinStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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
    } else if (auto notAcceptState =
                   std::dynamic_pointer_cast<FilterStateMachine::NotAccept>(curState)) {
      return true;
    } else if (auto notAcceptState =
                   std::dynamic_pointer_cast<PartialAggStateMachine::NotAccept>(
                       curState)) {
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

    if (auto aggNotAcceptState =
            std::dynamic_pointer_cast<PartialAggStateMachine::NotAccept>(getCurState())) {
      partialAggMatchResult_ = {};
    }

    if (auto filterNotAcceptState =
            std::dynamic_pointer_cast<FilterStateMachine::NotAccept>(getCurState())) {
      filterMatchResult_ = {};
    }

    VeloxPlanNodeAddrList joinMatchResult = StateMachine::matchResult();
    joinMatchResult.insert(
        joinMatchResult.end(), compoundMatchResult_.begin(), compoundMatchResult_.end());

    joinMatchResult.insert(joinMatchResult.end(),
                           partialAggMatchResult_.begin(),
                           partialAggMatchResult_.end());

    joinMatchResult.insert(
        joinMatchResult.end(), filterMatchResult_.begin(), filterMatchResult_.end());

    return joinMatchResult;
  } else {
    return VeloxPlanNodeAddrList{};
  }
}

void LeftDeepJoinStateMachine::addToMatchResult(const VeloxPlanNodeAddr& nodeAddr) {
  if (inCompoundState()) {
    compoundMatchResult_.emplace_back(nodeAddr);
  } else if (inPartialAggState()) {
    partialAggMatchResult_.emplace_back(nodeAddr);
  } else if (inFilterState()) {
    filterMatchResult_.emplace_back(nodeAddr);
  } else {
    StateMachine::addToMatchResult(nodeAddr);
  }
}

void LeftDeepJoinStateMachine::clearMatchResult() {
  compoundMatchResult_ = {};
  partialAggMatchResult_ = {};
  filterMatchResult_ = {};
  StateMachine::clearMatchResult();
}

bool LeftDeepJoinStateMachine::inCompoundState() {
  if (auto compoundState =
          std::dynamic_pointer_cast<CompoundStateMachine::CompoundState>(getCurState())) {
    return true;
  }
  return false;
}

bool LeftDeepJoinStateMachine::inFilterState() {
  if (auto filterState =
          std::dynamic_pointer_cast<FilterStateMachine::Filter>(getCurState())) {
    return true;
  }
  return false;
}

bool LeftDeepJoinStateMachine::inPartialAggState() {
  if (auto partialAggState =
          std::dynamic_pointer_cast<PartialAggStateMachine::PartialAgg>(getCurState())) {
    return true;
  }
  return false;
}

StatePtr LeftDeepJoinStateMachine::OneJoin::accept(const VeloxPlanNodeAddr& nodeAddr) {
  // encounter the sequence end
  if (StateMachine::endOfSequence().equal(nodeAddr)) {
    return std::make_shared<EndWithLeftJoin>();
  } else if (auto aggNode =
                 std::dynamic_pointer_cast<const AggregationNode>(nodeAddr.nodePtr)) {
    return std::make_shared<PartialAggStateMachine::Initial>()->accept(nodeAddr);
  } else if (auto filterNode =
                 std::dynamic_pointer_cast<const FilterNode>(nodeAddr.nodePtr)) {
    return std::make_shared<FilterStateMachine::Initial>()->accept(nodeAddr);
  } else {
    return std::make_shared<CompoundStateMachine::Initial>()->accept(nodeAddr);
  }
}

StatePtr FilterStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;
  if (auto filterNode = std::dynamic_pointer_cast<const FilterNode>(nodePtr)) {
    return std::make_shared<FilterStateMachine::Filter>();
  } else {
    return std::make_shared<FilterStateMachine::NotAccept>();
  }
}

bool FilterStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

StatePtr PartialAggStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;

  if (auto aggNode = std::dynamic_pointer_cast<const AggregationNode>(nodePtr)) {
    if (aggNode->step() == AggregationNode::Step::kPartial) {
      return std::make_shared<PartialAggStateMachine::PartialAgg>();
    }
  }

  return std::make_shared<PartialAggStateMachine::NotAccept>();
}

bool PartialAggStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

StatePtr OrderByStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;

  if (isPartialNode<OrderByNode>(nodePtr)) {
    return std::make_shared<OrderByStateMachine::OrderBy>();
  }
  return std::make_shared<OrderByStateMachine::NotAccept>();
}

bool OrderByStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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

StatePtr TopNStateMachine::Initial::accept(const VeloxPlanNodeAddr& nodeAddr) {
  VeloxPlanNodePtr nodePtr = nodeAddr.nodePtr;

  if (isPartialNode<TopNNode>(nodePtr)) {
    return std::make_shared<TopNStateMachine::TopN>();
  }

  return std::make_shared<TopNStateMachine::NotAccept>();
}

bool TopNStateMachine::accept(const VeloxPlanNodeAddr& nodeAddr) {
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
