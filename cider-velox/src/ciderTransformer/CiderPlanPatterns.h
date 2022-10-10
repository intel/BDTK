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

#include "../planTransformer/PlanPattern.h"
#include "../planTransformer/SequencePlanPattern.h"

namespace facebook::velox::plugin::plantransformer {
class CompoundStateMachine : public StateMachine {
 public:
  class CompoundState : public State {};
  class Initial : public CompoundState {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class Filter : public CompoundState {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class Project : public CompoundState {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class Aggregate : public CompoundState {
   public:
    bool isFinal() override { return true; };
  };
  class AcceptPrev : public CompoundState {
   public:
    bool isFinal() override { return true; };
  };
  class NotAccept : public CompoundState {
   public:
    bool isFinal() override { return true; };
  };
  CompoundStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(VeloxPlanNodeAddr nodeAddr) override;
};

class LeftDeepJoinStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class OneJoin : public State {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class LeftJoin : public State {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class EndWithLeftJoin : public State {
   public:
    bool isFinal() override { return true; };
  };
  LeftDeepJoinStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(VeloxPlanNodeAddr nodeAddr) override;
  VeloxPlanNodeAddrList matchResult() override;
  void addToMatchResult(VeloxPlanNodeAddr nodeAddr) override;
  void clearMatchResult() override;

 private:
  VeloxPlanNodeAddrList compoundMatchResult_;
  bool inCompoundState();
};

// PlanPattern for "filter(optional)->proj->agg(optional)"
class CompoundPattern : public SequencePlanPattern {
 public:
  CompoundPattern() { setStateMachine(std::make_shared<CompoundStateMachine>()); }
};

// PlanPattern for ...agg->proj->filter->join ->join...
//                                         |--->  |
//                                                |---->
class LeftDeepJoinPattern : public SequencePlanPattern {
 public:
  LeftDeepJoinPattern() { setStateMachine(std::make_shared<LeftDeepJoinStateMachine>()); }
};

class FilterStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(VeloxPlanNodeAddr nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class Filter : public State {
   public:
    bool isFinal() override { return true; };
  };
  FilterStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(VeloxPlanNodeAddr nodeAddr) override;
};

class FilterPattern : public SequencePlanPattern {
 public:
  FilterPattern() { setStateMachine(std::make_shared<FilterStateMachine>()); }
};
}  // namespace facebook::velox::plugin::plantransformer
