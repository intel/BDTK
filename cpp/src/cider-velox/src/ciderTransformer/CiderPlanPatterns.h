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
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class Filter : public CompoundState {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class Project : public CompoundState {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class Aggregate : public CompoundState {
   public:
    bool isFinal() override { return true; };
  };
  class TopN : public CompoundState {
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
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

class LeftDeepJoinStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class OneJoin : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class LeftJoin : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class EndWithLeftJoin : public State {
   public:
    bool isFinal() override { return true; };
  };
  LeftDeepJoinStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
  VeloxPlanNodeAddrList matchResult() override;
  void addToMatchResult(const VeloxPlanNodeAddr& nodeAddr) override;
  void clearMatchResult() override;

 private:
  VeloxPlanNodeAddrList compoundMatchResult_;
  VeloxPlanNodeAddrList filterMatchResult_;
  VeloxPlanNodeAddrList partialAggMatchResult_;
  bool inCompoundState();
  bool inFilterState();
  bool inPartialAggState();
};

class FilterStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
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
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

class ProjectStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class Project : public State {
   public:
    bool isFinal() override { return true; };
  };
  ProjectStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

class PartialAggStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class PartialAgg : public State {
   public:
    bool isFinal() override { return true; };
  };
  PartialAggStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

class OrderByStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class OrderBy : public State {
   public:
    bool isFinal() override { return true; };
  };
  OrderByStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

class TopNStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };
  class TopN : public State {
   public:
    bool isFinal() override { return true; };
  };
  TopNStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); };
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
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

class FilterPattern : public SequencePlanPattern {
 public:
  FilterPattern() { setStateMachine(std::make_shared<FilterStateMachine>()); }
};

class ProjectPattern : public SequencePlanPattern {
 public:
  ProjectPattern() { setStateMachine(std::make_shared<ProjectStateMachine>()); }
};

class PartialAggPattern : public SequencePlanPattern {
 public:
  PartialAggPattern() { setStateMachine(std::make_shared<PartialAggStateMachine>()); }
};

class OrderByPattern : public SequencePlanPattern {
 public:
  OrderByPattern() { setStateMachine(std::make_shared<OrderByStateMachine>()); }
};

class TopNPattern : public SequencePlanPattern {
 public:
  TopNPattern() { setStateMachine(std::make_shared<TopNStateMachine>()); }
};

}  // namespace facebook::velox::plugin::plantransformer
