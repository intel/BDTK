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

#pragma once

#include "planTransformer/PlanPattern.h"
#include "planTransformer/PlanRewriter.h"
#include "planTransformer/PlanTransformer.h"
#include "planTransformer/SequencePlanPattern.h"

namespace facebook::velox::plugin::plantransformer::test {
class ProjectFilterStateMachine : public StateMachine {
 public:
  class Initial : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class Project : public State {
   public:
    StatePtr accept(const VeloxPlanNodeAddr& nodeAddr) override;
  };
  class Filter : public State {
   public:
    bool isFinal() override { return true; };
  };
  class NotAccept : public State {
   public:
    bool isFinal() override { return true; };
  };

  ProjectFilterStateMachine() { setCurState(std::make_shared<Initial>()); }
  void setInitState() override { setCurState(std::make_shared<Initial>()); }
  bool accept(const VeloxPlanNodeAddr& nodeAddr) override;
};

// This class accept "Filter->Proj" as a pattern.
class ProjectFilterPattern : public SequencePlanPattern {
 public:
  ProjectFilterPattern() {
    setStateMachine(std::make_shared<ProjectFilterStateMachine>());
  }
};

class ProjcetFilterSwapRewriter : public PlanRewriter {
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithSingleSource(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddr& source) const override;
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithMultiSources(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddrList& srcList) const override;
};

class ProjcetFilterDeleteRewriter : public PlanRewriter {
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithSingleSource(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddr& source) const override;
  std::pair<bool, VeloxPlanNodePtr> rewritePlanSectionWithMultiSources(
      const VeloxNodeAddrPlanSection& planSection,
      const VeloxPlanNodeAddrList& srcList) const override;
};

}  // namespace facebook::velox::plugin::plantransformer::test
