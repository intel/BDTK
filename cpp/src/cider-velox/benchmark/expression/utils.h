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

#include "velox/core/PlanNode.h"

class VirtualTableTrimer {
 public:
  static void trim(facebook::velox::core::PlanNodePtr& plan) {
    auto planNode = std::const_pointer_cast<facebook::velox::core::PlanNode>(plan);
    if (auto filterNode =
            std::dynamic_pointer_cast<facebook::velox::core::FilterNode>(planNode)) {
      trim(filterNode);
      return;
    }
    if (auto valuesNode =
            std::dynamic_pointer_cast<facebook::velox::core::ValuesNode>(planNode)) {
      trim(valuesNode);
      return;
    }
    if (auto projectNode =
            std::dynamic_pointer_cast<facebook::velox::core::ProjectNode>(planNode)) {
      trim(projectNode);
      return;
    }
    if (auto aggregationNode =
            std::dynamic_pointer_cast<facebook::velox::core::AggregationNode>(planNode)) {
      trim(aggregationNode);
      return;
    }
  }

 private:
  static void trim(std::shared_ptr<facebook::velox::core::ValuesNode>& valueNode) {
    const_cast<std::vector<facebook::velox::RowVectorPtr>&>(valueNode->values()).clear();
  }
  static void trim(std::shared_ptr<facebook::velox::core::ProjectNode>& projectNode) {
    std::vector<facebook::velox::core::PlanNodePtr> sources = projectNode->sources();
    trim(sources[0]);
  }
  static void trim(std::shared_ptr<facebook::velox::core::FilterNode>& filterNode) {
    std::vector<facebook::velox::core::PlanNodePtr> sources = filterNode->sources();
    trim(sources[0]);
  }
  static void trim(
      std::shared_ptr<facebook::velox::core::AggregationNode>& aggregateNode) {
    std::vector<facebook::velox::core::PlanNodePtr> sources = aggregateNode->sources();
    trim(sources[0]);
  }
};  // namespace
