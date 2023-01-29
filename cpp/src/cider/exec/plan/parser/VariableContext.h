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

/**
 * @file    VariableContext.h
 * @brief   Variable Context
 **/

#pragma once

#include <memory>
#include <unordered_map>
#include "GeneratorContext.h"
#include "type/plan/Analyzer.h"

namespace generator {

class VariableContext {
 public:
  explicit VariableContext(int max_join_depth);
  void convert(std::shared_ptr<GeneratorContext> ctx_ptr);
  int getMaxJoinDepth();
  int getCurJoinDepth();
  bool getIsTargetExprsCollected();
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>> getExprMapPtr(
      bool is_join_right_node = false);
  void setMaxJoinDepth(int max_join_depth);
  void setCurJoinDepth(int cur_join_depth);
  void setIsTargetExprsCollected(bool is_target_exprs_collected);
  // merget left and right expr maps when visit join rel node
  void mergeLeftAndRightExprMaps();
  // merge expr maps when visit rel node
  void mergeOutExprMapsInside(
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          src_expr_map_ptr,
      bool is_join_right_node = false);

 private:
  void mergeTwoExprMaps(
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          src_expr_map_ptr,
      std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
          dst_expr_map_ptr);

 private:
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
      left_expr_map_ptr_ =
          std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
      right_expr_map_ptr_ =
          std::make_shared<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>();
  // use to records the cur join depth for fill the right table-id info
  int max_join_depth_ = 0;
  int cur_join_depth_ = 0;
  bool is_target_exprs_collected_ = false;
};

}  // namespace generator
