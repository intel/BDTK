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
 * @file    VariableContext.cpp
 * @brief   Variable Context
 **/

#include "VariableContext.h"

namespace generator {

VariableContext::VariableContext(int max_join_depth) : max_join_depth_(max_join_depth) {}

void VariableContext::convert(std::shared_ptr<GeneratorContext> ctx_ptr) {
  ctx_ptr->is_target_exprs_collected_ = is_target_exprs_collected_;
}

int VariableContext::getMaxJoinDepth() {
  return max_join_depth_;
}

int VariableContext::getCurJoinDepth() {
  return cur_join_depth_;
}

bool VariableContext::getIsTargetExprsCollected() {
  return is_target_exprs_collected_;
}

std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
VariableContext::getExprMapPtr(bool is_right_join_node) {
  if (is_right_join_node) {
    return right_expr_map_ptr_;
  } else {
    return left_expr_map_ptr_;
  }
}

void VariableContext::setMaxJoinDepth(int max_join_depth) {
  max_join_depth_ = max_join_depth;
}

void VariableContext::setCurJoinDepth(int cur_join_depth) {
  cur_join_depth_ = cur_join_depth;
}

void VariableContext::setIsTargetExprsCollected(bool is_target_exprs_collected) {
  is_target_exprs_collected_ = is_target_exprs_collected;
}

// merget left and right expr maps when visit join rel node
void VariableContext::mergeLeftAndRightExprMaps() {
  mergeTwoExprMaps(right_expr_map_ptr_, left_expr_map_ptr_);
  right_expr_map_ptr_->clear();
}

// merge expr maps when visit rel node
void VariableContext::mergeOutExprMapsInside(
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        src_expr_map_ptr,
    bool is_right_join_node) {
  if (is_right_join_node) {
    right_expr_map_ptr_->clear();
    mergeTwoExprMaps(src_expr_map_ptr, right_expr_map_ptr_);
  } else {
    left_expr_map_ptr_->clear();
    mergeTwoExprMaps(src_expr_map_ptr, left_expr_map_ptr_);
  }
}

void VariableContext::mergeTwoExprMaps(
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        src_expr_map_ptr,
    std::shared_ptr<std::unordered_map<int, std::shared_ptr<Analyzer::Expr>>>
        dst_expr_map_ptr) {
  int dst_size = dst_expr_map_ptr->size();
  for (auto iter = src_expr_map_ptr->begin(); iter != src_expr_map_ptr->end(); ++iter) {
    dst_expr_map_ptr->insert(std::pair(dst_size + iter->first, iter->second));
  }
}

}  // namespace generator
