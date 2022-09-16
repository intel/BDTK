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
 * @file    RelVisitor.cpp
 * @brief   Rel visitor
 **/

#include "RelVisitor.h"

namespace generator {

RelVisitor::RelVisitor(Substrait2AnalyzerExprConverter* toAnalyzerExprConverter,
                       const std::unordered_map<int, std::string>& function_map,
                       std::shared_ptr<VariableContext> variable_context_shared_ptr,
                       bool is_right_join_node)
    : toAnalyzerExprConverter_(toAnalyzerExprConverter)
    , function_map_(function_map)
    , variable_context_shared_ptr_(variable_context_shared_ptr)
    , is_right_join_node_(is_right_join_node) {}

RelVisitor::~RelVisitor() {}

// read
void RelVisitor::visit(InputDescContext* input_desc_context) {}

// agg、read、project
void RelVisitor::visit(TargetContext* target_context) {}

// agg
void RelVisitor::visit(GroupbyContext* groupby_context) {}

// filter
void RelVisitor::visit(FilterQualContext* filter_qual_context) {}

// join
void RelVisitor::visit(JoinQualContext* join_qual_context) {}

// sort
void RelVisitor::visit(OrderEntryContext* order_entry_context) {}

}  // namespace generator
