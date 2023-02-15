/*
 * Copyright(c) 2022-2023 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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
#ifndef QUERYENGINE_RELALGEXECUTIONUNIT_H
#define QUERYENGINE_RELALGEXECUTIONUNIT_H

#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/sqldefs.h"
#include "util/toString.h"

#include <boost/graph/adjacency_list.hpp>

#include <list>
#include <memory>
#include <optional>
#include <vector>

using AdjacentList = boost::adjacency_list<boost::setS, boost::vecS, boost::directedS>;
// node ID used when extracting query plan DAG
// note this ID is different from RelNode's id since query plan DAG extractor assigns an
// unique node ID only to a rel node which is included in extracted DAG (if we cannot
// extract a DAG from the query plan DAG extractor skips to assign unique IDs to rel nodes
// in that query plan
using RelNodeId = size_t;
// toString content of each extracted rel node
using RelNodeExplained = std::string;
// hash value of explained rel node
using RelNodeExplainedHash = size_t;
// a string representation of a query plan that is collected by visiting query plan DAG
// starting from root to leaf and concatenate each rel node's id
// where two adjacent rel nodes in a QueryPlan are connected via '|' delimiter
// i.e., 1|2|3|4|
using QueryPlan = std::string;
// join column's column id info
using JoinColumnsInfo = std::string;

enum JoinColumnSide {
  kInner,
  kOuter,
  kQual,   // INNER + OUTER
  kDirect  // set target directly (i.e., put Analyzer::Expr* instead of
           // Analyzer::BinOper*)
};
constexpr char const* EMPTY_QUERY_PLAN = "";

enum class SortAlgorithm { Default, SpeculativeTopN, StreamingTopN };

namespace Analyzer {
class Expr;
class ColumnVar;
class Estimator;
struct OrderEntry;

}  // namespace Analyzer

struct SortInfo {
  const std::list<Analyzer::OrderEntry> order_entries;
  const SortAlgorithm algorithm;
  const size_t limit;
  const size_t offset;
};

struct JoinCondition {
  std::list<std::shared_ptr<Analyzer::Expr>> quals;
  JoinType type;
};

using JoinQualsPerNestingLevel = std::vector<JoinCondition>;

struct RelAlgExecutionUnit {
  std::vector<InputDescriptor> input_descs;
  std::list<std::shared_ptr<const InputColDescriptor>> input_col_descs;
  std::list<std::shared_ptr<Analyzer::Expr>> simple_quals;
  std::list<std::shared_ptr<Analyzer::Expr>> quals;
  const JoinQualsPerNestingLevel join_quals;
  const std::list<std::shared_ptr<Analyzer::Expr>> groupby_exprs;
  std::vector<Analyzer::Expr*> target_exprs;
  const std::shared_ptr<Analyzer::Estimator> estimator;
  const SortInfo sort_info;
  size_t scan_limit;
  bool use_bump_allocator{false};
  // empty if not a UNION, true if UNION ALL, false if regular UNION
  const std::optional<bool> union_all;
  std::vector<std::shared_ptr<Analyzer::Expr>> shared_target_exprs{};
};

std::ostream& operator<<(std::ostream& os, const RelAlgExecutionUnit& ra_exe_unit);
std::string ra_exec_unit_desc_for_caching(const RelAlgExecutionUnit& ra_exe_unit);

class ResultSet;
using ResultSetPtr = std::shared_ptr<ResultSet>;

#endif  // QUERYENGINE_RELALGEXECUTIONUNIT_H
