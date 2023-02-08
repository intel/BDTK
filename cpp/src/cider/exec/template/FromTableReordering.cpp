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
#include "FromTableReordering.h"
#include "Execute.h"
#include "RangeTableIndexVisitor.h"
#include "type/plan/Analyzer.h"

#include <numeric>
#include <queue>
#include <regex>

namespace {

using cost_t = unsigned;
using node_t = size_t;

// Returns a lhs/rhs cost for the given qualifier. Must be strictly greater than 0.
std::pair<cost_t, cost_t> get_join_qual_cost(const Analyzer::Expr* qual,
                                             const Executor* executor) {
  const auto func_oper = dynamic_cast<const Analyzer::FunctionOper*>(qual);
  if (func_oper) {
    return {200, 200};
  }
  const auto bin_oper = dynamic_cast<const Analyzer::BinOper*>(qual);
  if (!bin_oper || !IS_EQUIVALENCE(bin_oper->get_optype())) {
    return {200, 200};
  }
  if (executor) {
    try {
      HashJoin::normalizeColumnPairs(
          bin_oper, executor->getSchemaProvider(), executor->getTemporaryTables());
    } catch (...) {
      return {200, 200};
    }
  }
  return {100, 100};
}

// Builds a graph with nesting levels as nodes and join condition costs as edges.
std::vector<std::map<node_t, cost_t>> build_join_cost_graph(
    const JoinQualsPerNestingLevel& left_deep_join_quals,
    const std::vector<InputTableInfo>& table_infos,
    const Executor* executor) {
  CHECK_EQ(left_deep_join_quals.size() + 1, table_infos.size());
  std::vector<std::map<node_t, cost_t>> join_cost_graph(table_infos.size());
  AllRangeTableIndexVisitor visitor;
  // Build the constraints graph: nodes are nest levels, edges are the existence of
  // qualifiers between levels.
  for (const auto& current_level_join_conditions : left_deep_join_quals) {
    for (const auto& qual : current_level_join_conditions.quals) {
      std::set<int> qual_nest_levels = visitor.visit(qual.get());
      if (qual_nest_levels.size() != 2) {
        continue;
      }
      int lhs_nest_level = *qual_nest_levels.begin();
      CHECK_GE(lhs_nest_level, 0);
      qual_nest_levels.erase(qual_nest_levels.begin());
      int rhs_nest_level = *qual_nest_levels.begin();
      CHECK_GE(rhs_nest_level, 0);

      // Get the {lhs, rhs} cost for the qual
      const auto cost_pair = get_join_qual_cost(qual.get(), executor);
      const auto edge_it = join_cost_graph[lhs_nest_level].find(rhs_nest_level);
      if (edge_it == join_cost_graph[lhs_nest_level].end() ||
          edge_it->second > cost_pair.second) {
        join_cost_graph[lhs_nest_level][rhs_nest_level] = cost_pair.second;
        join_cost_graph[rhs_nest_level][lhs_nest_level] = cost_pair.first;
      }
    }
  }
  return join_cost_graph;
}

// Tracks dependencies between nodes.
class SchedulingDependencyTracking {
 public:
  SchedulingDependencyTracking(const size_t node_count) : inbound_(node_count) {}

  // Add a from -> to dependency.
  void addEdge(const node_t from, const node_t to) { inbound_[to].insert(from); }

  // Removes from's outbound dependencies.
  void removeNode(const node_t from) {
    for (auto& inbound_for_node : inbound_) {
      inbound_for_node.erase(from);
    }
  }

  // Returns the set of all nodes without dependencies.
  std::unordered_set<node_t> getRoots() const {
    std::unordered_set<node_t> roots;
    for (node_t candidate = 0; candidate < inbound_.size(); ++candidate) {
      if (inbound_[candidate].empty()) {
        roots.insert(candidate);
      }
    }
    return roots;
  }

 private:
  std::vector<std::unordered_set<node_t>> inbound_;
};

// The tree edge for traversal of the cost graph.
struct TraversalEdge {
  node_t nest_level;
  cost_t join_cost;
};

// Builds dependency tracking based on left joins
SchedulingDependencyTracking build_dependency_tracking(
    const JoinQualsPerNestingLevel& left_deep_join_quals,
    const std::vector<std::map<node_t, cost_t>>& join_cost_graph) {
  SchedulingDependencyTracking dependency_tracking(left_deep_join_quals.size() + 1);
  // Add directed graph edges for left join dependencies.
  // See also start_it inside traverse_join_cost_graph(). These
  // edges prevent start_it from pointing to a table with a
  // left join dependency on another table.
  for (size_t level_idx = 0; level_idx < left_deep_join_quals.size(); ++level_idx) {
    if (left_deep_join_quals[level_idx].type == JoinType::LEFT) {
      dependency_tracking.addEdge(level_idx, level_idx + 1);
    }
  }
  return dependency_tracking;
}

// Do a breadth-first traversal of the cost graph. This avoids scheduling a nest level
// before the ones which constraint it are scheduled and it favors equi joins over loop
// joins.
std::vector<node_t> traverse_join_cost_graph(
    const std::vector<std::map<node_t, cost_t>>& join_cost_graph,
    const std::vector<InputTableInfo>& table_infos,
    const std::function<bool(const node_t lhs_nest_level, const node_t rhs_nest_level)>&
        compare_node,
    const std::function<bool(const TraversalEdge&, const TraversalEdge&)>& compare_edge,
    const JoinQualsPerNestingLevel& left_deep_join_quals) {
  std::vector<node_t> all_nest_levels(table_infos.size());
  std::iota(all_nest_levels.begin(), all_nest_levels.end(), 0);
  std::vector<node_t> input_permutation;
  std::unordered_set<node_t> visited;
  auto dependency_tracking =
      build_dependency_tracking(left_deep_join_quals, join_cost_graph);
  auto schedulable_node = [&dependency_tracking, &visited](const node_t node) {
    const auto nodes_ready = dependency_tracking.getRoots();
    return nodes_ready.find(node) != nodes_ready.end() &&
           visited.find(node) == visited.end();
  };
  while (visited.size() < table_infos.size()) {
    // Filter out nest levels which are already visited or have pending dependencies.
    std::vector<node_t> remaining_nest_levels;
    std::copy_if(all_nest_levels.begin(),
                 all_nest_levels.end(),
                 std::back_inserter(remaining_nest_levels),
                 schedulable_node);
    CHECK(!remaining_nest_levels.empty());
    // Start with the table with most tuples.
    const auto start_it = std::max_element(
        remaining_nest_levels.begin(), remaining_nest_levels.end(), compare_node);
    CHECK(start_it != remaining_nest_levels.end());
    std::priority_queue<TraversalEdge, std::vector<TraversalEdge>, decltype(compare_edge)>
        worklist(compare_edge);
    //  look at all edges, compare the
    //  cost of our edge vs theirs, and pick the best start edge
    node_t start = *start_it;
    TraversalEdge start_edge{start, 0};
    VLOG(2) << "Table reordering starting with nest level " << start;
    for (const auto& graph_edge : join_cost_graph[*start_it]) {
      const node_t succ = graph_edge.first;
      if (!schedulable_node(succ)) {
        continue;
      }
      const TraversalEdge succ_edge{succ, graph_edge.second};
      for (const auto& successor_edge : join_cost_graph[succ]) {
        if (successor_edge.first == start) {
          start_edge.join_cost = successor_edge.second;
          // lhs cost / num tuples less than rhs cost if compare edge is true, swap nest
          // levels
          if (compare_edge(start_edge, succ_edge)) {
            VLOG(2) << "Table reordering changing start nest level from " << start
                    << " to " << succ;
            start = succ;
            start_edge = succ_edge;
          }
        }
      }
    }
    VLOG(2) << "Table reordering picked start nest level " << start << " with cost "
            << start_edge.join_cost;
    CHECK_EQ(start, start_edge.nest_level);
    worklist.push(start_edge);
    const auto it_ok = visited.insert(start);
    CHECK(it_ok.second);
    while (!worklist.empty()) {
      // Extract a node and add it to the permutation.
      TraversalEdge crt = worklist.top();
      worklist.pop();
      input_permutation.push_back(crt.nest_level);
      dependency_tracking.removeNode(crt.nest_level);
      // Add successors which are ready and not visited yet to the queue.
      for (const auto& graph_edge : join_cost_graph[crt.nest_level]) {
        const node_t succ = graph_edge.first;
        if (!schedulable_node(succ)) {
          continue;
        }
        worklist.push(TraversalEdge{succ, graph_edge.second});
        const auto it_ok = visited.insert(succ);
        CHECK(it_ok.second);
      }
    }
  }
  return input_permutation;
}

}  // namespace

std::vector<node_t> get_node_input_permutation(
    const JoinQualsPerNestingLevel& left_deep_join_quals,
    const std::vector<InputTableInfo>& table_infos,
    const Executor* executor) {
  const auto join_cost_graph =
      build_join_cost_graph(left_deep_join_quals, table_infos, executor);
  // Use the number of tuples in each table to break ties in BFS.
  const auto compare_node = [&table_infos](const node_t lhs_nest_level,
                                           const node_t rhs_nest_level) {
    return table_infos[lhs_nest_level].info.getNumTuplesUpperBound() <
           table_infos[rhs_nest_level].info.getNumTuplesUpperBound();
  };
  const auto compare_edge = [&compare_node](const TraversalEdge& lhs_edge,
                                            const TraversalEdge& rhs_edge) {
    // Only use the number of tuples as a tie-breaker, if costs are equal.
    if (lhs_edge.join_cost == rhs_edge.join_cost) {
      return compare_node(lhs_edge.nest_level, rhs_edge.nest_level);
    }
    return lhs_edge.join_cost > rhs_edge.join_cost;
  };
  return traverse_join_cost_graph(
      join_cost_graph, table_infos, compare_node, compare_edge, left_deep_join_quals);
}
