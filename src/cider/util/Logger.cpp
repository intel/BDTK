/*
 * Copyright (c) 2022 Intel Corporation.
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

bool g_enable_debug_timer{false};

#include "Logger.h"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <boost/phoenix.hpp>
#include <boost/smart_ptr/weak_ptr.hpp>
#include <boost/variant.hpp>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <regex>
#include "cider/CiderException.h"

namespace logger {

// Return last component of path
std::string filename(char const* path) {
  return std::filesystem::path(path).filename().string();
}

thread_local std::atomic<QueryId> g_query_id{0};

QueryId query_id() {
  return g_query_id.load();
}

QidScopeGuard::~QidScopeGuard() {
  if (id_) {
    // Ideally this CHECK would be enabled, but it's too heavy for a destructor.
    // Would be ok for DEBUG mode.
    // CHECK(g_query_id.compare_exchange_strong(id_, 0));
    g_query_id = 0;
  }
}

// Only set g_query_id if its currently 0.
QidScopeGuard set_thread_local_query_id(QueryId const query_id) {
  QueryId expected = 0;
  return g_query_id.compare_exchange_strong(expected, query_id) ? QidScopeGuard(query_id)
                                                                : QidScopeGuard(0);
}

// DebugTimer-related classes and functions.
using Clock = std::chrono::steady_clock;

class DurationTree;

class Duration {
  DurationTree* const duration_tree_;
  Clock::time_point const start_;
  Clock::time_point stop_;

 public:
  int const depth_;
  Severity const severity_;
  char const* const file_;
  int const line_;
  char const* const name_;

  Duration(DurationTree* duration_tree,
           int depth,
           Severity severity,
           char const* file,
           int line,
           char const* name)
      : duration_tree_(duration_tree)
      , start_(Clock::now())
      , depth_(depth)
      , severity_(severity)
      , file_(file)
      , line_(line)
      , name_(name) {}
  bool stop();
  // Start time relative to parent DurationTree::start_.
  template <typename Units = std::chrono::milliseconds>
  typename Units::rep relative_start_time() const;
  // Duration value = stop_ - start_.
  template <typename Units = std::chrono::milliseconds>
  typename Units::rep value() const;
};

using DurationTreeNode = boost::variant<Duration, DurationTree&>;
using DurationTreeNodes = std::deque<DurationTreeNode>;

class DurationTree {
  DurationTreeNodes durations_;
  int current_depth_;  //< Depth of next DurationTreeNode.

 public:
  int const depth_;  //< Depth of tree within parent tree, 0 for root tree.
  Clock::time_point const start_;
  ThreadId const thread_id_;
  DurationTree(ThreadId thread_id, int start_depth)
      // Add +1 to current_depth_ for non-root DurationTrees for extra indentation.
      : current_depth_(start_depth + bool(start_depth))
      , depth_(start_depth)
      , start_(Clock::now())
      , thread_id_(thread_id) {}
  void pushDurationTree(DurationTree& duration_tree) {
    durations_.emplace_back(duration_tree);
  }
  const Duration& rootDuration() const {
    CHECK(!durations_.empty());
    return boost::get<Duration>(durations_.front());
  }
  int currentDepth() const { return current_depth_; }
  void decrementDepth() { --current_depth_; }
  DurationTreeNodes const& durations() const { return durations_; }
  template <typename... Ts>
  Duration* newDuration(Ts&&... args) {
    durations_.emplace_back(Duration(this, current_depth_++, std::forward<Ts>(args)...));
    return boost::get<Duration>(&durations_.back());
  }
};

/// Set stop_, decrement DurationTree::current_depth_.
/// Return true iff this Duration represents the root timer (see docs).
bool Duration::stop() {
  stop_ = Clock::now();
  duration_tree_->decrementDepth();
  return depth_ == 0;
}

template <typename Units>
typename Units::rep Duration::relative_start_time() const {
  return std::chrono::duration_cast<Units>(start_ - duration_tree_->start_).count();
}

template <typename Units>
typename Units::rep Duration::value() const {
  return std::chrono::duration_cast<Units>(stop_ - start_).count();
}

struct GetDepth : boost::static_visitor<int> {
  int operator()(Duration const& duration) const { return duration.depth_; }
  int operator()(DurationTree const& duration_tree) const { return duration_tree.depth_; }
};

using DurationTreeMap = std::unordered_map<ThreadId, std::unique_ptr<DurationTree>>;

std::mutex g_duration_tree_map_mutex;
DurationTreeMap g_duration_tree_map;
std::atomic<ThreadId> g_next_thread_id{0};
thread_local ThreadId const g_thread_id = g_next_thread_id++;

template <typename... Ts>
Duration* newDuration(Severity severity, Ts&&... args) {
  if (g_enable_debug_timer) {
    std::lock_guard<std::mutex> lock_guard(g_duration_tree_map_mutex);
    auto& duration_tree_ptr = g_duration_tree_map[g_thread_id];
    if (!duration_tree_ptr) {
      duration_tree_ptr = std::make_unique<DurationTree>(g_thread_id, 0);
    }
    return duration_tree_ptr->newDuration(severity, std::forward<Ts>(args)...);
  }
  return nullptr;  // Inactive - don't measure or report timing.
}

// Encode DurationTree into json.
// DurationTrees encode parent/child relationships using a combination
// of ordered descendents, and an int depth member value for each node.
// Direct-child nodes are those which:
//  * do not come after a node of depth <= parent_depth
//  * have depth == parent_depth + 1
class JsonEncoder : boost::static_visitor<rapidjson::Value> {
  std::shared_ptr<rapidjson::Document> doc_;
  rapidjson::Document::AllocatorType& alloc_;
  // Iterators are used to determine children when visiting a Duration node.
  DurationTreeNodes::const_iterator begin_;
  DurationTreeNodes::const_iterator end_;

  JsonEncoder(JsonEncoder& json_encoder,
              DurationTreeNodes::const_iterator begin,
              DurationTreeNodes::const_iterator end)
      : doc_(json_encoder.doc_), alloc_(doc_->GetAllocator()), begin_(begin), end_(end) {}

 public:
  JsonEncoder()
      : doc_(std::make_shared<rapidjson::Document>(rapidjson::kObjectType))
      , alloc_(doc_->GetAllocator()) {}
  rapidjson::Value operator()(Duration const& duration) {
    rapidjson::Value retval(rapidjson::kObjectType);
    retval.AddMember("type", "duration", alloc_);
    retval.AddMember("duration_ms", rapidjson::Value(duration.value()), alloc_);
    retval.AddMember(
        "start_ms", rapidjson::Value(duration.relative_start_time()), alloc_);
    retval.AddMember("name", rapidjson::StringRef(duration.name_), alloc_);
    retval.AddMember("file", filename(duration.file_), alloc_);
    retval.AddMember("line", rapidjson::Value(duration.line_), alloc_);
    retval.AddMember("children", childNodes(duration.depth_), alloc_);
    return retval;
  }
  rapidjson::Value operator()(DurationTree const& duration_tree) {
    begin_ = duration_tree.durations().cbegin();
    end_ = duration_tree.durations().cend();
    rapidjson::Value retval(rapidjson::kObjectType);
    retval.AddMember("type", "duration_tree", alloc_);
    retval.AddMember("thread_id", std::to_string(duration_tree.thread_id_), alloc_);
    retval.AddMember("children", childNodes(duration_tree.depth_), alloc_);
    return retval;
  }
  rapidjson::Value childNodes(int const parent_depth) {
    GetDepth const get_depth;
    rapidjson::Value children(rapidjson::kArrayType);
    for (auto itr = begin_; itr != end_; ++itr) {
      int const depth = apply_visitor(get_depth, *itr);
      if (depth <= parent_depth) {
        break;
      }
      if (depth == parent_depth + 1) {
        JsonEncoder json_encoder(*this, std::next(itr), end_);
        children.PushBack(apply_visitor(json_encoder, *itr), alloc_);
      }
    }
    return children;
  }
  // The root Duration is the "timer" node in the top level debug json object.
  // Only root Duration has overall total_duration_ms.
  rapidjson::Value timer(DurationTreeMap::const_reference kv_pair) {
    begin_ = kv_pair.second->durations().cbegin();
    end_ = kv_pair.second->durations().cend();
    rapidjson::Value retval(rapidjson::kObjectType);
    if (begin_ != end_) {
      auto const& root_duration = boost::get<Duration>(*(begin_++));
      retval.AddMember("type", "root", alloc_);
      retval.AddMember("thread_id", std::to_string(kv_pair.first), alloc_);
      retval.AddMember(
          "total_duration_ms", rapidjson::Value(root_duration.value()), alloc_);
      retval.AddMember("name", rapidjson::StringRef(root_duration.name_), alloc_);
      retval.AddMember("children", childNodes(0), alloc_);
    }
    return retval;
  }
  // Assumes *doc_ is empty.
  std::string str(DurationTreeMap::const_reference kv_pair) {
    doc_->AddMember("timer", timer(kv_pair), alloc_);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc_->Accept(writer);
    return {buffer.GetString(), buffer.GetSize()};
  }
};

/// Depth-first search and erase all DurationTrees. Not thread-safe.
struct EraseDurationTrees : boost::static_visitor<> {
  void operator()(DurationTreeMap::const_iterator const& itr) const {
    for (auto const& duration_tree_node : itr->second->durations()) {
      apply_visitor(*this, duration_tree_node);
    }
    g_duration_tree_map.erase(itr);
  }
  void operator()(Duration const&) const {}
  void operator()(DurationTree const& duration_tree) const {
    for (auto const& duration_tree_node : duration_tree.durations()) {
      apply_visitor(*this, duration_tree_node);
    }
    g_duration_tree_map.erase(duration_tree.thread_id_);
  }
};

void logAndEraseDurationTree(std::string* json_str) {
  std::lock_guard<std::mutex> lock_guard(g_duration_tree_map_mutex);
  DurationTreeMap::const_iterator const itr = g_duration_tree_map.find(g_thread_id);
  CHECK(itr != g_duration_tree_map.cend());
  auto const& root_duration = itr->second->rootDuration();
#if 0
  if (auto log = Logger(root_duration.severity_)) {
    log.stream(root_duration.file_, root_duration.line_) << *itr;
  }
#endif
  if (json_str) {
    JsonEncoder json_encoder;
    *json_str = json_encoder.str(*itr);
  }
  EraseDurationTrees erase_duration_trees;
  erase_duration_trees(itr);
}

DebugTimer::DebugTimer(Severity severity, char const* file, int line, char const* name)
    : duration_(newDuration(severity, file, line, name)) {}

DebugTimer::~DebugTimer() {
  stop();
}

void DebugTimer::stop() {
  if (duration_) {
    if (duration_->stop()) {
      logAndEraseDurationTree(nullptr);
    }
    duration_ = nullptr;
  }
}

std::string DebugTimer::stopAndGetJson() {
  std::string json_str;
  if (duration_) {
    if (duration_->stop()) {
      logAndEraseDurationTree(&json_str);
    }
    duration_ = nullptr;
  }
  return json_str;
}

/// Call this when a new thread is spawned that will have timers that need to be
/// associated with timers on the parent thread.
void debug_timer_new_thread(ThreadId parent_thread_id) {
  std::lock_guard<std::mutex> lock_guard(g_duration_tree_map_mutex);
  auto parent_itr = g_duration_tree_map.find(parent_thread_id);
  CHECK(parent_itr != g_duration_tree_map.end());
  auto const current_depth = parent_itr->second->currentDepth();
  auto& duration_tree_ptr = g_duration_tree_map[g_thread_id];
  if (!duration_tree_ptr) {
    duration_tree_ptr = std::make_unique<DurationTree>(g_thread_id, current_depth + 1);
    parent_itr->second->pushDurationTree(*duration_tree_ptr);
  } else {
    // If this is executed, then this was not really a new thread.
    // Since some libraries recycle threads, we won't trigger an error here.
  }
}

ThreadId thread_id() {
  return g_thread_id;
}

}  // namespace logger
