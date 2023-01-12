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
#include <boost/algorithm/string.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup.hpp>
#include <boost/phoenix.hpp>
#include <boost/smart_ptr/weak_ptr.hpp>
#include <boost/variant.hpp>

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <regex>
#include "cider/CiderException.h"

namespace logger {

namespace attr = boost::log::attributes;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;
namespace sinks = boost::log::sinks;
namespace sources = boost::log::sources;
namespace po = boost::program_options;

BOOST_LOG_ATTRIBUTE_KEYWORD(process_id, "ProcessID", attr::current_process_id::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(channel, "Channel", Channel)
BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", Severity)

BOOST_LOG_GLOBAL_LOGGER_CTOR_ARGS(gChannelLogger_IR,
                                  ChannelLogger,
                                  (keywords::channel = IR))
BOOST_LOG_GLOBAL_LOGGER_CTOR_ARGS(gChannelLogger_PTX,
                                  ChannelLogger,
                                  (keywords::channel = PTX))
BOOST_LOG_GLOBAL_LOGGER_CTOR_ARGS(gChannelLogger_ASM,
                                  ChannelLogger,
                                  (keywords::channel = ASM))
BOOST_LOG_GLOBAL_LOGGER_DEFAULT(gSeverityLogger, SeverityLogger)

// Return last component of path
std::string filename(char const* path) {
  return std::filesystem::path(path).filename().string();
}

template <typename TAG>
std::string replace_braces(std::string const& str, TAG const tag) {
  constexpr std::regex::flag_type flags = std::regex::ECMAScript | std::regex::optimize;
  static std::regex const regex(R"(\{SEVERITY\})", flags);
  if /*constexpr*/ (std::is_same<TAG, Channel>::value) {
    return std::regex_replace(str, regex, ChannelNames[tag]);
  } else {
    return std::regex_replace(str, regex, SeverityNames[tag]);
  }
}

// Print decimal value for process_id (14620) instead of hex (0x0000391c)
boost::log::attributes::current_process_id::value_type::native_type get_native_process_id(
    boost::log::value_ref<boost::log::attributes::current_process_id::value_type,
                          tag::process_id> const& pid) {
  return pid ? pid->native_id() : 0;
}

template <typename SINK>
sinks::text_file_backend::open_handler_type create_or_replace_symlink(
    boost::weak_ptr<SINK> weak_ptr,
    std::string&& symlink) {
  return [weak_ptr,
          symlink = std::move(symlink)](sinks::text_file_backend::stream_type& stream) {
    if (boost::shared_ptr<SINK> sink = weak_ptr.lock()) {
      std::error_code ec;
      std::filesystem::path const file_name =
          sink->locked_backend()->get_current_file_name().string();
      std::filesystem::path const symlink_path = file_name.parent_path() / symlink;
      std::filesystem::remove(symlink_path, ec);
      if (ec) {
        stream << filename(__FILE__) << ':' << __LINE__ << ' ' << ec.message() << '\n';
      }
      std::filesystem::create_symlink(file_name.filename(), symlink_path, ec);
      if (ec) {
        stream << filename(__FILE__) << ':' << __LINE__ << ' ' << ec.message() << '\n';
      }
    }
  };
}

boost::log::formatting_ostream& operator<<(
    boost::log::formatting_ostream& strm,
    boost::log::to_log_manip<Channel, tag::channel> const& manip) {
  return strm << ChannelSymbols[manip.get()];
}

boost::log::formatting_ostream& operator<<(
    boost::log::formatting_ostream& strm,
    boost::log::to_log_manip<Severity, tag::severity> const& manip) {
  return strm << SeveritySymbols[manip.get()];
}

template <typename TAG, typename SINK>
void set_formatter(SINK& sink) {
  if /*constexpr*/ (std::is_same<TAG, Channel>::value) {
    sink->set_formatter(
        expr::stream << expr::format_date_time<boost::posix_time::ptime>(
                            "TimeStamp", "%Y-%m-%dT%H:%M:%S.%f")
                     << ' ' << channel << ' '
                     << boost::phoenix::bind(&get_native_process_id, process_id.or_none())
                     << ' ' << expr::smessage);
  } else {
    sink->set_formatter(
        expr::stream << expr::format_date_time<boost::posix_time::ptime>(
                            "TimeStamp", "%Y-%m-%dT%H:%M:%S.%f")
                     << ' ' << severity << ' '
                     << boost::phoenix::bind(&get_native_process_id, process_id.or_none())
                     << ' ' << expr::smessage);
  }
}

// Pointer to function to optionally call on LOG(FATAL).
std::atomic<FatalFunc> g_fatal_func{nullptr};
std::once_flag g_fatal_func_flag;

// Locking/atomicity not needed for g_any_active_channels or g_min_active_severity
// as they are modifed by init() once.
bool g_any_active_channels{false};
Severity g_min_active_severity{Severity::FATAL};

void set_once_fatal_func(FatalFunc fatal_func) {
  if (g_fatal_func.exchange(fatal_func)) {
    CIDER_THROW(CiderCompileException,
                "logger::set_once_fatal_func() should not be called more than once.");
  }
}

namespace {

// Remove quotes if they match from beginning and end of string.
// Does not check for escaped quotes within string.
void unquote(std::string& str) {
  if (1 < str.size() && (str.front() == '\'' || str.front() == '"') &&
      str.front() == str.back()) {
    str.erase(str.size() - 1, 1);
    str.erase(0, 1);
  }
}

}  // namespace

// Used by boost::program_options when parsing enum Channel.
std::istream& operator>>(std::istream& in, Channels& channels) {
  std::string line;
  std::getline(in, line);
  unquote(line);
  std::regex const rex(R"(\w+)");
  using TokenItr = std::regex_token_iterator<std::string::iterator>;
  TokenItr const end;
  for (TokenItr tok(line.begin(), line.end(), rex); tok != end; ++tok) {
    auto itr = std::find(ChannelNames.cbegin(), ChannelNames.cend(), *tok);
    if (itr == ChannelNames.cend()) {
      in.setstate(std::ios_base::failbit);
      break;
    } else {
      channels.emplace(static_cast<Channel>(itr - ChannelNames.cbegin()));
    }
  }
  return in;
}

// Used by boost::program_options when stringifying Channels.
std::ostream& operator<<(std::ostream& out, Channels const& channels) {
  int i = 0;
  for (auto const channel : channels) {
    out << (i++ ? " " : "") << ChannelNames.at(channel);
  }
  return out;
}

// Used by boost::program_options when parsing enum Severity.
std::istream& operator>>(std::istream& in, Severity& sev) {
  std::string token;
  in >> token;
  unquote(token);
  auto itr = std::find(SeverityNames.cbegin(), SeverityNames.cend(), token);
  if (itr == SeverityNames.cend()) {
    in.setstate(std::ios_base::failbit);
  } else {
    sev = static_cast<Severity>(itr - SeverityNames.cbegin());
  }
  return in;
}

// Used by boost::program_options when stringifying enum Severity.
std::ostream& operator<<(std::ostream& out, Severity const& sev) {
  return out << SeverityNames.at(sev);
}

namespace {
ChannelLogger& get_channel_logger(Channel const channel) {
  switch (channel) {
    default:
    case IR:
      return gChannelLogger_IR::get();
    case PTX:
      return gChannelLogger_PTX::get();
    case ASM:
      return gChannelLogger_ASM::get();
  }
}
}  // namespace

Logger::Logger(Channel channel)
    : is_channel_(true)
    , enum_value_(channel)
    , record_(std::make_unique<boost::log::record>(
          get_channel_logger(channel).open_record())) {
  if (*record_) {
    stream_ = std::make_unique<boost::log::record_ostream>(*record_);
  }
}

Logger::Logger(Severity severity)
    : is_channel_(false)
    , enum_value_(severity)
    , record_(std::make_unique<boost::log::record>(
          gSeverityLogger::get().open_record(keywords::severity = severity))) {
  if (*record_) {
    stream_ = std::make_unique<boost::log::record_ostream>(*record_);
  }
}

Logger::~Logger() noexcept(false) {
  if (stream_) {
    if (is_channel_) {
      get_channel_logger(static_cast<Channel>(enum_value_))
          .push_record(boost::move(stream_->get_record()));
    } else {
      gSeverityLogger::get().push_record(boost::move(stream_->get_record()));
    }
  }
  if (!is_channel_ && static_cast<Severity>(enum_value_) == Severity::FATAL) {
    if (FatalFunc fatal_func = g_fatal_func.load()) {
      // set_once_fatal_func() prevents race condition.
      // Exceptions thrown by (*fatal_func)() are propagated here.
      std::call_once(g_fatal_func_flag, *fatal_func);
    }
    CIDER_THROW(CheckFatalException, "");
  }
}

Logger::operator bool() const {
  return static_cast<bool>(stream_);
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

boost::log::record_ostream& Logger::stream(char const* file, int line) {
  return *stream_ << query_id() << ' ' << thread_id() << ' ' << filename(file) << ':'
                  << line << ' ';
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

std::ostream& operator<<(std::ostream& os, Duration const& duration) {
  return os << std::setw(2 * duration.depth_) << ' ' << duration.value() << "ms start("
            << duration.relative_start_time() << "ms) " << duration.name_ << ' '
            << filename(duration.file_) << ':' << duration.line_;
}

std::ostream& operator<<(std::ostream& os, DurationTree const& duration_tree) {
  os << std::setw(2 * duration_tree.depth_) << ' ' << "New thread("
     << duration_tree.thread_id_ << ')';
  for (auto const& duration_tree_node : duration_tree.durations()) {
    os << '\n' << duration_tree_node;
  }
  return os << '\n'
            << std::setw(2 * duration_tree.depth_) << ' ' << "End thread("
            << duration_tree.thread_id_ << ')';
}

// Only called by logAndEraseDurationTree() on root tree
boost::log::record_ostream& operator<<(boost::log::record_ostream& os,
                                       DurationTreeMap::const_reference kv_pair) {
  auto itr = kv_pair.second->durations().cbegin();
  auto const end = kv_pair.second->durations().cend();
  auto const& root_duration = boost::get<Duration>(*itr);
  os << "DEBUG_TIMER thread_id(" << kv_pair.first << ")\n"
     << root_duration.value() << "ms total duration for " << root_duration.name_;
  for (++itr; itr != end; ++itr) {
    os << '\n' << *itr;
  }
  return os;
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
  if (auto log = Logger(root_duration.severity_)) {
    log.stream(root_duration.file_, root_duration.line_) << *itr;
  }
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
