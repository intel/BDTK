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

#include "StringDictionaryProxy.h"

#include "function/string/StringLike.h"
#include "type/data/sqltypes.h"
#include "util/Logger.h"
#include "util/Regexp.h"
#include "util/misc.h"

StringDictionaryProxy::StringDictionaryProxy(std::shared_ptr<StringDictionary> sd,
                                             const int32_t string_dict_id,
                                             const int64_t generation)
    : string_dict_(sd), string_dict_id_(string_dict_id), generation_(generation) {}

int32_t truncate_to_generation(const int32_t id, const size_t generation) {
  if (id == StringDictionary::INVALID_STR_ID) {
    return id;
  }
  CHECK_GE(id, 0);
  return static_cast<size_t>(id) >= generation ? StringDictionary::INVALID_STR_ID : id;
}

std::vector<int32_t> StringDictionaryProxy::getTransientBulk(
    const std::vector<std::string>& strings) const {
  CHECK_GE(generation_, 0);
  std::vector<int32_t> string_ids(strings.size());
  getTransientBulkImpl(strings, string_ids.data(), true);
  return string_ids;
}

std::vector<int32_t> StringDictionaryProxy::getOrAddTransientBulk(
    const std::vector<std::string>& strings) {
  CHECK_GE(generation_, 0);
  const size_t num_strings = strings.size();
  std::vector<int32_t> string_ids(num_strings);
  if (num_strings == 0) {
    return string_ids;
  }
  // Since new strings added to a StringDictionaryProxy are not materialized in the
  // proxy's underlying StringDictionary, we can use the fast parallel
  // StringDictionary::getBulk method to fetch ids from the underlying dictionary (which
  // will return StringDictionary::INVALID_STR_ID for strings that don't exist)

  // Don't need to be under lock here as the string ids for strings in the underlying
  // materialized dictionary are immutable
  const size_t num_strings_not_found =
      string_dict_->getBulk(strings, string_ids.data(), generation_);
  if (num_strings_not_found > 0) {
    std::lock_guard<std::shared_mutex> write_lock(rw_mutex_);
    for (size_t string_idx = 0; string_idx < num_strings; ++string_idx) {
      if (string_ids[string_idx] == StringDictionary::INVALID_STR_ID) {
        string_ids[string_idx] = getOrAddTransientUnlocked(strings[string_idx]);
      }
    }
  }
  return string_ids;
}

template <typename String>
int32_t StringDictionaryProxy::getOrAddTransientUnlocked(String const& str) {
  unsigned const new_index = transient_str_to_int_.size();
  auto transient_id = transientIndexToId(new_index);
  auto const emplaced = transient_str_to_int_.emplace(str, transient_id);
  if (emplaced.second) {  // (str, transient_id) was added to transient_str_to_int_.
    transient_string_vec_.push_back(&emplaced.first->first);
  } else {  // str already exists in transient_str_to_int_. Return existing transient_id.
    transient_id = emplaced.first->second;
  }
  return transient_id;
}

int32_t StringDictionaryProxy::getOrAddTransient(const std::string& str) {
  auto const string_id = getIdOfStringFromClient(str);
  if (string_id != StringDictionary::INVALID_STR_ID) {
    return string_id;
  }
  std::lock_guard<std::shared_mutex> write_lock(rw_mutex_);
  return getOrAddTransientUnlocked(str);
}

int32_t StringDictionaryProxy::getIdOfString(const std::string& str) const {
  std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
  auto const str_id = getIdOfStringFromClient(str);
  if (str_id != StringDictionary::INVALID_STR_ID || transient_str_to_int_.empty()) {
    return str_id;
  }
  auto it = transient_str_to_int_.find(str);
  return it != transient_str_to_int_.end() ? it->second
                                           : StringDictionary::INVALID_STR_ID;
}

template <typename String>
int32_t StringDictionaryProxy::getIdOfStringFromClient(const String& str) const {
  CHECK_GE(generation_, 0);
  return truncate_to_generation(string_dict_->getIdOfString(str), generation_);
}

int32_t StringDictionaryProxy::getIdOfStringNoGeneration(const std::string& str) const {
  std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
  auto str_id = string_dict_->getIdOfString(str);
  if (str_id != StringDictionary::INVALID_STR_ID || transient_str_to_int_.empty()) {
    return str_id;
  }
  auto it = transient_str_to_int_.find(str);
  return it != transient_str_to_int_.end() ? it->second
                                           : StringDictionary::INVALID_STR_ID;
}

extern "C" RUNTIME_EXPORT const char* StringDictionaryProxy_getStringBytes(
    int8_t* proxy_ptr,
    int32_t string_id) {
  CHECK(proxy_ptr != nullptr);
  auto proxy = reinterpret_cast<StringDictionaryProxy*>(proxy_ptr);
  auto [c_str, len] = proxy->getStringBytes(string_id);
  return c_str;
}

extern "C" RUNTIME_EXPORT size_t
StringDictionaryProxy_getStringLength(int8_t* proxy_ptr, int32_t string_id) {
  CHECK(proxy_ptr != nullptr);
  auto proxy = reinterpret_cast<StringDictionaryProxy*>(proxy_ptr);
  auto [c_str, len] = proxy->getStringBytes(string_id);
  return len;
}

extern "C" RUNTIME_EXPORT int32_t StringDictionaryProxy_getStringId(int8_t* proxy_ptr,
                                                                    char* c_str_ptr) {
  CHECK(proxy_ptr != nullptr);
  auto proxy = reinterpret_cast<StringDictionaryProxy*>(proxy_ptr);
  std::string str(c_str_ptr);
  return proxy->getOrAddTransient(str);
}

std::string StringDictionaryProxy::getString(int32_t string_id) const {
  if (inline_int_null_value<int32_t>() == string_id) {
    return "";
  }
  std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
  return getStringUnlocked(string_id);
}

std::string StringDictionaryProxy::getStringUnlocked(const int32_t string_id) const {
  if (string_id >= 0 && storageEntryCount() > 0) {
    return string_dict_->getString(string_id);
  }
  unsigned const string_index = transientIdToIndex(string_id);
  CHECK_LT(string_index, transient_string_vec_.size());
  return *transient_string_vec_[string_index];
}

std::vector<std::string> StringDictionaryProxy::getStrings(
    const std::vector<int32_t>& string_ids) const {
  std::vector<std::string> strings;
  if (!string_ids.empty()) {
    strings.reserve(string_ids.size());
    for (const auto string_id : string_ids) {
      if (string_id >= 0) {
        strings.emplace_back(string_dict_->getString(string_id));
      } else if (inline_int_null_value<int32_t>() == string_id) {
        strings.emplace_back("");
      } else {
        unsigned const string_index = transientIdToIndex(string_id);
        strings.emplace_back(*transient_string_vec_[string_index]);
      }
    }
  }
  return strings;
}

template <typename String>
int32_t StringDictionaryProxy::lookupTransientStringUnlocked(
    const String& lookup_string) const {
  const auto it = transient_str_to_int_.find(lookup_string);
  return it == transient_str_to_int_.end() ? StringDictionary::INVALID_STR_ID
                                           : it->second;
}

void order_translation_locks(const int32_t source_dict_id,
                             const int32_t dest_dict_id,
                             std::shared_lock<std::shared_mutex>& source_proxy_read_lock,
                             std::unique_lock<std::shared_mutex>& dest_proxy_write_lock) {
  if (source_dict_id == dest_dict_id) {
    // proxies are same, only take one write lock
    dest_proxy_write_lock.lock();
  } else if (source_dict_id < dest_dict_id) {
    source_proxy_read_lock.lock();
    dest_proxy_write_lock.lock();
  } else {
    dest_proxy_write_lock.lock();
    source_proxy_read_lock.lock();
  }
}

namespace {

bool is_like(const std::string& str,
             const std::string& pattern,
             const bool icase,
             const bool is_simple,
             const char escape) {
  return icase
             ? (is_simple ? string_ilike_simple(
                                str.c_str(), str.size(), pattern.c_str(), pattern.size())
                          : string_ilike(str.c_str(),
                                         str.size(),
                                         pattern.c_str(),
                                         pattern.size(),
                                         escape))
             : (is_simple ? string_like_simple(
                                str.c_str(), str.size(), pattern.c_str(), pattern.size())
                          : string_like(str.c_str(),
                                        str.size(),
                                        pattern.c_str(),
                                        pattern.size(),
                                        escape));
}

}  // namespace

std::vector<int32_t> StringDictionaryProxy::getLike(const std::string& pattern,
                                                    const bool icase,
                                                    const bool is_simple,
                                                    const char escape) const {
  CHECK_GE(generation_, 0);
  auto result = string_dict_->getLike(pattern, icase, is_simple, escape, generation_);
  for (unsigned index = 0; index < transient_string_vec_.size(); ++index) {
    if (is_like(*transient_string_vec_[index], pattern, icase, is_simple, escape)) {
      result.push_back(transientIndexToId(index));
    }
  }
  return result;
}

namespace {

bool do_compare(const std::string& str,
                const std::string& pattern,
                const std::string& comp_operator) {
  int res = str.compare(pattern);
  if (comp_operator == "<") {
    return res < 0;
  } else if (comp_operator == "<=") {
    return res <= 0;
  } else if (comp_operator == "=") {
    return res == 0;
  } else if (comp_operator == ">") {
    return res > 0;
  } else if (comp_operator == ">=") {
    return res >= 0;
  } else if (comp_operator == "<>") {
    return res != 0;
  }
  throw std::runtime_error("unsupported string compare operator");
}

}  // namespace

std::vector<int32_t> StringDictionaryProxy::getCompare(
    const std::string& pattern,
    const std::string& comp_operator) const {
  CHECK_GE(generation_, 0);
  auto result = string_dict_->getCompare(pattern, comp_operator, generation_);
  for (unsigned index = 0; index < transient_string_vec_.size(); ++index) {
    if (do_compare(*transient_string_vec_[index], pattern, comp_operator)) {
      result.push_back(transientIndexToId(index));
    }
  }
  return result;
}

namespace {

bool is_regexp_like(const std::string& str,
                    const std::string& pattern,
                    const char escape) {
  return regexp_like(str.c_str(), str.size(), pattern.c_str(), pattern.size(), escape);
}

}  // namespace

std::vector<int32_t> StringDictionaryProxy::getRegexpLike(const std::string& pattern,
                                                          const char escape) const {
  CHECK_GE(generation_, 0);
  auto result = string_dict_->getRegexpLike(pattern, escape, generation_);
  for (unsigned index = 0; index < transient_string_vec_.size(); ++index) {
    if (is_regexp_like(*transient_string_vec_[index], pattern, escape)) {
      result.push_back(transientIndexToId(index));
    }
  }
  return result;
}

int32_t StringDictionaryProxy::getOrAdd(const std::string& str) noexcept {
  return string_dict_->getOrAdd(str);
}

std::pair<const char*, size_t> StringDictionaryProxy::getStringBytes(
    int32_t string_id) const noexcept {
  if (string_id >= 0) {
    return string_dict_.get()->getStringBytes(string_id);
  }
  unsigned const string_index = transientIdToIndex(string_id);
  CHECK_LT(string_index, transient_string_vec_.size());
  std::string const* const str_ptr = transient_string_vec_[string_index];
  return {str_ptr->c_str(), str_ptr->size()};
}

size_t StringDictionaryProxy::storageEntryCount() const {
  const size_t num_storage_entries{generation_ == -1 ? string_dict_->storageEntryCount()
                                                     : generation_};
  CHECK_LE(num_storage_entries, static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  return num_storage_entries;
}

size_t StringDictionaryProxy::transientEntryCountUnlocked() const {
  // CHECK_LE(num_storage_entries,
  // static_cast<size_t>(std::numeric_limits<int32_t>::max()));
  const size_t num_transient_entries{transient_str_to_int_.size()};
  CHECK_LE(num_transient_entries,
           static_cast<size_t>(std::numeric_limits<int32_t>::max()) - 1);
  return num_transient_entries;
}

size_t StringDictionaryProxy::transientEntryCount() const {
  std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
  return transientEntryCountUnlocked();
}

size_t StringDictionaryProxy::entryCountUnlocked() const {
  return storageEntryCount() + transientEntryCountUnlocked();
}

size_t StringDictionaryProxy::entryCount() const {
  std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
  return entryCountUnlocked();
}

// Iterate over transient strings, then non-transients.
void StringDictionaryProxy::eachStringSerially(
    StringDictionary::StringCallback& serial_callback) const {
  constexpr int32_t max_transient_id = -2;
  // Iterate over transient strings.
  for (unsigned index = 0; index < transient_string_vec_.size(); ++index) {
    std::string const& str = *transient_string_vec_[index];
    int32_t const string_id = max_transient_id - index;
    serial_callback(str, string_id);
  }
  // Iterate over non-transient strings.
  string_dict_->eachStringSerially(generation_, serial_callback);
}

// For each (string/_view,old_id) pair passed in:
//  * Get the new_id based on sdp_'s dictionary, or add it as a transient.
//  * The StringDictionary is local, so call the faster getUnlocked() method.
//  * Store the old_id -> new_id translation into the id_map_.
class StringLocalCallback : public StringDictionary::StringCallback {
  StringDictionaryProxy* sdp_;
  StringDictionaryProxy::IdMap& id_map_;

 public:
  StringLocalCallback(StringDictionaryProxy* sdp, StringDictionaryProxy::IdMap& id_map)
      : sdp_(sdp), id_map_(id_map) {}
  void operator()(std::string const& str, int32_t const string_id) override {
    operator()(std::string_view(str), string_id);
  }
  void operator()(std::string_view const sv, int32_t const old_id) override {
    int32_t const new_id = sdp_->string_dict_->getUnlocked(sv);
    id_map_[old_id] = new_id == StringDictionary::INVALID_STR_ID
                          ? sdp_->getOrAddTransientUnlocked(sv)
                          : new_id;
  }
};

// Union strings from both StringDictionaryProxies into *this as transients.
// Return id_map: sdp_rhs:string_id -> this:string_id for each string in sdp_rhs.
StringDictionaryProxy::IdMap StringDictionaryProxy::transientUnion(
    StringDictionaryProxy const& sdp_rhs) {
  IdMap id_map = sdp_rhs.initIdMap();
  // serial_callback cannot be parallelized due to calling getOrAddTransientUnlocked().
  std::unique_ptr<StringDictionary::StringCallback> serial_callback;

  serial_callback = std::make_unique<StringLocalCallback>(this, id_map);
  // Import all non-duplicate strings (transient and non-transient) and add to id_map.
  sdp_rhs.eachStringSerially(*serial_callback);
  return id_map;
}

std::ostream& operator<<(std::ostream& os, StringDictionaryProxy::IdMap const& id_map) {
  return os << "IdMap(offset_(" << id_map.offset_ << ") vector_map_"
            << shared::printContainer(id_map.vector_map_) << ')';
}

void StringDictionaryProxy::updateGeneration(const int64_t generation) noexcept {
  if (generation == -1) {
    return;
  }
  if (generation_ != -1) {
    CHECK_EQ(generation_, generation);
    return;
  }
  generation_ = generation;
}

size_t StringDictionaryProxy::getTransientBulkImpl(
    const std::vector<std::string>& strings,
    int32_t* string_ids,
    const bool take_read_lock) const {
  const size_t num_strings = strings.size();
  if (num_strings == 0) {
    return 0UL;
  }
  // StringDictionary::getBulk returns the number of strings not found
  if (string_dict_->getBulk(strings, string_ids, generation_) == 0UL) {
    return 0UL;
  }

  // If here, dictionary could not find at least 1 target string,
  // now look these up in the transient dictionary
  // transientLookupBulk returns the number of strings not found
  return transientLookupBulk(strings, string_ids, take_read_lock);
}

template <typename String>
size_t StringDictionaryProxy::transientLookupBulk(
    const std::vector<String>& lookup_strings,
    int32_t* string_ids,
    const bool take_read_lock) const {
  const size_t num_strings = lookup_strings.size();
  auto read_lock = take_read_lock ? std::shared_lock<std::shared_mutex>(rw_mutex_)
                                  : std::shared_lock<std::shared_mutex>();

  if (num_strings == static_cast<size_t>(0) || transient_str_to_int_.empty()) {
    return 0UL;
  }
  constexpr size_t tbb_parallel_threshold{20000};
  if (num_strings < tbb_parallel_threshold) {
    return transientLookupBulkUnlocked(lookup_strings, string_ids);
  } else {
    throw std::runtime_error("not support parallel transient lookup.");
  }
}

template <typename String>
size_t StringDictionaryProxy::transientLookupBulkUnlocked(
    const std::vector<String>& lookup_strings,
    int32_t* string_ids) const {
  const size_t num_strings = lookup_strings.size();
  size_t num_strings_not_found = 0;
  for (size_t string_idx = 0; string_idx < num_strings; ++string_idx) {
    if (string_ids[string_idx] != StringDictionary::INVALID_STR_ID) {
      continue;
    }
    // If we're here it means we need to look up this string as we don't
    // have a valid id for it
    string_ids[string_idx] = lookupTransientStringUnlocked(lookup_strings[string_idx]);
    if (string_ids[string_idx] == StringDictionary::INVALID_STR_ID) {
      num_strings_not_found++;
    }
  }
  return num_strings_not_found;
}

StringDictionary* StringDictionaryProxy::getDictionary() const noexcept {
  return string_dict_.get();
}

int64_t StringDictionaryProxy::getGeneration() const noexcept {
  return generation_;
}

bool StringDictionaryProxy::operator==(StringDictionaryProxy const& rhs) const {
  return string_dict_id_ == rhs.string_dict_id_ &&
         transient_str_to_int_ == rhs.transient_str_to_int_;
}

bool StringDictionaryProxy::operator!=(StringDictionaryProxy const& rhs) const {
  return !operator==(rhs);
}
