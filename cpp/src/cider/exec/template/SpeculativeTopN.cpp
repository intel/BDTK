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

#include "SpeculativeTopN.h"

#include "ResultSet.h"
#include "util/Logger.h"

SpeculativeTopNMap::SpeculativeTopNMap() : unknown_(0) {}

void SpeculativeTopNMap::reduce(SpeculativeTopNMap& that) {
  for (auto& kv : map_) {
    auto& this_entry = kv.second;
    const auto that_it = that.map_.find(kv.first);
    if (that_it != that.map_.end()) {
      const auto& that_entry = that_it->second;
      CHECK(!that_entry.unknown);
      this_entry.val += that_entry.val;
      that.map_.erase(that_it);
    } else {
      this_entry.val += that.unknown_;
      this_entry.unknown = that.unknown_;
    }
  }
  for (const auto& kv : that.map_) {
    const auto it_ok = map_.emplace(
        kv.first, SpeculativeTopNVal{kv.second.val + unknown_, unknown_ != 0});
    CHECK(it_ok.second);
  }
  unknown_ += that.unknown_;
}

void SpeculativeTopNBlacklist::add(const std::shared_ptr<Analyzer::Expr> expr,
                                   const bool desc) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& e : blacklist_) {
    CHECK(!(*e.first == *expr) || e.second != desc);
  }
  blacklist_.emplace_back(expr, desc);
}

bool SpeculativeTopNBlacklist::contains(const std::shared_ptr<Analyzer::Expr> expr,
                                        const bool desc) const {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& e : blacklist_) {
    if (*e.first == *expr && e.second == desc) {
      return true;
    }
  }
  return false;
}
