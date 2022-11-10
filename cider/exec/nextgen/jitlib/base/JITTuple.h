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

#ifndef JITLIB_BASE_JITTUPLE_H
#define JITLIB_BASE_JITTUPLE_H

#include <boost/container/small_vector.hpp>

#include "exec/nextgen/jitlib/base/JITValue.h"
#include "util/Logger.h"

namespace cider::jitlib {
class JITTuple final : public JITBaseValue {
 public:
  JITTuple(JITTypeTag type_tag) : JITBaseValue(type_tag){};

  template <typename ChildValueTpye = JITValue>
  ChildValueTpye& getElementAs(size_t index) const {
    if (index >= children.size()) {
      LOG(FATAL) << "JITTuple access out of boundary by index: " << index
                 << ", tuple size is: " << children.size();
    }
    static_assert(std::is_base_of_v<JITBaseValue, ChildValueTpye>,
                  "Unsupport JITValue type.");

    return *dynamic_cast<ChildValueTpye*>(children.at(index));
  }

  void append(JITBaseValue& child) { children.push_back(&child); };

  void insert(JITBaseValue& child, size_t index) {
    if (index > children.size()) {
      LOG(FATAL) << "JITTuple insert out of boundary by index: " << index
                 << ", tuple size is: " << children.size();
    }
    auto it = children.begin();
    for (size_t i = 0; i < index; i++) {
      it++;
    }
    children.insert(it, &child);
  };

  size_t getSize() { return children.size(); };

 private:
  static constexpr size_t DefaultVectorSize = 4;
  boost::container::small_vector<JITBaseValue*, DefaultVectorSize> children;
};

class JITExprValue {
 public:
  JITExprValue() = default;
  ~JITExprValue() = default;
  JITExprValue(const JITExprValue&) = delete;
  JITExprValue(JITExprValue&&) = delete;

  template <typename T>
  JITExprValue(T&& ptrs, bool is_variadic = false)
      : ptrs_(std::forward<T>(ptrs)), is_variadic_(is_variadic) {}

  // convert JITValuePointer to JITExprValue
  JITExprValue(JITValuePointer&& val) {
    is_variadic_ = false;
    ptrs_.push_back(std::move(val));
  }

  cider::jitlib::JITValue& getValue() {
    CHECK_GT(ptrs_.size(), 0);
    return *ptrs_[0];
  }
  cider::jitlib::JITValue& getNull() {
    CHECK_GT(ptrs_.size(), 1);
    return *ptrs_[1];
  }
  cider::jitlib::JITValue& getLen() {
    CHECK(is_variadic_);
    CHECK_EQ(ptrs_.size(), 3);
    return *ptrs_[2];
  }

 private:
  // fixed witdth column: value and null
  // variadic(eg. string): value, null and len
  std::vector<cider::jitlib::JITValuePointer> ptrs_{};
  bool is_variadic_ = false;
};

};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITVALUE_H