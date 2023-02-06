/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#ifndef NEXTGEN_UTILS_JITEXPRVALUE_H
#define NEXTGEN_UTILS_JITEXPRVALUE_H

#include "exec/nextgen/jitlib/base/JITValue.h"

namespace cider::exec::nextgen::utils {
enum class JITExprValueType { ROW, BATCH };

class JITExprValue {
  template <typename T>
  using IsJITValuePointer = typename std::enable_if_t<
      std::is_same_v<typename std::remove_reference<T>::type, jitlib::JITValuePointer>,
      bool>;

 public:
  explicit JITExprValue(size_t size = 0, JITExprValueType type = JITExprValueType::ROW)
      : ptrs_(0), value_type_(type) {
    ptrs_.reserve(size);
  }

  // for {JITValuePointer, ...}
  template <typename... T>
  explicit JITExprValue(JITExprValueType type, T&&... ptrs) {
    value_type_ = type;
    append(std::forward<T>(ptrs)...);
  }

  template <typename... T>
  JITExprValue& append(T&&... values) {
    ptrs_.reserve(sizeof...(values));
    (ptrs_.emplace_back(jitlib::JITValuePointer(values)), ...);
    return *this;
  }

  JITExprValue& operator=(const JITExprValue& rh) {
    if (this != &rh) {
      resize(rh.size());
      value_type_ = rh.value_type_;
      for (size_t i = 0; i < rh.size(); ++i) {
        ptrs_[i].replace(rh[i]);
      }
    }

    return *this;
  }

  void resize(size_t attributes_num) { ptrs_.resize(attributes_num); }

  size_t size() const { return ptrs_.size(); }

  bool empty() { return ptrs_.empty(); }

  void clear() { resize(0); }

  operator bool() { return size(); }

  jitlib::JITValuePointer& operator[](size_t index) { return ptrs_[index]; }

  const jitlib::JITValuePointer& operator[](size_t index) const { return ptrs_[index]; }

 private:
  std::vector<cider::jitlib::JITValuePointer> ptrs_{};
  JITExprValueType value_type_{JITExprValueType::ROW};
};

class JITExprValueAdaptor {
 public:
  explicit JITExprValueAdaptor(JITExprValue& values) : values_(values) {}

  jitlib::JITValuePointer& getNull() { return values_[0]; }

  void setNull(const jitlib::JITValuePointer& rh) { values_[0].replace(rh); }

 protected:
  JITExprValue& values_;
};

class FixSizeJITExprValue : public JITExprValueAdaptor {
 public:
  explicit FixSizeJITExprValue(JITExprValue& values) : JITExprValueAdaptor(values) {
    values_.resize(2);
  }

  jitlib::JITValuePointer& getValue() { return values_[1]; }

  void setValue(const jitlib::JITValuePointer& rh) { values_[1].replace(rh); }
};

class VarSizeJITExprValue : public JITExprValueAdaptor {
 public:
  explicit VarSizeJITExprValue(JITExprValue& values) : JITExprValueAdaptor(values) {
    values_.resize(3);
  }
  jitlib::JITValuePointer& getLength() { return values_[1]; }

  jitlib::JITValuePointer& getValue() { return values_[2]; }
};

class VarSizeArrayExprValue : public JITExprValueAdaptor {
 public:
  explicit VarSizeArrayExprValue(JITExprValue& values) : JITExprValueAdaptor(values) {
    values_.resize(4);
  }
  // offsets buffer
  jitlib::JITValuePointer& getOffsets() { return values_[1]; }

  jitlib::JITValuePointer& getElemNull() { return values_[2]; }

  jitlib::JITValuePointer& getValue() { return values_[3]; }
};

}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_JITEXPRVALUE_H
