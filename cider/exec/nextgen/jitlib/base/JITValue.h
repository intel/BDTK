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
#ifndef JITLIB_BASE_JITVALUE_H
#define JITLIB_BASE_JITVALUE_H

#include <memory>

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace cider::jitlib {
class JITValue;
class JITFunction;

class JITBaseValue {
 public:
  JITBaseValue(JITTypeTag type_tag) : type_tag_(type_tag) {}
  virtual ~JITBaseValue() = default;

 protected:
  JITTypeTag type_tag_;
};

class JITValuePointer {
 public:
  JITValuePointer(std::nullptr_t ptr) : ptr_(ptr) {}

  template <typename T>
  JITValuePointer(std::unique_ptr<T>&& ptr) : ptr_(ptr.release()) {}

  JITValuePointer(const JITValuePointer&) = delete;

  JITValuePointer(JITValuePointer&& rh) noexcept : ptr_(rh.ptr_.release()) {}

  JITValue* get() { return ptr_.get(); }

 public:
  JITValuePointer& operator=(const JITValuePointer&) = delete;

  JITValuePointer& operator=(JITValuePointer&& rh) noexcept;
  JITValuePointer& operator=(JITValue& rh) noexcept;

  JITValue& operator*() { return *ptr_; }

  JITValue* operator->() { return ptr_.get(); }

  operator JITValue&() { return *ptr_; }

  JITValuePointer operator[](JITValue& index);

 private:
  std::unique_ptr<JITValue> ptr_;
};

class JITValue : public JITBaseValue {
 public:
  JITValue(JITTypeTag type_tag,
           JITFunction& parent_function,
           const std::string& name,
           JITTypeTag sub_type_tag)
      : JITBaseValue(type_tag)
      , value_name_(name)
      , parent_function_(parent_function)
      , sub_type_tag_(sub_type_tag) {}

  virtual ~JITValue() = default;

  const std::string& getValueName() const { return value_name_; }

  JITTypeTag getValueTypeTag() const { return type_tag_; }

  JITTypeTag getValueSubTypeTag() const { return sub_type_tag_; }

  JITFunction& getParentJITFunction() { return parent_function_; }

  JITValue& operator=(JITValue& rh) { return assign(rh); }
  JITValuePointer operator[](JITValue& index) { return getElemAt(index); }

 public:
  JITValue(const JITValue&) = delete;
  JITValue(JITValue&&) = delete;
  JITValue& operator=(JITValue&& rh) = delete;

 public:
  virtual JITValue& assign(JITValue& value) = 0;
  virtual JITValuePointer getElemAt(JITValue& index) = 0;

  // // Logical Operators
  virtual JITValuePointer andOp(JITValue& rh) = 0;
  virtual JITValuePointer orOp(JITValue& rh) = 0;
  virtual JITValuePointer notOp() = 0;

  // // Arithmetic Operations
  // TBD: Overflow-check related Arithmetic Operations.
  virtual JITValuePointer add(JITValue& rh) = 0;
  virtual JITValuePointer sub(JITValue& rh) = 0;
  virtual JITValuePointer mul(JITValue& rh) = 0;
  virtual JITValuePointer div(JITValue& rh) = 0;
  virtual JITValuePointer mod(JITValue& rh) = 0;

  // // Compare Operators
  virtual JITValuePointer eq(JITValue& rh) = 0;
  virtual JITValuePointer ne(JITValue& rh) = 0;
  virtual JITValuePointer lt(JITValue& rh) = 0;
  virtual JITValuePointer le(JITValue& rh) = 0;
  virtual JITValuePointer gt(JITValue& rh) = 0;
  virtual JITValuePointer ge(JITValue& rh) = 0;

  // Pointer Operators
  virtual JITValuePointer castPointerSubType(JITTypeTag type_tag) = 0;
  virtual JITValuePointer dereference() = 0;

 private:
  std::string value_name_;
  JITFunction& parent_function_;
  JITTypeTag sub_type_tag_;
};

inline JITValuePointer& JITValuePointer::operator=(JITValuePointer&& rh) noexcept {
  *ptr_ = *rh;
  return *this;
}

inline JITValuePointer& JITValuePointer::operator=(JITValue& rh) noexcept {
  *ptr_ = rh;
  return *this;
}

inline JITValuePointer JITValuePointer::operator[](JITValue& index) {
  return (*ptr_)[index];
}

};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITVALUE_H
