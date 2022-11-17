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

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "exec/nextgen/utils/ReferenceCounter.h"

namespace cider::jitlib {
class JITFunction;
class JITValuePointer;

template <typename ValueType, typename... Args>
auto makeJITValuePointer(Args&&... args) {
  return JITValuePointer(new ValueType(std::forward<Args>(args)...));
}

class JITValue : protected ReferenceCounter {
  friend JITValuePointer;

 public:
  JITValue(JITTypeTag type_tag,
           JITFunction& parent_function,
           const std::string& name,
           JITTypeTag sub_type_tag)
      : value_name_(name)
      , type_tag_(type_tag)
      , parent_function_(parent_function)
      , sub_type_tag_(sub_type_tag) {}

  ~JITValue() override = default;

  const std::string& getValueName() const { return value_name_; }

  JITTypeTag getValueTypeTag() const { return type_tag_; }

  JITTypeTag getValueSubTypeTag() const { return sub_type_tag_; }

  JITFunction& getParentJITFunction() { return parent_function_; }

  JITTypeTag getTypeTag() { return type_tag_; }

  JITValue& operator=(JITValue& rh) { return assign(rh); }

  JITValuePointer operator[](JITValue& index);

 public:
  JITValue(const JITValue&) = delete;
  JITValue(JITValue&&) = delete;
  JITValue& operator=(JITValue&& rh) = delete;

 public:
  virtual void setName(const std::string& name) = 0;
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

 protected:
  std::string value_name_;

 private:
  JITTypeTag type_tag_;
  JITFunction& parent_function_;
  JITTypeTag sub_type_tag_;
};

class JITValuePointer {
  template <typename ValueType, typename... Args>
  friend auto makeJITValuePointer(Args&&... args);

 public:
  JITValuePointer(std::nullptr_t ptr_) : ptr_(ptr_) {}

  JITValuePointer(const JITValuePointer& lh) {
    ptr_ = lh.ptr_;
    ptr_->addRef();
  };

  JITValuePointer(JITValuePointer&& rh) noexcept : ptr_(rh.ptr_) { rh.ptr_ = nullptr; }

  JITValue* get() { return ptr_; }

  size_t getRefNum() { return ptr_->getRefNum(); }

  JITValuePointer replace(const JITValuePointer& rh) {
    if (ptr_ != rh.ptr_) {
      release();
      ptr_ = rh.ptr_;
      ptr_->addRef();
    }
    return *this;
  }

  ~JITValuePointer() { release(); }

 public:
  // Note: To simplify the usage, copy assignment will be deleted and move assignment will
  // be overloaded as assignmet of JITValue. If you want to change ptr_ with another
  // JITValuePointer, please use replace() method.
  JITValuePointer& operator=(const JITValuePointer&) = delete;

  JITValuePointer& operator=(JITValuePointer&& rh) noexcept;

  JITValuePointer& operator=(JITValue& rh) noexcept;

  JITValue& operator*() { return *ptr_; }

  JITValue* operator->() { return ptr_; }

  operator JITValue&() { return *ptr_; }

  JITValuePointer operator[](JITValue& index);

 private:
  void release() {
    if (ptr_) {
      if (ptr_->getRefNum() == 1) {
        delete ptr_;
      }
      ptr_->decRef();
    }
  }

  JITValuePointer(JITValue* value) : ptr_(value) {
    if (ptr_) {
      ptr_->addRef();
    }
  }

  JITValue* ptr_;
};

inline JITValuePointer JITValue::operator[](JITValue& index) {
  return getElemAt(index);
}

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

class JITExprValue {
 public:
  JITExprValue() = default;
  ~JITExprValue() = default;
  JITExprValue(const JITExprValue&) = delete;
  JITExprValue(JITExprValue&&) = delete;

  // for vector<JITValuePointer>;
  template <typename T>
  JITExprValue(T&& ptrs, bool is_variadic = false)
      : ptrs_(std::forward<T>(ptrs)), is_variadic_(is_variadic) {}

  // for {JITValuePointer, ...}
  template <typename... T>
  JITExprValue(T&&... ptrs, bool is_variadic = false) : is_variadic_(is_variadic) {
    (ptrs_.emplace_back(std::forward<T>(ptrs)), ...);
  }

  // for JITValuePointer
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
