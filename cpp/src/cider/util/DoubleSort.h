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

// Used for creating iterators that can be used to sort two vectors.
// In other words, double_sort::Iterator<> is a writable zip iterator.

#pragma once

#include "type/data/funcannotations.h"

#include <algorithm>
#include <iterator>
#include <numeric>
#include <ostream>
#include <vector>

// Overriding swap() isn't sufficient, since std:sort() uses insertion sort
// when the number of elements is 16 or less which bypasses swap.

// Iterator sent to sort() sorts two containers simultaneously.
namespace double_sort {

template <typename T>
union Variant {
  T* ptr_;
  T value_;
};

// Named Value insofar as it is the return value of Iterator::operator*().
// Default and copy/move constructors initial to a value (ref_=false).
template <typename T0, typename T1>
struct Value {
  Variant<T0> v0_;
  Variant<T1> v1_;
  bool const ref_;  // Use ptr_ if true, else value_.
  Value(T0* ptr0, T1* ptr1) : v0_{ptr0}, v1_{ptr1}, ref_(true) {}
  // thrust::sort() copies Values, std::sort() moves Values.
  Value(Value&& b) : ref_(false) {
    v0_.value_ = b.ref_ ? std::move(*b.v0_.ptr_) : std::move(b.v0_.value_);
    v1_.value_ = b.ref_ ? std::move(*b.v1_.ptr_) : std::move(b.v1_.value_);
  }
  Value& operator=(Value&& b) {
    if (ref_) {
      *v0_.ptr_ = b.ref_ ? std::move(*b.v0_.ptr_) : std::move(b.v0_.value_);
      *v1_.ptr_ = b.ref_ ? std::move(*b.v1_.ptr_) : std::move(b.v1_.value_);
    } else {
      v0_.value_ = b.ref_ ? std::move(*b.v0_.ptr_) : std::move(b.v0_.value_);
      v1_.value_ = b.ref_ ? std::move(*b.v1_.ptr_) : std::move(b.v1_.value_);
    }
    return *this;
  }
  T0 value0() const { return ref_ ? *v0_.ptr_ : v0_.value_; }
  T1 value1() const { return ref_ ? *v1_.ptr_ : v1_.value_; }
};

template <typename T0, typename T1>
std::ostream& operator<<(std::ostream& out, Value<T0, T1> const& ds) {
  return out << "ref_(" << ds.ref_ << ") v0_.value_(" << ds.v0_.value_ << ") v1_.value_("
             << ds.v1_.value_ << ')' << std::endl;
}

template <typename T0, typename T1>
struct Iterator : public std::iterator<std::input_iterator_tag, Value<T0, T1>> {
  Value<T0, T1> this_;  // this_ is always a reference object. I.e. this_.ref_ == true.
  Iterator(T0* ptr0, T1* ptr1) : this_(ptr0, ptr1) {}
  Iterator(Iterator const& b) : this_(b.this_.v0_.ptr_, b.this_.v1_.ptr_) {}
  Iterator(Iterator&& b) : this_(b.this_.v0_.ptr_, b.this_.v1_.ptr_) {}
  Iterator& operator=(Iterator const& b) {
    this_.v0_.ptr_ = b.this_.v0_.ptr_;
    this_.v1_.ptr_ = b.this_.v1_.ptr_;
    return *this;
  }
  Iterator& operator=(Iterator&& b) {
    this_.v0_.ptr_ = b.this_.v0_.ptr_;
    this_.v1_.ptr_ = b.this_.v1_.ptr_;
    return *this;
  }
  // Returns a reference object by reference
  Value<T0, T1>& operator*() const { return const_cast<Iterator<T0, T1>*>(this)->this_; }
  // Required by thrust::sort().
  // Returns a reference object by value
  Value<T0, T1> operator[](int i) const { return operator+(i).this_; }
  Iterator& operator++() {
    ++this_.v0_.ptr_;
    ++this_.v1_.ptr_;
    return *this;
  }
  // Required by thrust::sort().
  Iterator& operator+=(int i) {
    this_.v0_.ptr_ += i;
    this_.v1_.ptr_ += i;
    return *this;
  }
  Iterator& operator--() {
    --this_.v0_.ptr_;
    --this_.v1_.ptr_;
    return *this;
  }

  auto operator-(Iterator const& b) const { return this_.v0_.ptr_ - b.this_.v0_.ptr_; }

  Iterator operator+(int i) const { return {this_.v0_.ptr_ + i, this_.v1_.ptr_ + i}; }

  Iterator operator-(int i) const { return {this_.v0_.ptr_ - i, this_.v1_.ptr_ - i}; }

  bool operator==(Iterator const& b) const { return this_.v0_.ptr_ == b.this_.v0_.ptr_; }

  bool operator!=(Iterator const& b) const { return this_.v0_.ptr_ != b.this_.v0_.ptr_; }

  bool operator<(Iterator const& b) const { return this_.v0_.ptr_ < b.this_.v0_.ptr_; }
  // Required by MacOS /usr/local/opt/llvm/include/c++/v1/algorithm:4036

  bool operator>(Iterator const& b) const { return this_.v0_.ptr_ > b.this_.v0_.ptr_; }
  // Required by MacOS /usr/local/opt/llvm/include/c++/v1/algorithm:4000

  bool operator>=(Iterator const& b) const { return this_.v0_.ptr_ >= b.this_.v0_.ptr_; }
};

}  // namespace double_sort
