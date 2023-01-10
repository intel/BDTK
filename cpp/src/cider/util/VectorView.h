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

/*
 * @file    VectorView.h
 * @description Like string_view but as a vector.
 *   Useful for splitting memory among thread workers.
 *
 */

#pragma once

#include "type/data/funcannotations.h"

#include <cassert>

/**
 * Manage externally allocated memory ranges with a vector-like interface.
 */
template <typename T>
class VectorView {
 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = value_type const&;
  using pointer = value_type*;
  using const_pointer = value_type const*;
  using iterator = pointer;
  using const_iterator = const_pointer;

 private:
  T* data_{nullptr};
  size_type size_{0};
  size_type capacity_{0};

 public:
  VectorView() = default;
  VectorView(T* data, size_type const size, size_type const capacity)
      : data_(data), size_(size), capacity_(capacity) {}
  VectorView(T* data, size_type const size) : VectorView(data, size, size) {}

  T& back() { return data_[size_ - 1]; }
  T const& back() const { return data_[size_ - 1]; }
  T* begin() const { return data_; }
  size_type capacity() const { return capacity_; }
  T const* cbegin() const { return data_; }
  T const* cend() const { return data_ + size_; }
  void clear() { size_ = 0; }
  T* data() { return data_; }
  T const* data() const { return data_; }
  bool empty() const { return size_ == 0; }
  T* end() const { return data_ + size_; }
  bool full() const { return size_ == capacity_; }
  T& front() { return *data_; }
  T const& front() const { return *data_; }
  T& operator[](size_type const i) { return data_[i]; }
  T const& operator[](size_type const i) const { return data_[i]; }
  void push_back(T const& value) { data_[size_++] = value; }
  void resize(size_type const size) {
    assert(size <= capacity_);
    size_ = size;
  }
  // Does not change capacity_.
  void set(T* data, size_type const size) {
    resize(size);
    data_ = data;
  }
  size_type size() const { return size_; }
};
