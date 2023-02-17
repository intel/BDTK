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
#pragma once

namespace cider_hashtable {

class HT_Row {
 public:
  HT_Row(int8_t* key_ptr, size_t key_size) : key_ptr_(key_ptr_), key_size_(key_size) {}
  HT_Row() {}
  ~HT_Row() {
    if (nullptr == key_ptr_) {
      delete[] key_ptr_;
      key_ptr_ = nullptr;
    }
  }
  template <typename T>
  void make_row(T& value) {
    if constexpr (std::is_same_v<T, std::string>) {
      key_size_ = value.size();
      if (nullptr == key_ptr_) {
        key_ptr_ = new int8_t[key_size_ + 1];
        std::strcpy(reinterpret_cast<char*>(key_ptr_), value.c_str());
      }
    } else {
      key_size_ = sizeof(T);
      if (nullptr == key_ptr_) {
        key_ptr_ = new int8_t[key_size_];
        memcpy(key_ptr_, &value, key_size_);
      }
    }
  }

  // todo(xinyihe): add append function to support build multi keys Row
  template <typename... Args>
  void make_row_from_multi_key(Args... args) {}

 public:
  int8_t* key_ptr_ = nullptr;
  size_t key_size_ = 0;
};

template <typename KeyType>
struct table_key {
  KeyType key;
  bool is_not_null;
  std::size_t duplicate_num;
};

struct Equal {
  bool operator()(int lhs, int rhs) { return lhs == rhs; }
  bool operator()(std::string lhs, std::string rhs) { return lhs == rhs; }
};

struct HTRowEqual {
  bool operator()(const HT_Row& lhs, const HT_Row& rhs) {
    return (lhs.key_size_ == rhs.key_size_) &&
           (memcmp(lhs.key_ptr_, rhs.key_ptr_, lhs.key_size_) == 0);
  }
};

}  // namespace cider_hashtable