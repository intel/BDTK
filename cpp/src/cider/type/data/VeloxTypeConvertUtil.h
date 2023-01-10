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

#pragma once

#include <string>

#include "velox/functions/UDFOutputString.h"

namespace cider {

class CiderUDFOutputString : public facebook::velox::UDFOutputString {
 public:
  CiderUDFOutputString() : storage_{} { setData(storage_.data()); }

  CiderUDFOutputString(const CiderUDFOutputString& rh) : storage_{rh.storage_} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  CiderUDFOutputString(CiderUDFOutputString&& rh) noexcept
      : storage_{std::move(rh.storage_)} {
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
  }

  CiderUDFOutputString& operator=(const CiderUDFOutputString& rh) {
    storage_ = rh.storage_;
    reserve(rh.capacity());
    resize(rh.size());
    return *this;
  }

  CiderUDFOutputString& operator=(CiderUDFOutputString&& rh) noexcept {
    storage_ = std::move(rh.storage_);
    setData(storage_.data());
    setSize(rh.size());
    setCapacity(rh.capacity());
    return *this;
  }

  void reserve(size_t size) override {
    storage_.resize(size);
    setData(storage_.data());
    setCapacity(size);
  }

  void finalize() { storage_.resize(size()); }

 private:
  std::string storage_;
};

}  // namespace cider
