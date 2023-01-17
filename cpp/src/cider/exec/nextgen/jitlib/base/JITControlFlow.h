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
#ifndef JITLIB_BASE_JITCONTROLFLOW_H
#define JITLIB_BASE_JITCONTROLFLOW_H

#include <functional>

#include "exec/nextgen/jitlib/base/JITValue.h"

namespace cider::jitlib {  // namespace cider::jitlib
class IfBuilder {
 public:
  virtual ~IfBuilder() = default;

  virtual void build() = 0;

  template <
      typename T,
      typename std::enable_if_t<std::is_invocable_r_v<JITValuePointer, T>, bool> = true>
  IfBuilder* condition(T&& fun) {
    condition_ = fun;
    return this;
  }

  template <typename T, typename std::enable_if_t<std::is_invocable_v<T>, bool> = true>
  IfBuilder* ifTrue(T&& fun) {
    if_true_ = fun;
    return this;
  }

  template <typename T, typename std::enable_if_t<std::is_invocable_v<T>, bool> = true>
  IfBuilder* ifFalse(T&& fun) {
    if_false_ = fun;
    return this;
  }

 protected:
  std::function<JITValuePointer()> condition_;
  std::function<void()> if_true_;
  std::function<void()> if_false_;
};

class LoopBuilder {
 public:
  virtual ~LoopBuilder() = default;

  virtual void build() = 0;

  template <
      typename T,
      typename std::enable_if_t<std::is_invocable_r_v<JITValuePointer, T>, bool> = true>
  LoopBuilder* condition(T&& fun) {
    condition_ = fun;
    return this;
  }

  template <typename T,
            typename std::enable_if_t<std::is_invocable_v<T, LoopBuilder*>, bool> = true>
  LoopBuilder* loop(T&& fun) {
    loop_body_ = fun;
    return this;
  }

  template <typename T, typename std::enable_if_t<std::is_invocable_v<T>, bool> = true>
  LoopBuilder* update(T&& fun) {
    update_ = fun;
    return this;
  }

  virtual void loopContinue() = 0;

 protected:
  std::function<JITValuePointer()> condition_;
  std::function<void(LoopBuilder*)> loop_body_;
  std::function<void()> update_;
};

using IfBuilderPointer = std::unique_ptr<IfBuilder>;
using LoopBuilderPointer = std::unique_ptr<LoopBuilder>;
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITCONTROLFLOW_H
