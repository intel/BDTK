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

  virtual void build(
      const std::function<JITValuePointer()>& condition,
      const std::function<void()>& if_true_block,
      const std::function<void()>& else_block = []() {}) = 0;
};

class ForBuilder {
 public:
  virtual ~ForBuilder() = default;

  virtual void build(const std::function<JITValuePointer()>& condition,
                     const std::function<void()>& main_block,
                     const std::function<void()>& update_block) = 0;
};

using IfBuilderPointer = std::unique_ptr<IfBuilder>;
using ForBuilderPointer = std::unique_ptr<ForBuilder>;
};  // namespace cider::jitlib

#endif  // JITLIB_BASE_JITCONTROLFLOW_H
