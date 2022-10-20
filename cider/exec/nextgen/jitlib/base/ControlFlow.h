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
#ifndef JITLIB_JIT_CONTROL_FLOW_H
#define JITLIB_JIT_CONTROL_FLOW_H

#include "exec/nextgen/jitlib/base/Values.h"

namespace jitlib {
template <TypeTag, typename FunctionImpl>
class Ret;

template <TypeTag Type = VOID, typename FunctionImpl>
inline void createRet(const FunctionImpl& function) {
  Ret<Type, FunctionImpl> ret(function);
}

template <TypeTag Type, typename FunctionImpl>
inline void createRet(const FunctionImpl& function, Value<Type, FunctionImpl>& value) {
  Ret<Type, FunctionImpl> ret(function, value);
}
};  // namespace jitlib

#endif