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
#ifndef JITLIB_VALUES_H
#define JITLIB_VALUES_H

#include "exec/nextgen/jitlib/base/ValueTypes.h"

namespace jitlib {

// TODO (bigPYJ1151): Move Type out from template list.
template <TypeTag, typename FunctionImpl>
class Value;

template <typename ValueImpl, TypeTag Type, typename FunctionImpl>
class BasicValue {
 public:
  using NativeType = typename TypeTraits<Type>::NativeType;

  static ValueImpl createVariable(FunctionImpl& function,
                                  const char* name,
                                  NativeType init) {
    return ValueImpl::createVariableImpl(function, name, init);
  }
};

template <TypeTag Type,
          typename FunctionImpl,
          typename NativeType = typename TypeTraits<Type>::NativeType>
inline Value<Type, FunctionImpl> createVariable(FunctionImpl& function,
                                                const char* name,
                                                NativeType init) {
  return Value<Type, FunctionImpl>::createVariable(function, name, init);
}

};  // namespace jitlib

#endif