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
#ifndef NEXTGEN_UTILS_FUNCTORUTILS_H
#define NEXTGEN_UTILS_FUNCTORUTILS_H

#include <utility>

namespace cider::exec::nextgen::utils {
template <typename FuncT>
struct RecursiveFunctor {
  template <typename... Args>
  decltype(auto) operator()(Args&&... args) const {
    return func(*this, std::forward<Args>(args)...);
  }

  FuncT func;
};

// Deduction guidance for usage likes RecursiveFunctor{[](){}};
template <typename FuncT>
RecursiveFunctor(FuncT)->RecursiveFunctor<FuncT>;
}  // namespace cider::exec::nextgen::utils

#endif  // NEXTGEN_UTILS_FUNCTORUTILS_H
