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
#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_DUMMY_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_DUMMY_H

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "exec/nextgen/jitlib/base/JITFunction.h"
#include "exec/nextgen/jitlib/JITLib.h"
#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen::operators {
using namespace cider::jitlib;

class Context {
 public:
  Context(JITFunction* func_) : query_func_(func_) {}
  JITFunction* query_func_;
  std::vector<cider::jitlib::JITExprValue*> expr_outs_;
};

class OpNode {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;
  OpNode() = default;
  virtual ~OpNode() = default;
};

template <typename T, typename ST>
struct is_vector_of {
  using type = typename std::remove_reference<T>::type;
  static constexpr bool v = std::is_same_v<type, std::vector<ST>>;
};
template <typename T, typename ST>
inline constexpr bool is_vector_of_v = is_vector_of<T, ST>::v;

template <typename T, typename ST>
using IsVecOf = typename std::enable_if_t<is_vector_of_v<T, ST>, bool>;

class Translator {
 public:
  using ExprPtr = std::shared_ptr<Analyzer::Expr>;

  Translator() = default;
  virtual ~Translator() = default;

  virtual void consume(Context& context) = 0;
};

}  // namespace cider::exec::nextgen::operators
#endif
