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

#ifndef CIDER_EXEC_NEXTGEN_TRANSLATOR_H
#define CIDER_EXEC_NEXTGEN_TRANSLATOR_H

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen {
class QueryContext;
class JITTuple;

/// \brief A translator will generate code for an OpNode
///
/// A translator is associated to a Context and consume a JITTuple, the generate code will
/// save to the Context
class Translator {
 public:
  Translator(const OpNodePtr& node, const std::shared_ptr<QueryContext>& context)
      : node_(node), context_(context) {}

  virtual ~Translator() = default;

  virtual void consume(const JITTuple& input) = 0;

  void setSuccessor(const std::shared_ptr<Translator>& successor) {
    successor_ = successor;
  }

 protected:
  OpNodePtr node_;
  std::shared_ptr<QueryContext> context_;
  std::shared_ptr<Translator> successor_{nullptr};
};
}  // namespace cider::exec::nextgen

#endif  // CIDER_EXEC_NEXTGEN_TRANSLATOR_H
