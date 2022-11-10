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

#ifndef NEXTGEN_TRANSLATOR_OPNODE_H
#define NEXTGEN_TRANSLATOR_OPNODE_H

#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen {
class Translator;
class OpNode;

using OpNodePtr = std::shared_ptr<OpNode>;

/// \brief A OpNode is a relational operation in a plan
///
/// Note: Each OpNode has zero or one source
class OpNode {
 public:
  OpNode(const OpNodePtr& input) : input_(input) {}

  virtual ~OpNode() = default;

  /// \brief The name of the operator node
  const char* name() const { return name_; }

  /// \brief Transform the operator to a translator
  virtual std::shared_ptr<Translator> toTranslator() const = 0;

 protected:
  OpNodePtr input_;
  const char* name_;
  // schema
};

using OpNodePtrVector = std::vector<OpNodePtr>;
}  // namespace cider::exec::nextgen

#endif // NEXTGEN_TRANSLATOR_OPNODE_H
