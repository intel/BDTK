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

#ifndef CIDER_EXEC_NEXTGEN_OP_NODE_H
#define CIDER_EXEC_NEXTGEN_OP_NODE_H

#include <memory>
#include <vector>

#include "type/plan/Analyzer.h"

namespace cider::exec::nextgen {

/// \brief A OpNode is a relational operation in a plan
///
/// Note: Each OpNode has zero or one source
class OpNode {
 public:
  OpNode() = default;
  virtual ~OpNode() = default;

  /// \brief The name of the operator node
  virtual const char* name() const = 0;

 protected:
  // input_;
  // schema_;
};

using OpNodePtr = std::shared_ptr<OpNode>;
using OpNodeVector = std::vector<OpNodePtr>;

class FilterNode : public OpNode {
 public:
  FilterNode() : OpNode() {}

  FilterNode(std::vector<Analyzer::Expr*>& filter) : filter_(std::move(filter)) {}

  const char* name() const override { return "FilterNode"; }

 private:
  std::vector<Analyzer::Expr*> filter_;
};

}  // namespace cider::exec::nextgen

#endif  // CIDER_EXEC_NEXTGEN_OP_NODE_H
