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

#ifndef EXEC_NEXTGEN_OPPIPELINE_H
#define EXEC_NEXTGEN_OPPIPELINE_H

#include <memory>
#include <vector>

#include "exec/nextgen/operators/OpNode.h"

namespace cider::exec::nextgen {

/// \brief An OpPipeline is a linear sequence of relational operators that operate on
/// tuple data
class OpPipeline {
 public:
  OpPipeline() = default;

  explicit OpPipeline(const OpNodePtrVector& nodes) : nodes_(nodes) {}

  virtual ~OpPipeline() = default;

  /// \brief Register an operator node in this pipeline
  ///
  /// \input opNode The operator node to add to the pipeline
  void appendOpNode(OpNodePtr node) { nodes_.push_back(node); }

  const OpNodePtrVector& getOpNodes() const { return nodes_; }

 private:
  OpNodePtrVector nodes_;
};

using OpPipelinePtr = std::unique_ptr<OpPipeline>;
}  // namespace cider::exec::nextgen

#endif  // EXEC_NEXTGEN_OPPIPELINE_H
