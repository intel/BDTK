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

#include "exec/nextgen/operators/ColSourceNode.h"
#include "exec/nextgen/operators/ProjectNode.h"
#include "exec/nextgen/parsers/Parser.h"
#include "exec/template/common/descriptors/InputDescriptors.h"
#include "util/Logger.h"

namespace cider::exec::nextgen::parsers {

using namespace cider::exec::nextgen::operators;

static void insertSourceNode(const std::vector<InputColDescriptor>& input_descs,
                             OpPipeline& pipeline) {
  InputAnalyzer<ColSourceNode> analyzer(input_descs, pipeline);
  analyzer.run();
}

operators::OpPipeline toOpPipeline(const ExprPtrVector& expr,
                                   const std::vector<InputColDescriptor>& input_descs) {
  OpPipeline ops;

  ops.emplace_back(createOpNode<operators::ProjectNode>(expr));

  insertSourceNode(input_descs, ops);

  return ops;
}

}  // namespace cider::exec::nextgen::parsers
