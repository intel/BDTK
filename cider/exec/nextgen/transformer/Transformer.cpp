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
#include "exec/nextgen/transformer/Transformer.h"
#include <algorithm>

namespace cider::exec::nextgen::transformer {

static TranslatorPtr generateTranslators(OpPipeline& pipeline) {
  CHECK_GT(pipeline.size(), 0);

  OpNodePtr input = nullptr;
  std::for_each(pipeline.begin(), pipeline.end(), [&input](const OpNodePtr& op) {
    op->setInputOpNode(input);
    input = op;
  });

  TranslatorPtr ptr = nullptr;
  std::for_each(pipeline.rbegin(), pipeline.rend(), [&ptr](const OpNodePtr& op) {
    auto translator = op->toTranslator(ptr);
    ptr = translator;
  });

  return ptr;
}

TranslatorPtr Transformer::toTranslator(OpPipeline& pipeline) {
  // TODO (bigPYJ1151): Insert C2ROp, R2COp
  return generateTranslators(pipeline);
}
}  // namespace cider::exec::nextgen::transformer
