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

#ifndef CIDER_EXEC_NEXTGEN_TRANSFORMER_H
#define CIDER_EXEC_NEXTGEN_TRANSFORMER_H

#include "exec/nextgen/OpPipeline.h"

namespace cider::exec::nextgen {

/// \brief The Transformer class is responsible for transforming the OpPipeline to a
/// translator
class Transformer {
 public:
    static std::shared_ptr<Translator> toTranslator(const OpPipeline& pipeline) {
      std::shared_ptr<Translator> root = nullptr;
      std::shared_ptr<Translator> last = nullptr;
      for (const auto& node : pipeline.getOpNodes()) {
        // auto translator = node->toTranslator();
        if (root == nullptr) {
          // root = last = translator;
        } else {
          // last->setSuccessor(translator);
          // last = translator;
        }
      }
      return root;
    }
};

}  // namespace cider::exec::nextgen

#endif  // CIDER_EXEC_NEXTGEN_TRANSFORMER_H
