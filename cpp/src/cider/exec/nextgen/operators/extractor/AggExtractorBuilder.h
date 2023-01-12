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

#ifndef NEXTGEN_AGG_EXTRACTOR_BUILDER_H
#define NEXTGEN_AGG_EXTRACTOR_BUILDER_H

#include "exec/nextgen/context/CodegenContext.h"
#include "exec/nextgen/operators/extractor/AggExtractor.h"

namespace cider::exec::nextgen::operators {
class NextgenAggExtractorBuilder {
 public:
  static std::unique_ptr<NextgenAggExtractor> buildNextgenAggExtractor(
      const int8_t* buffer,
      context::AggExprsInfo& info);

 private:
  static std::unique_ptr<NextgenAggExtractor> buildBasicAggExtractor(
      const int8_t* buffer,
      context::AggExprsInfo& info);

  static std::unique_ptr<NextgenAggExtractor> buildAVGAggExtractor(const int8_t* buffer);
};
}  // namespace cider::exec::nextgen::operators

#endif  // NEXTGEN_AGG_EXTRACTOR_BUILDER_H
