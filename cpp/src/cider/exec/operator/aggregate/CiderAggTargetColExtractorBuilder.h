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

#ifndef CIDER_CIDERAGGTARGETCOLEXTRACTORBUILDER_H
#define CIDER_CIDERAGGTARGETCOLEXTRACTORBUILDER_H

#include <memory>

#include "exec/operator/aggregate/CiderAggTargetColExtractor.h"

class CiderAggHashTable;

class CiderAggTargetColExtractorBuilder {
 public:
  static std::unique_ptr<CiderAggTargetColExtractor> buildCiderAggTargetColExtractor(
      const CiderAggHashTable* hash_table,
      size_t col_index,
      bool force_double_output = false);

 private:
  static std::unique_ptr<CiderAggTargetColExtractor> buildSimpleAggExtractor(
      const CiderAggHashTable* hash_table,
      size_t col_index,
      bool force_double_output);

  static std::unique_ptr<CiderAggTargetColExtractor> buildAVGAggExtractor(
      const CiderAggHashTable* hash_table,
      size_t col_index);

  static std::unique_ptr<CiderAggTargetColExtractor> buildCountAggExtractor(
      const CiderAggHashTable* hash_table,
      size_t col_index);
};

#endif
