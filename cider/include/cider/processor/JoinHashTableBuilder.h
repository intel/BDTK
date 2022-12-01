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

#ifndef CIDER_JOIN_HASH_TABLE_BUILDER_H
#define CIDER_JOIN_HASH_TABLE_BUILDER_H

#include <memory>
#include "include/cider/batch/CiderBatch.h"
#include "substrait/algebra.pb.h"

namespace cider::processor {

class JoinHashTable;

class JoinHashTableBuilder {
 public:
  virtual void appendBatch(std::shared_ptr<CiderBatch> batch) = 0;

  virtual std::unique_ptr<JoinHashTable> build() = 0;
};

/// Factory method to create an instance of  JoinHashTableBuilder
std::shared_ptr<JoinHashTableBuilder> makeJoinHashTableBuilder(
    const ::substrait::JoinRel& joinRel);

}  // namespace cider::processor

#endif  // CIDER_JOIN_HASH_TABLE_BUILDER_H
