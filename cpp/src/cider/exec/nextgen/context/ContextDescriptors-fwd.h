/*
 * Copyright(c) 2022-2023 Intel Corporation.
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
#ifndef NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H
#define NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H

#include <functional>
#include <memory>

#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/Buffer.h"
#include "exec/nextgen/context/CiderSet.h"

namespace cider::exec::processor {
class JoinHashTable;
};

namespace cider::exec::nextgen::context {

struct AggExprsInfo;

using TrimCharMapsPtr = std::shared_ptr<std::vector<std::vector<int8_t>>>;

struct BatchDescriptor;
struct BufferDescriptor;
struct HashTableDescriptor;
struct CrossJoinBuildTableDescriptor;
struct CiderSetDescriptor;

using BatchDescriptorPtr = std::shared_ptr<BatchDescriptor>;
using BufferDescriptorPtr = std::shared_ptr<BufferDescriptor>;
using HashTableDescriptorPtr = std::shared_ptr<HashTableDescriptor>;
using BuildTableDescriptorPtr = std::shared_ptr<CrossJoinBuildTableDescriptor>;
using CiderSetDescriptorPtr = std::shared_ptr<CiderSetDescriptor>;

};  // namespace cider::exec::nextgen::context

#endif  // NEXTGEN_CONTEXT_CONTEXTDESCRIPTORS_FWD_H
