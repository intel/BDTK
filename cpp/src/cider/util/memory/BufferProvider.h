/*
 * Copyright (c) 2022 Intel Corporation.
 * Copyright (c) OmniSci, Inc. and its affiliates.
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

#pragma once

#include "util/MemoryLevel.h"
#include "util/memory/Buffer/AbstractBuffer.h"

using namespace Data_Namespace;

class BufferProvider {
 public:
  virtual ~BufferProvider() = default;

  // allocator APIs (ThrustAllocator)
  virtual void free(AbstractBuffer* buffer) = 0;
  virtual AbstractBuffer* alloc(const MemoryLevel memory_level,
                                const int device_id,
                                const size_t num_bytes) = 0;
};
