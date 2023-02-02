/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include <vector>

namespace Buffer_Namespace {
// Memory Pages types in buffer pool
enum MemStatus { FREE, USED };

}  // namespace Buffer_Namespace

namespace Data_Namespace {

struct MemoryData {
  size_t slabNum;
  int32_t startPage;
  size_t numPages;
  uint32_t touch;
  std::vector<int32_t> chunk_key;
  Buffer_Namespace::MemStatus memStatus;
};

struct MemoryInfo {
  size_t pageSize;
  size_t maxNumPages;
  size_t numPageAllocated;
  bool isAllocationCapped;
  std::vector<MemoryData> nodeMemoryData;
};

}  // namespace Data_Namespace
