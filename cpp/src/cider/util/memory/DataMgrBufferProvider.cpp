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

#include "DataMgrBufferProvider.h"

#include "DataMgr.h"

using namespace Data_Namespace;

void DataMgrBufferProvider::free(AbstractBuffer* buffer) {
  CHECK(data_mgr_);
  data_mgr_->free(buffer);
}

AbstractBuffer* DataMgrBufferProvider::alloc(const MemoryLevel memory_level,
                                             const int device_id,
                                             const size_t num_bytes) {
  CHECK(data_mgr_);
  return data_mgr_->alloc(memory_level, device_id, num_bytes);
}
