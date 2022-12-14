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

#include "CacheInvalidator.h"

/**
 * External cache invalidators clear caches not managed by the Buffer Manager (i.e.
 * external to the buffer manager).
 */

// Classes that are involved in needing a cache invalidated
#include "exec/template/operator/join/hashtable/BaselineJoinHashTable.h"
#include "exec/template/operator/join/hashtable/OverlapsJoinHashTable.h"
#include "exec/template/operator/join/hashtable/PerfectJoinHashTable.h"

// Note that this is functionally the same as the above two invalidators. The
// JoinHashTableCacheInvalidator is a generic invalidator used during `clear_cpu` calls.
// The above cache invalidators are specific invalidators called during update/delete and
// will likely be extended in the future.
using JoinHashTableCacheInvalidator =
    CacheInvalidator<OverlapsJoinHashTable, BaselineJoinHashTable, PerfectJoinHashTable>;
