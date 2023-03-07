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

#include <limits>
#include <stdexcept>
#include <type_traits>

/* `../` is required for UDFCompiler */
// #include "cider/CiderException.h"
// #include "InlineNullValues.h"
#include "funcannotations.h"

#define EXTENSION_INLINE extern "C" RUNTIME_EXPORT ALWAYS_INLINE
#define EXTENSION_NOINLINE extern "C" RUNTIME_EXPORT NEVER_INLINE
#define TEMPLATE_INLINE ALWAYS_INLINE
#define TEMPLATE_NOINLINE NEVER_INLINE

EXTENSION_NOINLINE int8_t* allocate_varlen_buffer(int64_t element_count,
                                                  int64_t element_size);
