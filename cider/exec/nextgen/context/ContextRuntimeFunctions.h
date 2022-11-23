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
#ifndef NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
#define NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H

#include "exec/nextgen/context/RuntimeContext.h"
#include "type/data/funcannotations.h"

extern "C" ALWAYS_INLINE int8_t* get_query_context_ptr(int8_t* context, size_t id) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return reinterpret_cast<int8_t*>(context_ptr->getContextItem(id));
}

#endif  // NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
