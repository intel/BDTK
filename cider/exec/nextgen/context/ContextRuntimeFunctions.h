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

#include "exec/module/batch/ArrowABI.h"
#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/RuntimeContext.h"
#include "type/data/funcannotations.h"

extern "C" ALWAYS_INLINE int8_t* get_arrow_array_ptr(int8_t* batch) {
  auto batch_ptr = reinterpret_cast<cider::exec::nextgen::context::Batch*>(batch);
  return reinterpret_cast<int8_t*>(batch_ptr->getArray());
}

extern "C" ALWAYS_INLINE int8_t* get_query_context_ptr(int8_t* context, size_t id) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return reinterpret_cast<int8_t*>(context_ptr->getContextItem(id));
}

extern "C" ALWAYS_INLINE void allocate_arrow_buffer(int8_t* array, int64_t index) {
  auto array_pointer = reinterpret_cast<ArrowArray*>(array);
  auto holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(array_pointer->private_data);
  holder->allocBuffer(index, 1000);
}

extern "C" ALWAYS_INLINE int32_t* extract_arrow_array_buffer(int8_t* arrow_pointer,
                                                             int64_t index) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return reinterpret_cast<int32_t*>(const_cast<void*>(array->buffers[index]));
}

extern "C" ALWAYS_INLINE void* extract_arrow_array_child(int8_t* arrow_pointer,
                                                         int64_t index) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return reinterpret_cast<void*>(array->children[index]);
}

#endif  // NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
