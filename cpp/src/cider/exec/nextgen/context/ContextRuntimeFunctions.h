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
#ifndef NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
#define NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H

#include "exec/module/batch/CiderArrowBufferHolder.h"
#include "exec/nextgen/context/Batch.h"
#include "exec/nextgen/context/Buffer.h"
#include "exec/nextgen/context/RuntimeContext.h"
#include "type/data/funcannotations.h"

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_query_context_item_ptr(int8_t* context,
                                                                        size_t id) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return reinterpret_cast<int8_t*>(context_ptr->getContextItem(id));
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_query_context_string_heap_ptr(
    int8_t* context) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return reinterpret_cast<int8_t*>(context_ptr->getStringHeapPtr());
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* cast_int64_to_ptr(int64_t address) {
  return (int8_t*)address;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t cast_ptr_to_int64(int8_t* ptr) {
  return (int64_t)ptr;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_query_context_trim_char_map_by_id(
    int8_t* context,
    int id) {
  auto context_ptr =
      reinterpret_cast<cider::exec::nextgen::context::RuntimeContext*>(context);
  return const_cast<int8_t*>(context_ptr->getTrimStringOperCharMapById(id));
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_arrow_array_ptr(int8_t* batch) {
  auto batch_ptr = reinterpret_cast<cider::exec::nextgen::context::Batch*>(batch);
  return reinterpret_cast<int8_t*>(batch_ptr->getArray());
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void* extract_arrow_array_buffer(
    int8_t* arrow_pointer,
    int64_t index) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return const_cast<void*>(array->buffers[index]);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void* extract_arrow_array_child(
    int8_t* arrow_pointer,
    int64_t index) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return reinterpret_cast<void*>(array->children[index]);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void set_arrow_array_child(int8_t* arrow_pointer,
                                                                int8_t* child_pointer,
                                                                int64_t index) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  ArrowArray* child = reinterpret_cast<ArrowArray*>(child_pointer);
  array->children[index] = child;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* extract_arrow_array_dictionary(
    int8_t* arrow_pointer) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return reinterpret_cast<int8_t*>(array->dictionary);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int64_t
extract_arrow_array_len(int8_t* arrow_pointer) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  return array->length;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void set_arrow_array_len(int8_t* arrow_pointer,
                                                              int64_t len) {
  ArrowArray* array = reinterpret_cast<ArrowArray*>(arrow_pointer);
  array->length = len;
}

extern "C" RUNTIME_FUNC ALLOW_INLINE void
allocate_arrow_array_buffer(int8_t* array, int64_t buffer_index, int64_t bytes) {
  auto array_pointer = reinterpret_cast<ArrowArray*>(array);
  auto holder =
      reinterpret_cast<CiderArrowArrayBufferHolder*>(array_pointer->private_data);
  holder->allocBuffer(buffer_index, bytes);
}

extern "C" RUNTIME_FUNC ALLOW_INLINE int8_t* get_under_level_buffer_ptr(int8_t* buffer) {
  auto batch_ptr = reinterpret_cast<cider::exec::nextgen::context::Buffer*>(buffer);
  return batch_ptr->getBuffer();
}

#endif  // NEXTGEN_CONTEXT_CONTEXTRUNTIMEFUNCTIONS_H
